"""
producer.py
-----------
Simulates a live e-commerce clickstream + transaction platform and streams
JSON events to a Kafka topic (default: ecommerce_events_raw) at a
configurable rate.

Design notes:
- Uses a pool of synthetic "sessions" so events for the same user/session
  arrive close together in time (realistic funnel behavior: page_view ->
  add_to_cart -> checkout_start -> purchase), rather than pure noise.
- event_type is chosen with a weighted distribution that mimics a typical
  e-commerce funnel (lots of page views, few purchases).
- Uses the Kafka message key = user_id so that all events for a given user
  land on the same partition (ordering guarantee per user).
- Delivery reports and idempotent producer config used for at-least-once /
  effectively-once semantics on the Kafka side.
- Optional "imperfect data" injection (--duplicate-rate / --late-rate /
  --invalid-rate) deliberately re-sends events, back-dates timestamps and emits
  malformed records, so the downstream pipeline's de-duplication, late-data
  handling and quarantine logic are exercised continuously without any manual
  simulation.

Usage:
    python producer.py --rate 5 --topic ecommerce_events_raw \
        --bootstrap-servers localhost:9092
"""

import argparse
import collections
import json
import logging
import random
import signal
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional

from confluent_kafka import Producer
from faker import Faker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | producer | %(message)s",
)
logger = logging.getLogger("producer")

fake = Faker()

CATEGORIES = [
    "electronics", "apparel", "home_and_kitchen", "beauty",
    "sports_outdoors", "toys_games", "books", "grocery",
    "automotive", "office_supplies",
]

DEVICE_TYPES = ["mobile", "desktop", "tablet"]
DEVICE_WEIGHTS = [0.55, 0.35, 0.10]  # mobile-heavy traffic, like real e-commerce

# Funnel-realistic event-type weights (most traffic is page views, fewest are purchases)
EVENT_TYPE_WEIGHTS = {
    "page_view": 0.55,
    "add_to_cart": 0.20,
    "remove_from_cart": 0.07,
    "checkout_start": 0.10,
    "purchase": 0.08,
}

PRICE_RANGE_BY_CATEGORY = {
    "electronics": (25, 1800),
    "apparel": (10, 250),
    "home_and_kitchen": (8, 600),
    "beauty": (5, 150),
    "sports_outdoors": (10, 900),
    "toys_games": (5, 200),
    "books": (5, 60),
    "grocery": (2, 80),
    "automotive": (10, 1200),
    "office_supplies": (3, 300),
}


@dataclass
class SessionState:
    """Tracks an in-flight synthetic user session so related events
    (page_view -> add_to_cart -> purchase) share user_id/session_id/device."""
    user_id: str
    session_id: str
    device_type: str
    events_emitted: int = 0
    cart_product_ids: list = field(default_factory=list)
    max_events: int = field(default_factory=lambda: random.randint(3, 25))


class SessionPool:
    """Maintains a rolling pool of active sessions, recycling expired ones."""

    def __init__(self, pool_size: int = 200):
        self.pool_size = pool_size
        self.sessions: list[SessionState] = [self._new_session() for _ in range(pool_size)]

    @staticmethod
    def _new_session() -> SessionState:
        return SessionState(
            user_id=f"user_{fake.uuid4()[:8]}",
            session_id=str(uuid.uuid4()),
            device_type=random.choices(DEVICE_TYPES, weights=DEVICE_WEIGHTS, k=1)[0],
        )

    def get_session(self) -> SessionState:
        session = random.choice(self.sessions)
        if session.events_emitted >= session.max_events:
            idx = self.sessions.index(session)
            self.sessions[idx] = self._new_session()
            session = self.sessions[idx]
        return session


def build_event(session: SessionState) -> dict:
    """Constructs a single clickstream/transaction event for the given session."""
    category = random.choice(CATEGORIES)
    lo, hi = PRICE_RANGE_BY_CATEGORY[category]
    price = round(random.uniform(lo, hi), 2)
    product_id = f"prod_{random.randint(1000, 9999)}"

    # Bias event_type toward a coherent funnel: once a user has items in cart,
    # they're more likely to check out; purchase only follows checkout_start.
    if session.cart_product_ids and random.random() < 0.35:
        event_type = random.choices(
            ["checkout_start", "purchase", "remove_from_cart"],
            weights=[0.5, 0.35, 0.15],
            k=1,
        )[0]
    else:
        event_type = random.choices(
            list(EVENT_TYPE_WEIGHTS.keys()),
            weights=list(EVENT_TYPE_WEIGHTS.values()),
            k=1,
        )[0]

    if event_type == "add_to_cart":
        session.cart_product_ids.append(product_id)
    elif event_type in ("purchase", "remove_from_cart") and session.cart_product_ids:
        product_id = session.cart_product_ids.pop(0)

    session.events_emitted += 1

    event = {
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "user_id": session.user_id,
        "session_id": session.session_id,
        "event_type": event_type,
        "product_id": product_id,
        "category": category,
        "price": price,
        "device_type": session.device_type,
        # Extra, useful-in-practice fields kept lightweight:
        "country": fake.country_code(),
        "referrer": random.choice(["direct", "google", "email", "social", "affiliate"]),
    }
    return event


@dataclass
class ChaosConfig:
    """Probabilities (0..1) of injecting imperfect data per generated event."""
    duplicate_rate: float = 0.0
    late_rate: float = 0.0
    invalid_rate: float = 0.0
    max_late_seconds: int = 2400  # back-date by up to 40 min (watermark is 10 min)


class ChaosInjector:
    """
    Turns one clean event into the list of Kafka payloads actually sent:
      * duplicate : re-sends a previously sent event byte-for-byte (Kafka redelivery)
      * late      : back-dates event time, half within and half beyond the watermark
      * invalid   : malformed JSON / negative price / unknown event_type / missing id
    """

    INVALID_KINDS = ("malformed_json", "negative_price", "bad_event_type", "missing_event_id")

    def __init__(self, config: ChaosConfig, rng: Optional[random.Random] = None):
        self.config = config
        self.rng = rng or random.Random()
        self._recent: collections.deque = collections.deque(maxlen=500)
        self.counts = collections.Counter()

    def mutate(self, event: dict) -> list:
        cfg, rng = self.config, self.rng
        payloads: list = []

        if rng.random() < cfg.invalid_rate:
            payloads.append(self._invalid_payload(event))
            self.counts["invalid"] += 1
        else:
            if rng.random() < cfg.late_rate:
                lateness = rng.choice(
                    [rng.randint(30, 300), rng.randint(900, max(901, cfg.max_late_seconds))]
                )
                shifted = datetime.fromisoformat(event["timestamp"]) - timedelta(seconds=lateness)
                event = {**event, "timestamp": shifted.isoformat()}
                self.counts["late"] += 1
            payload = json.dumps(event)
            payloads.append(payload)
            self._recent.append(payload)

        if self._recent and rng.random() < cfg.duplicate_rate:
            payloads.append(rng.choice(self._recent))
            self.counts["duplicate"] += 1

        return payloads

    def _invalid_payload(self, event: dict) -> str:
        kind = self.rng.choice(self.INVALID_KINDS)
        if kind == "malformed_json":
            return json.dumps(event)[: self.rng.randint(10, 60)]  # truncated JSON
        broken = dict(event)
        if kind == "negative_price":
            broken["price"] = -abs(broken["price"])
        elif kind == "bad_event_type":
            broken["event_type"] = "teleport"
        else:
            broken.pop("event_id", None)
        return json.dumps(broken)


class GracefulKiller:
    kill_now = False

    def __init__(self):
        signal.signal(signal.SIGINT, self._exit)
        signal.signal(signal.SIGTERM, self._exit)

    def _exit(self, *_args):
        self.kill_now = True


def delivery_report(err, msg):
    if err is not None:
        logger.error("Delivery failed for record %s: %s", msg.key(), err)


def build_producer(bootstrap_servers: str) -> Producer:
    conf = {
        "bootstrap.servers": bootstrap_servers,
        "enable.idempotence": True,          # idempotent Kafka producer delivery
        "acks": "all",
        "retries": 5,
        "linger.ms": 50,                     # small batching window
        "compression.type": "snappy",
        "client.id": "ecommerce-clickstream-producer",
    }
    return Producer(conf)


def run(args: argparse.Namespace) -> None:
    producer = build_producer(args.bootstrap_servers)
    pool = SessionPool(pool_size=args.concurrent_sessions)
    chaos = ChaosInjector(
        ChaosConfig(
            duplicate_rate=args.duplicate_rate,
            late_rate=args.late_rate,
            invalid_rate=args.invalid_rate,
        )
    )
    killer = GracefulKiller()

    interval = 1.0 / args.rate if args.rate > 0 else 0
    total_sent = 0
    generated = 0
    start_time = time.time()

    logger.info(
        "Starting producer -> topic=%s rate=%s events/sec bootstrap=%s",
        args.topic, args.rate, args.bootstrap_servers,
    )

    try:
        while not killer.kill_now:
            session = pool.get_session()
            event = build_event(session)
            generated += 1

            try:
                for payload in chaos.mutate(event):
                    producer.produce(
                        topic=args.topic,
                        key=event["user_id"].encode("utf-8"),
                        value=payload.encode("utf-8"),
                        callback=delivery_report,
                    )
                    total_sent += 1
            except BufferError:
                logger.warning("Local producer queue full, polling to free space")
                producer.poll(0.5)
                continue

            producer.poll(0)  # trigger delivery-report callbacks, non-blocking

            if generated % 100 == 0:
                elapsed = time.time() - start_time
                logger.info(
                    "Sent %d events (%.1f events/sec avg) injected=%s",
                    total_sent, total_sent / elapsed if elapsed > 0 else 0,
                    dict(chaos.counts),
                )

            if interval:
                time.sleep(interval)
    finally:
        logger.info("Flushing producer and shutting down (total sent=%d)...", total_sent)
        producer.flush(timeout=10)
        logger.info("Producer stopped cleanly.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stream synthetic e-commerce events to Kafka.")
    parser.add_argument("--bootstrap-servers", default="localhost:9092",
                         help="Kafka bootstrap servers (default: localhost:9092)")
    parser.add_argument("--topic", default="ecommerce_events_raw",
                         help="Target Kafka topic (default: ecommerce_events_raw)")
    parser.add_argument("--rate", type=float, default=5.0,
                         help="Target events per second (default: 5)")
    parser.add_argument("--concurrent-sessions", type=int, default=200,
                         help="Number of concurrent synthetic user sessions to simulate")
    parser.add_argument("--duplicate-rate", type=float, default=0.0,
                         help="Probability of re-sending an earlier event (default: 0)")
    parser.add_argument("--late-rate", type=float, default=0.0,
                         help="Probability of back-dating an event's timestamp (default: 0)")
    parser.add_argument("--invalid-rate", type=float, default=0.0,
                         help="Probability of emitting a malformed/invalid record (default: 0)")
    return parser.parse_args()


if __name__ == "__main__":
    try:
        run(parse_args())
    except Exception:
        logger.exception("Producer crashed with an unhandled exception")
        sys.exit(1)
