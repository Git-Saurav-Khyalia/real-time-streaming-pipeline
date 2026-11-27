import json
from datetime import datetime

from data_generator import producer


def test_session_pool_creates_requested_number_of_sessions():
    pool = producer.SessionPool(pool_size=10)

    assert len(pool.sessions) == 10

    for session in pool.sessions:
        assert session.user_id.startswith("user_")
        assert session.session_id
        assert session.device_type in producer.DEVICE_TYPES
        assert session.events_emitted == 0


def test_get_session_returns_valid_session():
    pool = producer.SessionPool(pool_size=5)

    session = pool.get_session()

    assert isinstance(session, producer.SessionState)
    assert session.user_id
    assert session.session_id
    assert session.device_type in producer.DEVICE_TYPES


def test_build_event_contains_required_fields():
    pool = producer.SessionPool(pool_size=1)
    session = pool.get_session()

    event = producer.build_event(session)

    required_fields = {
        "event_id",
        "timestamp",
        "user_id",
        "session_id",
        "event_type",
        "product_id",
        "category",
        "price",
        "device_type",
        "country",
        "referrer",
    }

    assert required_fields.issubset(event.keys())


def test_build_event_has_valid_values():
    pool = producer.SessionPool(pool_size=1)
    session = pool.get_session()

    event = producer.build_event(session)

    assert event["event_type"] in producer.EVENT_TYPE_WEIGHTS
    assert event["category"] in producer.CATEGORIES
    assert event["device_type"] in producer.DEVICE_TYPES
    assert event["price"] > 0
    assert event["user_id"] == session.user_id
    assert event["session_id"] == session.session_id


def test_build_event_timestamp_is_valid_iso_format():
    pool = producer.SessionPool(pool_size=1)
    session = pool.get_session()

    event = producer.build_event(session)

    timestamp = datetime.fromisoformat(event["timestamp"])

    assert timestamp.tzinfo is not None


def test_build_event_is_json_serializable():
    pool = producer.SessionPool(pool_size=1)
    session = pool.get_session()

    event = producer.build_event(session)

    serialized = json.dumps(event)

    assert isinstance(serialized, str)
    assert json.loads(serialized) == event


def test_add_to_cart_updates_cart(monkeypatch):
    pool = producer.SessionPool(pool_size=1)
    session = pool.get_session()

    monkeypatch.setattr(
        producer.random,
        "choices",
        lambda *args, **kwargs: ["add_to_cart"],
    )

    event = producer.build_event(session)

    assert event["event_type"] == "add_to_cart"
    assert event["product_id"] in session.cart_product_ids
    assert session.events_emitted == 1


def test_build_producer_uses_idempotent_configuration(monkeypatch):
    captured_config = {}

    class FakeProducer:
        def __init__(self, config):
            captured_config.update(config)

    monkeypatch.setattr(producer, "Producer", FakeProducer)

    result = producer.build_producer("localhost:9092")

    assert isinstance(result, FakeProducer)
    assert captured_config["bootstrap.servers"] == "localhost:9092"
    assert captured_config["enable.idempotence"] is True
    assert captured_config["acks"] == "all"
    assert captured_config["retries"] == 5

# ----------------------------------------------------- imperfect-data injection
import random


def _event():
    pool = producer.SessionPool(pool_size=1)
    return producer.build_event(pool.get_session())


def test_chaos_disabled_emits_exactly_the_clean_event():
    injector = producer.ChaosInjector(producer.ChaosConfig(), random.Random(1))
    event = _event()

    assert injector.mutate(event) == [json.dumps(event)]
    assert not injector.counts


def test_chaos_duplicates_resend_an_identical_earlier_payload():
    injector = producer.ChaosInjector(producer.ChaosConfig(duplicate_rate=1.0), random.Random(1))
    sent = []
    for _ in range(5):
        payloads = injector.mutate(_event())
        assert len(payloads) == 2  # the event plus one redelivery
        assert payloads[1] in sent + [payloads[0]]  # byte-for-byte copy of something already sent
        sent.append(payloads[0])
    assert injector.counts["duplicate"] == 5


def test_chaos_late_events_are_backdated():
    injector = producer.ChaosInjector(producer.ChaosConfig(late_rate=1.0), random.Random(3))
    event = _event()
    [payload] = injector.mutate(event)

    original = datetime.fromisoformat(event["timestamp"])
    shifted = datetime.fromisoformat(json.loads(payload)["timestamp"])
    assert (original - shifted).total_seconds() >= 30
    assert injector.counts["late"] == 1


def test_chaos_invalid_payloads_break_the_contract():
    injector = producer.ChaosInjector(producer.ChaosConfig(invalid_rate=1.0), random.Random(7))
    broken_kinds = set()
    for _ in range(60):
        [payload] = injector.mutate(_event())
        try:
            parsed = json.loads(payload)
        except json.JSONDecodeError:
            broken_kinds.add("malformed_json")
            continue
        if "event_id" not in parsed:
            broken_kinds.add("missing_event_id")
        elif parsed["price"] < 0:
            broken_kinds.add("negative_price")
        elif parsed["event_type"] not in producer.EVENT_TYPE_WEIGHTS:
            broken_kinds.add("bad_event_type")
        else:
            raise AssertionError("invalid_rate=1.0 produced a valid event")
    assert broken_kinds == set(producer.ChaosInjector.INVALID_KINDS)
