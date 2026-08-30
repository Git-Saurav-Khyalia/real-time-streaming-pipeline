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