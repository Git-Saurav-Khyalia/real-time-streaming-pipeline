import json

import pytest

from ingestion import consumer_snowflake


def test_validate_env_fails_when_variables_are_missing(monkeypatch):
    for variable in consumer_snowflake.REQUIRED_ENV_VARS:
        monkeypatch.delenv(variable, raising=False)

    with pytest.raises(SystemExit) as exc:
        consumer_snowflake.validate_env()

    assert exc.value.code == 1


def test_validate_env_succeeds_when_all_variables_exist(monkeypatch):
    for variable in consumer_snowflake.REQUIRED_ENV_VARS:
        monkeypatch.setenv(variable, "test_value")

    consumer_snowflake.validate_env()


def test_get_snowflake_connection_uses_environment_variables(monkeypatch):
    captured = {}

    def fake_connect(**kwargs):
        captured.update(kwargs)
        return "fake_connection"

    monkeypatch.setattr(
        consumer_snowflake.snowflake.connector,
        "connect",
        fake_connect,
    )

    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "test_account")
    monkeypatch.setenv("SNOWFLAKE_USER", "test_user")
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "test_password")
    monkeypatch.setenv("SNOWFLAKE_WAREHOUSE", "test_warehouse")
    monkeypatch.setenv("SNOWFLAKE_DATABASE", "test_database")
    monkeypatch.setenv("SNOWFLAKE_SCHEMA", "test_schema")
    monkeypatch.setenv("SNOWFLAKE_ROLE", "test_role")

    result = consumer_snowflake.get_snowflake_connection()

    assert result == "fake_connection"
    assert captured["account"] == "test_account"
    assert captured["user"] == "test_user"
    assert captured["password"] == "test_password"
    assert captured["warehouse"] == "test_warehouse"
    assert captured["database"] == "test_database"
    assert captured["schema"] == "test_schema"
    assert captured["role"] == "test_role"
    assert captured["client_session_keep_alive"] is True


class FakeCursor:
    def __init__(self):
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def execute(self, sql, params=None):
        self.executed.append((sql, params))


class FakeConnection:
    def __init__(self):
        self.cursor_instance = FakeCursor()
        self.commit_called = False
        self.rollback_called = False

    def cursor(self):
        return self.cursor_instance

    def commit(self):
        self.commit_called = True

    def rollback(self):
        self.rollback_called = True


def test_flush_batch_does_nothing_for_empty_batch():
    connection = FakeConnection()

    consumer_snowflake.flush_batch(
        connection,
        "RAW_DB.RAW_SCHEMA.RAW_ECOMMERCE_EVENTS",
        [],
    )

    assert connection.cursor_instance.executed == []
    assert connection.commit_called is False


def test_flush_batch_inserts_json_events():
    connection = FakeConnection()

    batch = [
        (
            json.dumps({
                "event_id": "event-1",
                "event_type": "page_view",
                "user_id": "user-1",
                "price": 99.99,
            }),
            0,
            100,
        ),
        (
            json.dumps({
                "event_id": "event-2",
                "event_type": "purchase",
                "user_id": "user-2",
                "price": 199.99,
            }),
            1,
            101,
        ),
    ]

    consumer_snowflake.flush_batch(
        connection,
        "RAW_DB.RAW_SCHEMA.RAW_ECOMMERCE_EVENTS",
        batch,
    )

    assert connection.commit_called is True
    assert connection.rollback_called is False

    executions = connection.cursor_instance.executed

    assert len(executions) == 2

    assert executions[0][0] == "BEGIN"

    insert_sql, params = executions[1]

    assert "INSERT INTO RAW_DB.RAW_SCHEMA.RAW_ECOMMERCE_EVENTS" in insert_sql
    assert "PARSE_JSON(column1)" in insert_sql
    assert "FROM VALUES" in insert_sql

    assert params == [
        batch[0][0],
        0,
        100,
        batch[1][0],
        1,
        101,
    ]


def test_build_consumer_configures_manual_offset_commit(monkeypatch):
    captured = {}

    class FakeConsumer:
        def __init__(self, config):
            captured.update(config)

        def subscribe(self, topics):
            captured["topics"] = topics

    monkeypatch.setattr(
        consumer_snowflake,
        "Consumer",
        FakeConsumer,
    )

    result = consumer_snowflake.build_consumer(
        "localhost:9092",
        "test-group",
        "ecommerce_events_raw",
    )

    assert isinstance(result, FakeConsumer)

    assert captured["bootstrap.servers"] == "localhost:9092"
    assert captured["group.id"] == "test-group"
    assert captured["enable.auto.commit"] is False
    assert captured["auto.offset.reset"] == "earliest"
    assert captured["max.poll.interval.ms"] == 300000
    assert captured["topics"] == ["ecommerce_events_raw"]