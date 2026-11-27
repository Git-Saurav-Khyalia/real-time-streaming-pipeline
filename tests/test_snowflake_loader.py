import pytest

from warehouse import load_silver_to_snowflake as loader

RAW = "STREAMING_DB.RAW_SCHEMA.RAW_ECOMMERCE_EVENTS"


class FakeCursor:
    def __init__(self, rowcount_for_publish=0):
        self.executed = []
        self.rowcount = None
        self._publish_rowcount = rowcount_for_publish

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        self.rowcount = self._publish_rowcount if "WHERE NOT EXISTS" in sql else None


class FakeConn:
    def __init__(self, rowcount_for_publish=0):
        self.cur = FakeCursor(rowcount_for_publish)
        self.committed = False

    def cursor(self):
        return self.cur

    def commit(self):
        self.committed = True


def rows(n):
    return [(f"e{i}", f'{{"event_id": "e{i}"}}', i % 3, 100 + i) for i in range(n)]


def test_snowflake_configured_requires_every_variable(monkeypatch):
    for v in loader.REQUIRED_ENV_VARS:
        monkeypatch.delenv(v, raising=False)
    assert loader.snowflake_configured() is False

    for v in loader.REQUIRED_ENV_VARS:
        monkeypatch.setenv(v, "x")
    assert loader.snowflake_configured() is True

    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "")
    assert loader.snowflake_configured() is False


def test_identifiers_are_validated(monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_DATABASE", "STREAMING_DB")
    monkeypatch.setenv("SNOWFLAKE_SCHEMA", "RAW_SCHEMA")
    assert loader.qualified_table() == RAW

    monkeypatch.setenv("SNOWFLAKE_SCHEMA", "RAW; DROP TABLE x")
    with pytest.raises(ValueError):
        loader.qualified_table()


def test_publish_sql_is_idempotent_by_event_id():
    sql = loader.publish_sql(RAW)
    assert "NOT EXISTS" in sql
    assert "r.raw_event:event_id::string = s.event_id" in sql
    assert sql.strip().startswith(f"INSERT INTO {RAW}")


def test_load_rows_stages_in_batches_then_publishes_once():
    conn = FakeConn(rowcount_for_publish=1100)
    result = loader.load_rows(conn, RAW, rows(1200), batch_size=500)

    assert result == {"staged": 1200, "inserted": 1100, "already_present": 100}
    assert conn.committed

    statements = [s for s, _ in conn.cur.executed]
    stage_inserts = [(s, p) for s, p in conn.cur.executed if s.startswith("INSERT INTO ECOMMERCE_EVENTS_STAGE")]
    assert len(stage_inserts) == 3  # 500 + 500 + 200
    assert [len(p) for _, p in stage_inserts] == [2000, 2000, 800]  # 4 binds per row
    assert stage_inserts[0][1][:4] == ["e0", '{"event_id": "e0"}', 0, 100]
    assert sum("WHERE NOT EXISTS" in s for s in statements) == 1
    assert any(s.startswith("CREATE DATABASE IF NOT EXISTS STREAMING_DB") for s in statements)


def test_load_rows_with_nothing_to_load_skips_publish():
    conn = FakeConn()
    result = loader.load_rows(conn, RAW, [], batch_size=500)
    assert result == {"staged": 0, "inserted": 0, "already_present": 0}
    assert not any("WHERE NOT EXISTS" in s for s, _ in conn.cur.executed)
