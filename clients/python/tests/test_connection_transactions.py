"""Tests for DB-API transaction state behavior on Connection."""

from veridicaldb.connection import Connection


class _FakeCursor:
    def __init__(self, conn):
        self._conn = conn
        self.closed = False
        self.executed = []

    def execute(self, sql):
        self.executed.append(sql)

    def close(self):
        self.closed = True


class TestConnectionTransactions:
    def test_commit_noop_when_not_in_transaction(self):
        conn = Connection.__new__(Connection)
        conn._closed = False
        conn._in_transaction = False

        called = {'cursor': 0}

        def _cursor():
            called['cursor'] += 1
            return _FakeCursor(conn)

        conn.cursor = _cursor
        conn.commit()

        assert called['cursor'] == 0
        assert conn.in_transaction is False

    def test_commit_executes_and_clears_transaction(self):
        conn = Connection.__new__(Connection)
        conn._closed = False
        conn._in_transaction = True

        cursor = _FakeCursor(conn)
        conn.cursor = lambda: cursor

        conn.commit()

        assert cursor.executed == ["COMMIT"]
        assert cursor.closed is True
        assert conn.in_transaction is False

    def test_rollback_executes_and_clears_transaction(self):
        conn = Connection.__new__(Connection)
        conn._closed = False
        conn._in_transaction = True

        cursor = _FakeCursor(conn)
        conn.cursor = lambda: cursor

        conn.rollback()

        assert cursor.executed == ["ROLLBACK"]
        assert cursor.closed is True
        assert conn.in_transaction is False
