"""Tests for the transaction helper, using a fake psycopg2 connection."""

import psycopg2
import pytest

from common import db


class FakeCursor:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class FakeConnection:
    def __init__(self, broken_on_rollback=False):
        self.closed = 0
        self.commits = 0
        self.rollbacks = 0
        self.broken_on_rollback = broken_on_rollback

    def cursor(self):
        return FakeCursor()

    def commit(self):
        self.commits += 1

    def rollback(self):
        if self.broken_on_rollback:
            self.closed = 2  # what psycopg2 reports after the server connection is lost
            raise psycopg2.InterfaceError("connection already closed")
        self.rollbacks += 1


@pytest.fixture
def connections(monkeypatch):
    created = []

    def connect(**_kwargs):
        conn = FakeConnection(broken_on_rollback=not created)  # only the first one breaks
        created.append(conn)
        return conn

    monkeypatch.setattr(db, "_conn", None)
    monkeypatch.setattr(db.psycopg2, "connect", connect)
    return created


def test_commits_on_success(connections):
    with db.transaction():
        pass
    assert connections[0].commits == 1


def test_rolls_back_on_error_and_reuses_healthy_connection(monkeypatch):
    conn = FakeConnection()
    monkeypatch.setattr(db, "_conn", conn)

    with pytest.raises(ValueError):
        with db.transaction():
            raise ValueError("bad row")
    assert conn.rollbacks == 1 and conn.commits == 0

    with db.transaction():
        pass
    assert db._conn is conn and conn.commits == 1


def test_reconnects_after_connection_breaks(connections):
    with pytest.raises(psycopg2.OperationalError):
        with db.transaction():
            raise psycopg2.OperationalError("server closed the connection")

    with db.transaction():
        pass
    assert len(connections) == 2
    assert connections[1].commits == 1
