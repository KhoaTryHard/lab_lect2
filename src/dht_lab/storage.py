# Luu tru node, ban ghi registry va tin nhan bang SQLite.
"""Small SQLite persistence layer for node identity, registry and inbox data."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any


class RecordConflict(ValueError):
    """Raised when the same registry revision contains different data."""


class MessageConflict(ValueError):
    """Raised when a message ID is reused for different message data."""


class Storage:
    def __init__(self, directory: str | Path, node_name: str, node_id: int) -> None:
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.path = self.directory / "node.sqlite3"
        self.connection = sqlite3.connect(self.path, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=FULL")
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS registry (
                name TEXT PRIMARY KEY,
                node_id INTEGER NOT NULL,
                host TEXT NOT NULL,
                port INTEGER NOT NULL,
                revision INTEGER NOT NULL,
                updated_at TEXT NOT NULL,
                payload TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS inbox (
                message_id TEXT PRIMARY KEY,
                sender TEXT NOT NULL,
                recipient TEXT NOT NULL,
                body TEXT NOT NULL,
                received_at TEXT NOT NULL
            );
            """
        )
        self.connection.execute(
            "INSERT OR IGNORE INTO meta(key, value) VALUES ('node_name', ?)", (node_name,)
        )
        self.connection.execute(
            "INSERT OR IGNORE INTO meta(key, value) VALUES ('node_id', ?)", (str(node_id),)
        )
        self.connection.execute(
            "INSERT OR IGNORE INTO meta(key, value) VALUES ('revision', '0')"
        )
        self.connection.commit()

    def next_revision(self) -> int:
        current = int(self.get_meta("revision", "0")) + 1
        self.set_meta("revision", str(current))
        return current

    def get_meta(self, key: str, default: str | None = None) -> str | None:
        row = self.connection.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
        return str(row[0]) if row else default

    def set_meta(self, key: str, value: str) -> None:
        self.connection.execute(
            "INSERT INTO meta(key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )
        self.connection.commit()

    def put_record(self, record: dict[str, Any]) -> bool:
        """Store only the newest version and return whether it was applied."""

        existing = self.connection.execute(
            "SELECT revision, payload FROM registry WHERE name = ?", (record["name"],)
        ).fetchone()
        incoming_revision = int(record["revision"])
        if existing:
            current_revision = int(existing["revision"])
            if incoming_revision < current_revision:
                return False
            if incoming_revision == current_revision:
                incoming_payload = json.dumps(record, ensure_ascii=False, sort_keys=True)
                if incoming_payload == str(existing["payload"]):
                    return False
                raise RecordConflict(
                    f"record {record['name']!r} has conflicting revision {incoming_revision}"
                )
        self.connection.execute(
            "INSERT INTO registry(name, node_id, host, port, revision, updated_at, payload) "
            "VALUES (?, ?, ?, ?, ?, datetime('now'), ?) "
            "ON CONFLICT(name) DO UPDATE SET node_id=excluded.node_id, "
            "host=excluded.host, port=excluded.port, revision=excluded.revision, "
            "updated_at=excluded.updated_at, payload=excluded.payload",
            (
                record["name"],
                int(record["node_id"]),
                record["host"],
                int(record["port"]),
                incoming_revision,
                json.dumps(record, ensure_ascii=False, sort_keys=True),
            ),
        )
        self.connection.commit()
        return True

    def get_record(self, name: str) -> dict[str, Any] | None:
        row = self.connection.execute(
            "SELECT payload FROM registry WHERE name = ?", (name,)
        ).fetchone()
        return json.loads(row[0]) if row else None

    def list_records(self) -> list[dict[str, Any]]:
        rows = self.connection.execute("SELECT payload FROM registry ORDER BY name").fetchall()
        return [json.loads(row[0]) for row in rows]

    def deliver_once(
        self, message_id: str, sender: str, recipient: str, body: str
    ) -> tuple[bool, bool]:
        """Persist a message exactly once; return (inserted, already_present)."""

        existing = self.connection.execute(
            "SELECT sender, recipient, body FROM inbox WHERE message_id = ?", (message_id,)
        ).fetchone()
        if existing:
            if (
                str(existing["sender"]) != sender
                or str(existing["recipient"]) != recipient
                or str(existing["body"]) != body
            ):
                raise MessageConflict(f"message_id {message_id!r} is already used for another message")
            return False, True
        self.connection.execute(
            "INSERT INTO inbox(message_id, sender, recipient, body, received_at) "
            "VALUES (?, ?, ?, ?, datetime('now'))",
            (message_id, sender, recipient, body),
        )
        self.connection.commit()
        return True, False

    def list_messages(self) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT message_id, sender, recipient, body, received_at "
            "FROM inbox ORDER BY received_at, message_id"
        ).fetchall()
        return [dict(row) for row in rows]

    def close(self) -> None:
        self.connection.close()
