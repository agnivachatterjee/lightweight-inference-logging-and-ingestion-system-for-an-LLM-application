from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any


class Database:
    def __init__(self, path: str):
        self.path = path
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._local = threading.local()
        self.init()

    def connect(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(self.path, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA foreign_keys=ON")
            self._local.conn = conn
        return conn

    def init(self) -> None:
        conn = self.connect()
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS conversations (
              id TEXT PRIMARY KEY,
              title TEXT NOT NULL,
              status TEXT NOT NULL DEFAULT 'active',
              provider TEXT NOT NULL,
              model TEXT NOT NULL,
              created_at INTEGER NOT NULL,
              updated_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS chat_messages (
              id TEXT PRIMARY KEY,
              conversation_id TEXT NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
              role TEXT NOT NULL CHECK(role IN ('system', 'user', 'assistant')),
              content TEXT NOT NULL,
              sequence INTEGER NOT NULL,
              created_at INTEGER NOT NULL,
              metadata_json TEXT NOT NULL DEFAULT '{}'
            );

            CREATE TABLE IF NOT EXISTS inference_logs (
              id TEXT PRIMARY KEY,
              conversation_id TEXT NOT NULL,
              provider TEXT NOT NULL,
              model TEXT NOT NULL,
              started_at INTEGER NOT NULL,
              ended_at INTEGER NOT NULL,
              latency_ms INTEGER NOT NULL,
              status TEXT NOT NULL,
              error TEXT,
              prompt_tokens INTEGER NOT NULL DEFAULT 0,
              completion_tokens INTEGER NOT NULL DEFAULT 0,
              total_tokens INTEGER NOT NULL DEFAULT 0,
              input_preview TEXT,
              output_preview TEXT,
              raw_json TEXT NOT NULL,
              created_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS inference_metadata (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              log_id TEXT NOT NULL REFERENCES inference_logs(id) ON DELETE CASCADE,
              key TEXT NOT NULL,
              value_json TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_messages_conversation_sequence
              ON chat_messages(conversation_id, sequence);
            CREATE INDEX IF NOT EXISTS idx_logs_created_at
              ON inference_logs(created_at);
            CREATE INDEX IF NOT EXISTS idx_logs_conversation
              ON inference_logs(conversation_id);
            CREATE INDEX IF NOT EXISTS idx_logs_provider_model
              ON inference_logs(provider, model);
            """
        )
        conn.commit()

    @staticmethod
    def now() -> int:
        return int(time.time() * 1000)

    def create_conversation(self, conversation_id: str, title: str, provider: str, model: str) -> None:
        now = self.now()
        self.connect().execute(
            """
            INSERT OR IGNORE INTO conversations
              (id, title, status, provider, model, created_at, updated_at)
            VALUES (?, ?, 'active', ?, ?, ?, ?)
            """,
            (conversation_id, title, provider, model, now, now),
        )
        self.connect().commit()

    def touch_conversation(self, conversation_id: str, title: str | None = None) -> None:
        if title:
            self.connect().execute(
                "UPDATE conversations SET title = ?, updated_at = ? WHERE id = ?",
                (title, self.now(), conversation_id),
            )
        else:
            self.connect().execute(
                "UPDATE conversations SET updated_at = ? WHERE id = ?",
                (self.now(), conversation_id),
            )
        self.connect().commit()

    def cancel_conversation(self, conversation_id: str) -> None:
        self.connect().execute(
            "UPDATE conversations SET status = 'cancelled', updated_at = ? WHERE id = ?",
            (self.now(), conversation_id),
        )
        self.connect().commit()

    def activate_conversation(self, conversation_id: str) -> None:
        self.connect().execute(
            "UPDATE conversations SET status = 'active', updated_at = ? WHERE id = ?",
            (self.now(), conversation_id),
        )
        self.connect().commit()

    def get_conversation(self, conversation_id: str) -> dict[str, Any] | None:
        row = self.connect().execute("SELECT * FROM conversations WHERE id = ?", (conversation_id,)).fetchone()
        return dict(row) if row else None

    def list_conversations(self) -> list[dict[str, Any]]:
        rows = self.connect().execute(
            """
            SELECT c.*,
              (SELECT COUNT(*) FROM chat_messages m WHERE m.conversation_id = c.id) AS message_count
            FROM conversations c
            ORDER BY updated_at DESC
            LIMIT 100
            """
        ).fetchall()
        return [dict(row) for row in rows]

    def add_message(self, message_id: str, conversation_id: str, role: str, content: str, metadata: dict[str, Any] | None = None) -> None:
        row = self.connect().execute(
            "SELECT COALESCE(MAX(sequence), 0) + 1 AS next_seq FROM chat_messages WHERE conversation_id = ?",
            (conversation_id,),
        ).fetchone()
        self.connect().execute(
            """
            INSERT INTO chat_messages
              (id, conversation_id, role, content, sequence, created_at, metadata_json)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (message_id, conversation_id, role, content, row["next_seq"], self.now(), json.dumps(metadata or {})),
        )
        self.touch_conversation(conversation_id)

    def get_messages(self, conversation_id: str, limit: int = 50) -> list[dict[str, Any]]:
        rows = self.connect().execute(
            """
            SELECT * FROM (
              SELECT * FROM chat_messages
              WHERE conversation_id = ?
              ORDER BY sequence DESC
              LIMIT ?
            ) ORDER BY sequence ASC
            """,
            (conversation_id, limit),
        ).fetchall()
        return [dict(row) for row in rows]

    def store_inference_log(self, event: dict[str, Any], extracted: dict[str, Any]) -> None:
        conn = self.connect()
        conn.execute(
            """
            INSERT OR REPLACE INTO inference_logs
              (id, conversation_id, provider, model, started_at, ended_at, latency_ms, status, error,
               prompt_tokens, completion_tokens, total_tokens, input_preview, output_preview, raw_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event["event_id"],
                event["conversation_id"],
                event["provider"],
                event["model"],
                event["started_at"],
                event["ended_at"],
                event["latency_ms"],
                event["status"],
                event.get("error"),
                event.get("prompt_tokens", 0),
                event.get("completion_tokens", 0),
                event.get("total_tokens", 0),
                event.get("input_preview", ""),
                event.get("output_preview", ""),
                json.dumps(event),
                self.now(),
            ),
        )
        conn.execute("DELETE FROM inference_metadata WHERE log_id = ?", (event["event_id"],))
        for key, value in extracted.items():
            conn.execute(
                "INSERT INTO inference_metadata (log_id, key, value_json) VALUES (?, ?, ?)",
                (event["event_id"], key, json.dumps(value)),
            )
        conn.commit()

    def dashboard(self) -> dict[str, Any]:
        conn = self.connect()
        totals = dict(
            conn.execute(
                """
                SELECT
                  COUNT(*) AS requests,
                  COALESCE(AVG(latency_ms), 0) AS avg_latency_ms,
                  COALESCE(MAX(latency_ms), 0) AS max_latency_ms,
                  SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) AS errors,
                  SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled,
                  COALESCE(SUM(total_tokens), 0) AS total_tokens
                FROM inference_logs
                """
            ).fetchone()
        )
        by_provider = [
            dict(row)
            for row in conn.execute(
                """
                SELECT provider, model, COUNT(*) AS requests, COALESCE(AVG(latency_ms), 0) AS avg_latency_ms,
                  SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) AS errors
                FROM inference_logs
                GROUP BY provider, model
                ORDER BY requests DESC
                """
            ).fetchall()
        ]
        recent = [
            dict(row)
            for row in conn.execute(
                """
                SELECT id, conversation_id, provider, model, latency_ms, status, error, created_at
                FROM inference_logs
                ORDER BY created_at DESC
                LIMIT 20
                """
            ).fetchall()
        ]
        since = self.now() - 60 * 60 * 1000
        per_minute = [
            dict(row)
            for row in conn.execute(
                """
                SELECT ((created_at / 60000) * 60000) AS bucket, COUNT(*) AS requests
                FROM inference_logs
                WHERE created_at >= ?
                GROUP BY bucket
                ORDER BY bucket ASC
                """,
                (since,),
            ).fetchall()
        ]
        return {"totals": totals, "by_provider": by_provider, "recent": recent, "per_minute": per_minute}


def from_env() -> Database:
    return Database(os.getenv("DATABASE_PATH", "data/app.db"))
