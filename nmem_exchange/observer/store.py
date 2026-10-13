"""Persistent, searchable store of observed room messages — SQLite + FTS5, stdlib only.

Deliberately zero-dependency (no ORM, no server DB) so the observer is turnkey for anyone running a
fleet: point it at a file and go. One writer (the tap) and many readers (the web API) share one
connection guarded by a lock, so the store is safe to call from the event loop AND a thread executor.

Schema (all derived purely from what crosses the open ``room:*`` channels):
  * ``messages``   — one row per group message: ``(msg_id, room, sender, ts, text, in_reply_to)``.
  * ``messages_fts`` — an external-content FTS5 index over ``text`` for substring/keyword search.
  * ``mentions``   — one row per ``@mention`` found in a message: ``(msg_id, room, mentioner,
    mentioned, ts)`` — the aggregate source for "most mentioned" / "most responsive".
  * ``rooms``      — ``(room, first_seen, last_seen, post_count)``, upserted as messages arrive.

Mentions + replies are the only *interaction* signal available to a passive observer (who addressed
whom is in the plaintext; whether a post was drive-initiated vs human-relayed is agent-internal and
never on the wire — so we don't pretend to know it). ``in_reply_to`` is stored for future threading;
current senders rarely set it, so leaderboards lean on mentions.
"""
from __future__ import annotations

import re
import sqlite3
import threading
import time
from typing import Any, Iterable

# An @mention token: '@' then an id-ish run (letters/digits/._-), matched case-insensitively against
# the known-agent set so a stray '@' or an email never counts. Trailing punctuation is trimmed so
# "@agent-a." resolves to "agent-a".
_MENTION_RE = re.compile(r"@([A-Za-z0-9][\w.\-]*)")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS messages (
  msg_id      TEXT PRIMARY KEY,
  room        TEXT NOT NULL,
  sender      TEXT NOT NULL,
  ts          REAL NOT NULL,
  text        TEXT NOT NULL,
  in_reply_to TEXT
);
CREATE INDEX IF NOT EXISTS ix_messages_room_ts ON messages (room, ts DESC);
CREATE INDEX IF NOT EXISTS ix_messages_sender_ts ON messages (sender, ts DESC);

CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts
  USING fts5(text, content='messages', content_rowid='rowid');

CREATE TABLE IF NOT EXISTS mentions (
  msg_id    TEXT NOT NULL,
  room      TEXT NOT NULL,
  mentioner TEXT NOT NULL,
  mentioned TEXT NOT NULL,
  ts        REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_mentions_mentioned_ts ON mentions (mentioned, ts DESC);
CREATE INDEX IF NOT EXISTS ix_mentions_mentioner_ts ON mentions (mentioner, ts DESC);

CREATE TABLE IF NOT EXISTS rooms (
  room       TEXT PRIMARY KEY,
  first_seen REAL NOT NULL,
  last_seen  REAL NOT NULL,
  post_count INTEGER NOT NULL DEFAULT 0,
  name       TEXT
);
"""


def extract_mentions(text: str, known: Iterable[str] | None = None) -> list[str]:
    """The agent ids ``@``-mentioned in ``text``. When ``known`` is given, only tokens that resolve to
    a known agent (raw or trailing-punctuation-trimmed, case-insensitive) count — so arbitrary ``@``
    noise is ignored. De-duped, lowercased, order-preserving."""
    known_lc = {k.lower(): k for k in (known or [])}
    out: list[str] = []
    seen: set[str] = set()
    for raw in _MENTION_RE.findall(text or ""):
        for cand in (raw.lower(), raw.lower().rstrip("._-")):
            if known_lc:
                hit = known_lc.get(cand)
                if hit is None:
                    continue
                key = hit
            else:
                key = cand
            if key not in seen:
                seen.add(key)
                out.append(key)
            break
    return out


class ObserverStore:
    """Thread-safe SQLite store of observed room traffic. All public methods take the internal lock,
    so the ingest tap (event loop) and the web API (executor threads) can share one instance."""

    def __init__(self, path: str = ":memory:") -> None:
        self._path = path
        # check_same_thread=False + an explicit lock: one connection, callable from any thread.
        self._db = sqlite3.connect(path, check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=NORMAL")
        self._lock = threading.Lock()
        with self._lock:
            self._db.executescript(_SCHEMA)
            # Migrate pre-existing DBs: CREATE TABLE IF NOT EXISTS won't add rooms.name to an old table.
            try:
                self._db.execute("ALTER TABLE rooms ADD COLUMN name TEXT")
            except sqlite3.OperationalError:
                pass  # column already present
            self._db.commit()

    # ── write ─────────────────────────────────────────────────────
    def record_message(self, *, msg_id: str, room: str, sender: str, ts: float, text: str,
                        in_reply_to: str | None = None, known_agents: Iterable[str] | None = None,
                        ) -> bool:
        """Record one observed group message (idempotent on ``msg_id`` — a replayed envelope is a
        no-op). Returns True if it was newly inserted, False if already seen. Also indexes it for
        search and extracts its ``@mentions``."""
        with self._lock:
            cur = self._db.execute(
                "INSERT OR IGNORE INTO messages (msg_id, room, sender, ts, text, in_reply_to) "
                "VALUES (?,?,?,?,?,?)",
                (msg_id, room, sender, float(ts), text, in_reply_to))
            if cur.rowcount == 0:
                return False                                   # already have it
            rowid = cur.lastrowid
            self._db.execute("INSERT INTO messages_fts (rowid, text) VALUES (?,?)", (rowid, text))
            for m in extract_mentions(text, known_agents):
                if m != sender:                                # self-mention isn't an interaction
                    self._db.execute(
                        "INSERT INTO mentions (msg_id, room, mentioner, mentioned, ts) VALUES (?,?,?,?,?)",
                        (msg_id, room, sender, m, float(ts)))
            self._db.execute(
                "INSERT INTO rooms (room, first_seen, last_seen, post_count) VALUES (?,?,?,1) "
                "ON CONFLICT(room) DO UPDATE SET last_seen=excluded.last_seen, post_count=post_count+1",
                (room, float(ts), float(ts)))
            self._db.commit()
            return True

    def set_room_name(self, *, room: str, name: str, ts: float) -> None:
        """Record a room's human NAME (from a ``room.meta`` beacon). Upserts the room row so a named
        room is labelled even before its first message; never touches post_count/last_seen ordering."""
        if not room or not name:
            return
        with self._lock:
            self._db.execute(
                "INSERT INTO rooms (room, first_seen, last_seen, post_count, name) VALUES (?,?,?,0,?) "
                "ON CONFLICT(room) DO UPDATE SET name=excluded.name",
                (room, float(ts), float(ts), name))
            self._db.commit()

    # ── read ──────────────────────────────────────────────────────
    def rooms(self) -> list[dict[str, Any]]:
        """Every room seen, busiest/most-recent first, with its distinct posters."""
        with self._lock:
            rows = self._db.execute(
                "SELECT room, first_seen, last_seen, post_count, name FROM rooms "
                "ORDER BY last_seen DESC").fetchall()
            out = []
            for r in rows:
                posters = [x["sender"] for x in self._db.execute(
                    "SELECT sender, COUNT(*) c FROM messages WHERE room=? GROUP BY sender "
                    "ORDER BY c DESC", (r["room"],)).fetchall()]
                last = self._db.execute(
                    "SELECT sender, text, ts FROM messages WHERE room=? ORDER BY ts DESC LIMIT 1",
                    (r["room"],)).fetchone()
                out.append({**dict(r), "posters": posters,
                            "last_message": (dict(last) if last else None)})
            return out

    def messages(self, room: str | None = None, *, limit: int = 100,
                 before: float | None = None) -> list[dict[str, Any]]:
        """Recent messages (newest first), optionally scoped to one room and paged with ``before`` (a
        ts cursor). Each row carries its extracted mentions."""
        q = "SELECT msg_id, room, sender, ts, text, in_reply_to FROM messages"
        clauses, params = [], []
        if room:
            clauses.append("room=?"); params.append(room)
        if before is not None:
            clauses.append("ts<?"); params.append(float(before))
        if clauses:
            q += " WHERE " + " AND ".join(clauses)
        q += " ORDER BY ts DESC LIMIT ?"; params.append(int(limit))
        with self._lock:
            rows = [dict(r) for r in self._db.execute(q, params).fetchall()]
            for r in rows:
                r["mentions"] = [x["mentioned"] for x in self._db.execute(
                    "SELECT mentioned FROM mentions WHERE msg_id=?", (r["msg_id"],)).fetchall()]
            return rows

    def search(self, query: str, *, room: str | None = None, sender: str | None = None,
               limit: int = 100) -> list[dict[str, Any]]:
        """Full-text search over message bodies (FTS5), newest first. ``room``/``sender`` narrow it.
        An empty/whitespace query returns nothing (search is a deliberate act)."""
        query = (query or "").strip()
        if not query:
            return []
        q = ("SELECT m.msg_id, m.room, m.sender, m.ts, m.text, m.in_reply_to "
             "FROM messages_fts f JOIN messages m ON m.rowid=f.rowid WHERE f.text MATCH ?")
        params: list[Any] = [_fts_query(query)]
        if room:
            q += " AND m.room=?"; params.append(room)
        if sender:
            q += " AND m.sender=?"; params.append(sender)
        q += " ORDER BY m.ts DESC LIMIT ?"; params.append(int(limit))
        with self._lock:
            try:
                return [dict(r) for r in self._db.execute(q, params).fetchall()]
            except sqlite3.OperationalError:
                return []                                      # a malformed FTS expression → no hits

    def stats(self, *, since: float | None = None, top: int = 8) -> dict[str, Any]:
        """Leaderboards + totals over an optional time window (``since`` = epoch seconds, None = all
        time): who posts most, who gets addressed most, who answers most, which rooms are busiest,
        plus an hourly activity histogram. The shape the dashboard's summary cards read."""
        since = 0.0 if since is None else float(since)
        with self._lock:
            def leaderboard(sql: str) -> list[dict[str, Any]]:
                return [{"id": r[0], "count": r[1]}
                        for r in self._db.execute(sql, (since, top)).fetchall()]

            totals = self._db.execute(
                "SELECT COUNT(*) posts, COUNT(DISTINCT sender) agents, COUNT(DISTINCT room) rooms "
                "FROM messages WHERE ts>=?", (since,)).fetchone()
            top_posters = leaderboard(
                "SELECT sender, COUNT(*) c FROM messages WHERE ts>=? GROUP BY sender "
                "ORDER BY c DESC, sender LIMIT ?")
            most_mentioned = leaderboard(
                "SELECT mentioned, COUNT(*) c FROM mentions WHERE ts>=? GROUP BY mentioned "
                "ORDER BY c DESC, mentioned LIMIT ?")
            most_responsive = leaderboard(
                "SELECT mentioner, COUNT(*) c FROM mentions WHERE ts>=? GROUP BY mentioner "
                "ORDER BY c DESC, mentioner LIMIT ?")
            busiest_rooms = leaderboard(
                "SELECT room, COUNT(*) c FROM messages WHERE ts>=? GROUP BY room "
                "ORDER BY c DESC, room LIMIT ?")
            activity = [{"hour": int(r[0]), "count": r[1]} for r in self._db.execute(
                "SELECT CAST(ts/3600 AS INT) h, COUNT(*) c FROM messages WHERE ts>=? "
                "GROUP BY h ORDER BY h", (since,)).fetchall()]
            return {
                "window_since": since,
                "totals": dict(totals),
                "top_posters": top_posters,
                "most_mentioned": most_mentioned,
                "most_responsive": most_responsive,
                "busiest_rooms": busiest_rooms,
                "activity": activity,
            }

    def close(self) -> None:
        with self._lock:
            self._db.close()


def _fts_query(q: str) -> str:
    """Turn a plain user query into a safe FTS5 MATCH expression: quote each bare term as a phrase
    (so punctuation/operators in user input can't become FTS syntax) and AND them together."""
    terms = re.findall(r"[\w]+", q)
    if not terms:
        return '""'
    return " ".join(f'"{t}"' for t in terms)
