"""SQLite 存储层：连接、建表、启动时初始化。

数据库文件路径由环境变量 DATABASE_PATH 指定，默认 /data/inject.db。
所有写操作都在 app.main 的 state_lock 下执行（单事件循环内），
因此这里使用单连接即可。
"""

from __future__ import annotations

import os
import sqlite3
import time
from pathlib import Path

DB_PATH = os.environ.get("DATABASE_PATH", "/data/inject.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS rooms (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    code           TEXT NOT NULL UNIQUE,
    name           TEXT NOT NULL DEFAULT '',
    status         TEXT NOT NULL DEFAULT 'idle',      -- idle | running | paused
    elapsed_ms     INTEGER NOT NULL DEFAULT 0,        -- 已累计的虚拟时钟毫秒（暂停时冻结值）
    last_resume_at REAL,                              -- 最近一次进入 running 的墙钟时间（秒）
    created_at     REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS groups (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    room_id     INTEGER NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
    name        TEXT NOT NULL,                        -- 主持人可随时重命名
    join_code   TEXT NOT NULL UNIQUE,                 -- 该组独立的 6 位加入码
    created_at  REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS cards (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    room_id         INTEGER NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
    seq             INTEGER NOT NULL,                 -- 房间内稳定序号，揭示次序的决胜键
    trigger_seconds INTEGER NOT NULL,                 -- 相对演练开始的触发秒数
    title           TEXT NOT NULL,
    content         TEXT NOT NULL DEFAULT '',
    target_group_id INTEGER REFERENCES groups(id),    -- NULL = 全员；否则定向到某个参与组
    revealed        INTEGER NOT NULL DEFAULT 0,
    revealed_at_ms  INTEGER,                          -- 揭示时的虚拟时钟毫秒
    UNIQUE (room_id, seq)
);

CREATE TABLE IF NOT EXISTS events (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,     -- 全局单调递增事件 ID
    room_id    INTEGER NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
    group_id   INTEGER,                               -- NULL = 房间级（含全员卡片）事件；否则仅该组可见
    type       TEXT NOT NULL,
    payload    TEXT NOT NULL,                         -- JSON
    created_at REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_cards_due ON cards(room_id, revealed, trigger_seconds, seq);
CREATE INDEX IF NOT EXISTS idx_events_room ON events(room_id, id);
"""

# 依赖分组列的索引：放在迁移之后创建（旧库执行 SCHEMA 时这些列可能尚不存在）。
POST_MIGRATION_INDEXES = (
    "CREATE INDEX IF NOT EXISTS idx_cards_group ON cards(room_id, target_group_id)",
    "CREATE INDEX IF NOT EXISTS idx_groups_room ON groups(room_id, id)",
)

# 旧版本数据库（分组功能上线前）的轻量迁移：仅加列，历史卡片默认面向“全员”。
MIGRATIONS = (
    (
        "groups",
        """CREATE TABLE IF NOT EXISTS groups (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            room_id     INTEGER NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
            name        TEXT NOT NULL,
            join_code   TEXT NOT NULL UNIQUE,
            created_at  REAL NOT NULL
        )""",
    ),
    (
        "cards.target_group_id",
        "ALTER TABLE cards ADD COLUMN target_group_id INTEGER REFERENCES groups(id)",
    ),
    (
        "events.group_id",
        "ALTER TABLE events ADD COLUMN group_id INTEGER",
    ),
)


def connect() -> sqlite3.Connection:
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    return any(row["name"] == column for row in conn.execute(f"PRAGMA table_info({table})"))


def migrate(conn: sqlite3.Connection) -> None:
    for name, sql in MIGRATIONS:
        if "." in name:
            table, column = name.split(".", 1)
            if _column_exists(conn, table, column):
                continue
        else:
            # 表存在即视为已迁移（CREATE 本身也是 IF NOT EXISTS）
            row = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (name,)
            ).fetchone()
            if row is not None:
                continue
        conn.execute(sql)
    conn.commit()


def init_schema(conn: sqlite3.Connection) -> None:
    """建表、迁移，并把进程停止时仍处于 running 的房间安全冻结为 paused。

    虚拟时钟在暂停时不推进：重启后这些房间保持暂停，已流逝时间按
    停机前的墙钟差结算进 elapsed_ms，不会丢失也不会偷偷前进。
    """
    conn.executescript(SCHEMA)
    migrate(conn)
    for sql in POST_MIGRATION_INDEXES:
        conn.execute(sql)
    now = time.time()
    rows = conn.execute("SELECT * FROM rooms WHERE status = 'running'").fetchall()
    for row in rows:
        elapsed = row["elapsed_ms"]
        if row["last_resume_at"] is not None:
            elapsed += int((now - row["last_resume_at"]) * 1000)
        conn.execute(
            "UPDATE rooms SET status = 'paused', elapsed_ms = ?, last_resume_at = NULL WHERE id = ?",
            (max(0, elapsed), row["id"]),
        )
    conn.commit()
