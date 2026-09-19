"""情景注入台 —— FastAPI 主程序。

职责：
- 房间 / 注入卡片的 REST 管理
- 可暂停 / 继续的虚拟时钟（暂停期间绝不推进）
- 后台 tick：按 (trigger_seconds, seq) 稳定次序揭示到期卡片
- WebSocket：先下发权威快照，再广播单调递增的增量事件
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import secrets
import sqlite3
import string
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from . import db as dbmod

log = logging.getLogger("inject")
logging.basicConfig(level=logging.INFO)

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
TICK_INTERVAL = float(os.environ.get("TICK_INTERVAL", "0.25"))
CODE_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"  # 去掉易混淆字符

db: sqlite3.Connection
# 所有读改写 + 广播都在这把锁下串行，保证“快照先于其后发生的事件”这一顺序。
state_lock = asyncio.Lock()


# ---------------------------------------------------------------- 工具

def now_ms() -> int:
    return int(time.time() * 1000)


def room_elapsed_ms(room: sqlite3.Row) -> int:
    """虚拟时钟当前值：running 时 = 冻结值 + 本次运行段，否则 = 冻结值。"""
    elapsed = room["elapsed_ms"]
    if room["status"] == "running" and room["last_resume_at"] is not None:
        elapsed += int((time.time() - room["last_resume_at"]) * 1000)
    return max(0, elapsed)


def room_public(room: sqlite3.Row) -> dict:
    return {
        "code": room["code"],
        "name": room["name"],
        "status": room["status"],
        "elapsed_ms": room_elapsed_ms(room),
        "server_now_ms": now_ms(),
    }


def card_public(card: sqlite3.Row) -> dict:
    return {
        "id": card["id"],
        "seq": card["seq"],
        "trigger_seconds": card["trigger_seconds"],
        "title": card["title"],
        "content": card["content"],
        "revealed": bool(card["revealed"]),
        "revealed_at_ms": card["revealed_at_ms"],
    }


def get_room_or_404(code: str) -> sqlite3.Row:
    room = db.execute("SELECT * FROM rooms WHERE code = ?", (code.upper(),)).fetchone()
    if room is None:
        raise HTTPException(status_code=404, detail="room not found")
    return room


def build_snapshot(code: str) -> dict | None:
    """权威全量状态：房间时钟 + 全部卡片（按触发时间、序号排序）+ 事件水位。"""
    room = db.execute("SELECT * FROM rooms WHERE code = ?", (code.upper(),)).fetchone()
    if room is None:
        return None
    cards = db.execute(
        "SELECT * FROM cards WHERE room_id = ? ORDER BY trigger_seconds, seq",
        (room["id"],),
    ).fetchall()
    last_id = db.execute(
        "SELECT COALESCE(MAX(id), 0) AS m FROM events WHERE room_id = ?", (room["id"],)
    ).fetchone()["m"]
    return {
        "kind": "snapshot",
        "last_event_id": last_id,
        "server_now_ms": now_ms(),
        "room": room_public(room),
        "cards": [card_public(c) for c in cards],
    }


# ---------------------------------------------------------------- 广播

class Hub:
    def __init__(self) -> None:
        self._rooms: dict[str, set[WebSocket]] = {}

    def add(self, code: str, ws: WebSocket) -> None:
        self._rooms.setdefault(code, set()).add(ws)

    def discard(self, code: str, ws: WebSocket) -> None:
        subs = self._rooms.get(code)
        if subs:
            subs.discard(ws)
            if not subs:
                self._rooms.pop(code, None)

    async def broadcast(self, code: str, message: dict) -> None:
        for ws in list(self._rooms.get(code, ())):
            try:
                await ws.send_json(message)
            except Exception:
                self.discard(code, ws)


hub = Hub()


async def publish(code: str, room_id: int, type_: str, payload: dict) -> None:
    """落库一条递增事件并广播。必须在 state_lock 下调用。"""
    cur = db.execute(
        "INSERT INTO events (room_id, type, payload, created_at) VALUES (?, ?, ?, ?)",
        (room_id, type_, json.dumps(payload, ensure_ascii=False), time.time()),
    )
    db.commit()
    await hub.broadcast(code, {
        "kind": "event",
        "id": cur.lastrowid,
        "type": type_,
        "payload": payload,
        "server_now_ms": now_ms(),
    })


# ---------------------------------------------------------------- 时钟 tick

async def ticker_loop() -> None:
    """周期性扫描 running 房间，按 (trigger_seconds, seq) 稳定揭示到期卡片。"""
    while True:
        await asyncio.sleep(TICK_INTERVAL)
        try:
            async with state_lock:
                running = db.execute("SELECT * FROM rooms WHERE status = 'running'").fetchall()
                for room in running:
                    elapsed = room_elapsed_ms(room)
                    due = db.execute(
                        """SELECT * FROM cards
                           WHERE room_id = ? AND revealed = 0 AND trigger_seconds * 1000 <= ?
                           ORDER BY trigger_seconds, seq""",
                        (room["id"], elapsed),
                    ).fetchall()
                    for card in due:
                        db.execute(
                            "UPDATE cards SET revealed = 1, revealed_at_ms = ? WHERE id = ?",
                            (elapsed, card["id"]),
                        )
                        db.commit()
                        fresh = db.execute("SELECT * FROM cards WHERE id = ?", (card["id"],)).fetchone()
                        await publish(room["code"], room["id"], "card_revealed", card_public(fresh))
                        log.info("room %s revealed card #%s (%s)", room["code"], card["seq"], card["title"])
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("ticker error")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global db
    db = dbmod.connect()
    dbmod.init_schema(db)  # 启动即完成数据库初始化
    task = asyncio.create_task(ticker_loop())
    log.info("inject console ready, db=%s", dbmod.DB_PATH)
    yield
    task.cancel()
    db.close()


app = FastAPI(title="情景注入台", lifespan=lifespan)


# ---------------------------------------------------------------- REST：房间

class RoomIn(BaseModel):
    name: str = Field(default="", max_length=120)


@app.post("/api/rooms", status_code=201)
async def create_room(body: RoomIn):
    async with state_lock:
        code = "".join(secrets.choice(CODE_ALPHABET) for _ in range(6))
        db.execute(
            "INSERT INTO rooms (code, name, created_at) VALUES (?, ?, ?)",
            (code, body.name.strip(), time.time()),
        )
        db.commit()
        return {"code": code, "name": body.name.strip()}


@app.get("/api/rooms/{code}")
async def get_room(code: str):
    async with state_lock:
        snapshot = build_snapshot(code)
        if snapshot is None:
            raise HTTPException(status_code=404, detail="room not found")
        return snapshot


# ---------------------------------------------------------------- REST：卡片

class CardIn(BaseModel):
    title: str = Field(min_length=1, max_length=200)
    content: str = Field(default="", max_length=4000)
    trigger_seconds: int = Field(ge=0, le=24 * 3600)


@app.post("/api/rooms/{code}/cards", status_code=201)
async def add_card(code: str, body: CardIn):
    async with state_lock:
        room = get_room_or_404(code)
        if room["status"] == "running":
            raise HTTPException(status_code=409, detail="clock is running; pause before editing cards")
        seq = db.execute(
            "SELECT COALESCE(MAX(seq), 0) + 1 AS s FROM cards WHERE room_id = ?", (room["id"],)
        ).fetchone()["s"]
        cur = db.execute(
            "INSERT INTO cards (room_id, seq, trigger_seconds, title, content) VALUES (?, ?, ?, ?, ?)",
            (room["id"], seq, body.trigger_seconds, body.title.strip(), body.content),
        )
        db.commit()
        card = db.execute("SELECT * FROM cards WHERE id = ?", (cur.lastrowid,)).fetchone()
        payload = card_public(card)
        await publish(room["code"], room["id"], "card_added", payload)
        return payload


@app.delete("/api/rooms/{code}/cards/{card_id}")
async def delete_card(code: str, card_id: int):
    async with state_lock:
        room = get_room_or_404(code)
        if room["status"] == "running":
            raise HTTPException(status_code=409, detail="clock is running; pause before editing cards")
        cur = db.execute("DELETE FROM cards WHERE id = ? AND room_id = ?", (card_id, room["id"]))
        db.commit()
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="card not found")
        await publish(room["code"], room["id"], "card_removed", {"id": card_id})
        return {"ok": True}


# ---------------------------------------------------------------- REST：时钟控制

async def _clock_action(code: str, expect: tuple[str, ...], action: str, sql: str, params: tuple):
    async with state_lock:
        room = get_room_or_404(code)
        if room["status"] not in expect:
            raise HTTPException(status_code=409, detail=f"cannot {action} while status={room['status']}")
        db.execute(sql, (*params, room["id"]))
        db.commit()
        fresh = db.execute("SELECT * FROM rooms WHERE id = ?", (room["id"],)).fetchone()
        payload = {"action": action, "status": fresh["status"], "elapsed_ms": room_elapsed_ms(fresh)}
        await publish(room["code"], room["id"], "clock", payload)
        return payload


@app.post("/api/rooms/{code}/start")
async def start_room(code: str):
    return await _clock_action(
        code, ("idle",), "started",
        "UPDATE rooms SET status = 'running', elapsed_ms = 0, last_resume_at = ? WHERE id = ?",
        (time.time(),),
    )


@app.post("/api/rooms/{code}/pause")
async def pause_room(code: str):
    async with state_lock:
        room = get_room_or_404(code)
        if room["status"] != "running":
            raise HTTPException(status_code=409, detail=f"cannot pause while status={room['status']}")
        elapsed = room_elapsed_ms(room)  # 冻结当前值，暂停期间不再推进
        db.execute(
            "UPDATE rooms SET status = 'paused', elapsed_ms = ?, last_resume_at = NULL WHERE id = ?",
            (elapsed, room["id"]),
        )
        db.commit()
        payload = {"action": "paused", "status": "paused", "elapsed_ms": elapsed}
        await publish(room["code"], room["id"], "clock", payload)
        return payload


@app.post("/api/rooms/{code}/resume")
async def resume_room(code: str):
    return await _clock_action(
        code, ("paused",), "resumed",
        "UPDATE rooms SET status = 'running', last_resume_at = ? WHERE id = ?",
        (time.time(),),
    )


@app.post("/api/rooms/{code}/reset")
async def reset_room(code: str):
    async with state_lock:
        room = get_room_or_404(code)
        if room["status"] == "running":
            raise HTTPException(status_code=409, detail="pause before reset")
        db.execute(
            "UPDATE rooms SET status = 'idle', elapsed_ms = 0, last_resume_at = NULL WHERE id = ?",
            (room["id"],),
        )
        db.execute("UPDATE cards SET revealed = 0, revealed_at_ms = NULL WHERE room_id = ?", (room["id"],))
        db.commit()
        payload = {"action": "reset", "status": "idle", "elapsed_ms": 0}
        await publish(room["code"], room["id"], "reset", payload)
        return payload


# ---------------------------------------------------------------- WebSocket

@app.websocket("/ws/{code}")
async def room_ws(websocket: WebSocket, code: str, last_event_id: int = 0):
    """重连协议：客户端带上已应用的最大事件 ID。

    服务端在锁内先登记连接、再发权威快照（快照水位不低于客户端水位），
    之后的增量事件按递增 ID 广播；客户端丢弃 id <= 水位的事件，状态不回退。
    """
    code = code.upper()
    await websocket.accept()
    async with state_lock:
        snapshot = build_snapshot(code)
        if snapshot is None:
            await websocket.send_json({"kind": "error", "message": "room not found"})
            await websocket.close(code=4404)
            return
        hub.add(code, websocket)
        snapshot["last_event_id"] = max(snapshot["last_event_id"], last_event_id)
        await websocket.send_json(snapshot)
    try:
        while True:
            await websocket.receive_text()  # 客户端无需上行；仅用于感知断线
    except WebSocketDisconnect:
        pass
    finally:
        hub.discard(code, websocket)


# ---------------------------------------------------------------- 静态资源

@app.get("/")
async def index():
    return FileResponse(STATIC_DIR / "index.html")


app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
