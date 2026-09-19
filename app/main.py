"""情景注入台 —— FastAPI 主程序。

职责：
- 房间 / 参与分组 / 注入卡片的 REST 管理
- 可暂停 / 继续的虚拟时钟（暂停期间绝不推进）
- 后台 tick：按 (trigger_seconds, seq) 稳定次序揭示到期卡片
- WebSocket：先下发权威快照（按连接的身份过滤），再广播单调递增的增量事件

身份与可见性：
- 主持人凭房间码进入，房间码即主持凭证：快照含全部卡片与全部分组，收到全部事件。
- 参与者凭“组加入码”进入：服务端按该组生成快照（仅全员卡片 + 本组卡片），
  后续事件按 events.group_id / 卡片 target_group_id 过滤；未命中的卡片在触发
  前后都不会通过 REST 或 WebSocket 离开服务端。
- 未命中的事件不下发任何内容，仅把同 ID 的 watermark 发给对应连接，保证每个
  连接看到的事件水位同样单调、与全局顺序一致；刷新或断线重连后视图不回退、
  不混入他组内容。
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import secrets
import sqlite3
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
CODE_LEN = 6
MAX_GROUPS = 8  # 每个房间允许 2～8 个参与组（由主持人按需创建）

db: sqlite3.Connection
# 所有读改写 + 广播都在这把锁下串行，保证“快照先于其后发生的事件”这一顺序。
state_lock = asyncio.Lock()


# ---------------------------------------------------------------- 工具

def now_ms() -> int:
    return int(time.time() * 1000)


def gen_unique_code(exists) -> str:
    """生成一个 6 位码，exists(code) 为真则重试。房间码与组加入码共用同一码空间。"""
    for _ in range(50):
        code = "".join(secrets.choice(CODE_ALPHABET) for _ in range(CODE_LEN))
        if not exists(code):
            return code
    raise RuntimeError("failed to allocate unique code")


def code_taken(code: str) -> bool:
    return db.execute(
        "SELECT 1 FROM rooms WHERE code = ? UNION ALL SELECT 1 FROM groups WHERE join_code = ? LIMIT 1",
        (code, code),
    ).fetchone() is not None


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


def group_public(group: sqlite3.Row) -> dict:
    return {"id": group["id"], "name": group["name"], "join_code": group["join_code"]}


def card_public(card: sqlite3.Row) -> dict:
    return {
        "id": card["id"],
        "seq": card["seq"],
        "trigger_seconds": card["trigger_seconds"],
        "title": card["title"],
        "content": card["content"],
        "target_group_id": card["target_group_id"],
        "revealed": bool(card["revealed"]),
        "revealed_at_ms": card["revealed_at_ms"],
    }


def get_room_or_404(code: str) -> sqlite3.Row:
    room = db.execute("SELECT * FROM rooms WHERE code = ?", (code.upper(),)).fetchone()
    if room is None:
        raise HTTPException(status_code=404, detail="room not found")
    return room


def get_group_or_404(room_id: int, group_id: int) -> sqlite3.Row:
    group = db.execute(
        "SELECT * FROM groups WHERE id = ? AND room_id = ?", (group_id, room_id)
    ).fetchone()
    if group is None:
        raise HTTPException(status_code=404, detail="group not found")
    return group


def card_visible_to(card: sqlite3.Row, group_id: int | None) -> bool:
    """group_id=None 为主持人（全部可见）；否则仅全员卡片与本组卡片可见。"""
    return group_id is None or card["target_group_id"] is None or card["target_group_id"] == group_id


def build_snapshot(room: sqlite3.Row, group_id: int | None = None) -> dict:
    """权威全量状态，按连接身份过滤。

    主持人（group_id=None）：全部卡片 + 全部分组（含各自加入码）。
    参与者：仅全员 / 本组卡片 + 本组信息（含本组加入码，便于刷新恢复）。
    水位始终取房间全局最大事件 ID，保证重连快照水位不落后。
    """
    cards = db.execute(
        "SELECT * FROM cards WHERE room_id = ? ORDER BY trigger_seconds, seq",
        (room["id"],),
    ).fetchall()
    cards = [c for c in cards if card_visible_to(c, group_id)]
    last_id = db.execute(
        "SELECT COALESCE(MAX(id), 0) AS m FROM events WHERE room_id = ?", (room["id"],)
    ).fetchone()["m"]
    snapshot = {
        "kind": "snapshot",
        "last_event_id": last_id,
        "server_now_ms": now_ms(),
        "room": room_public(room),
        "cards": [card_public(c) for c in cards],
    }
    if group_id is None:
        groups = db.execute(
            "SELECT * FROM groups WHERE room_id = ? ORDER BY id", (room["id"],)
        ).fetchall()
        snapshot["groups"] = [group_public(g) for g in groups]
        snapshot["viewer"] = {"role": "host"}
    else:
        group = db.execute("SELECT * FROM groups WHERE id = ?", (group_id,)).fetchone()
        snapshot["groups"] = [group_public(group)] if group is not None else []
        snapshot["viewer"] = {"role": "player", "group_id": group_id}
    return snapshot


# ---------------------------------------------------------------- 广播

class Hub:
    """按房间维护连接；每个连接带身份（None=主持人，int=参与组）。"""

    def __init__(self) -> None:
        self._rooms: dict[str, dict[WebSocket, int | None]] = {}

    def add(self, code: str, ws: WebSocket, group_id: int | None) -> None:
        self._rooms.setdefault(code, {})[ws] = group_id

    def discard(self, code: str, ws: WebSocket) -> None:
        subs = self._rooms.get(code)
        if subs:
            subs.pop(ws, None)
            if not subs:
                self._rooms.pop(code, None)

    def members(self, code: str) -> dict[WebSocket, int | None]:
        return self._rooms.get(code, {})


hub = Hub()


async def publish(
    code: str,
    room_id: int,
    type_: str,
    payload: dict,
    group_id: int | None = None,
) -> None:
    """落库一条递增事件并按身份广播。必须在 state_lock 下调用。

    group_id=None：房间级事件（时钟 / 全员卡片 / 分组管理，管理类事件仅主持人可见）。
    group_id=int：定向事件，仅该组连接与主持人可见。
    对收不到 payload 的连接，改发同 ID 的 watermark —— 不泄露任何内容，
    同时让该连接的水位推进到全局最新，顺序保持单调。
    """
    cur = db.execute(
        "INSERT INTO events (room_id, group_id, type, payload, created_at) VALUES (?, ?, ?, ?, ?)",
        (room_id, group_id, type_, json.dumps(payload, ensure_ascii=False), time.time()),
    )
    db.commit()
    event_id = cur.lastrowid
    full = {
        "kind": "event",
        "id": event_id,
        "type": type_,
        "payload": payload,
        "server_now_ms": now_ms(),
    }
    watermark = {"kind": "watermark", "id": event_id}

    # 分组管理类事件（创建 / 改名 / 删除）只发给主持人；参与者连“有无此事件”
    # 都无需感知，直接推进水位即可。group_renamed 例外：被改名的那一组需要
    # 收到，好让其页面上的组名同步。
    group_admin_types = {"group_added", "group_removed"}

    for ws, viewer_group in list(hub.members(code).items()):
        visible = False
        if viewer_group is None:
            visible = True
        elif type_ in group_admin_types:
            visible = False
        elif type_ == "group_renamed":
            visible = group_id is not None and payload.get("id") == viewer_group
        elif group_id is None:
            visible = True
        else:
            visible = group_id == viewer_group
        try:
            await ws.send_json(full if visible else watermark)
        except Exception:
            hub.discard(code, ws)


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
                        # 定向卡片的揭示事件仅该组可见；他组连接只收到 watermark。
                        await publish(
                            room["code"], room["id"], "card_revealed",
                            card_public(fresh), fresh["target_group_id"],
                        )
                        log.info(
                            "room %s revealed card #%s (%s) group=%s",
                            room["code"], card["seq"], card["title"],
                            fresh["target_group_id"] or "ALL",
                        )
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("ticker error")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global db
    db = dbmod.connect()
    dbmod.init_schema(db)  # 启动即完成数据库初始化与迁移
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
        code = gen_unique_code(code_taken)
        db.execute(
            "INSERT INTO rooms (code, name, created_at) VALUES (?, ?, ?)",
            (code, body.name.strip(), time.time()),
        )
        db.commit()
        return {"code": code, "name": body.name.strip()}


@app.get("/api/rooms/{code}")
async def get_room(code: str):
    """房间码 = 主持凭证：返回含全部分组 / 全部卡片的完整快照。"""
    async with state_lock:
        room = get_room_or_404(code)
        return build_snapshot(room, None)


@app.get("/api/join/{join_code}")
async def join_by_code(join_code: str):
    """参与者凭组加入码进入：只返回该组可见的快照（不含任何他组信息）。"""
    async with state_lock:
        row = db.execute(
            """SELECT g.id AS gid, r.* FROM groups g JOIN rooms r ON r.id = g.room_id
               WHERE g.join_code = ?""",
            (join_code.upper(),),
        ).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="invalid join code")
        room = db.execute("SELECT * FROM rooms WHERE id = ?", (row["id"],)).fetchone()
        return build_snapshot(room, row["gid"])


# ---------------------------------------------------------------- REST：分组

class GroupIn(BaseModel):
    name: str = Field(min_length=1, max_length=80)


class GroupRename(BaseModel):
    name: str = Field(min_length=1, max_length=80)


@app.post("/api/rooms/{code}/groups", status_code=201)
async def create_group(code: str, body: GroupIn):
    async with state_lock:
        room = get_room_or_404(code)
        count = db.execute(
            "SELECT COUNT(*) AS c FROM groups WHERE room_id = ?", (room["id"],)
        ).fetchone()["c"]
        if count >= MAX_GROUPS:
            raise HTTPException(status_code=409, detail=f"at most {MAX_GROUPS} groups per room")
        join_code = gen_unique_code(code_taken)
        name = body.name.strip()
        cur = db.execute(
            "INSERT INTO groups (room_id, name, join_code, created_at) VALUES (?, ?, ?, ?)",
            (room["id"], name, join_code, time.time()),
        )
        db.commit()
        group = db.execute("SELECT * FROM groups WHERE id = ?", (cur.lastrowid,)).fetchone()
        payload = group_public(group)
        await publish(room["code"], room["id"], "group_added", payload)
        return payload


@app.patch("/api/rooms/{code}/groups/{group_id}")
async def rename_group(code: str, group_id: int, body: GroupRename):
    async with state_lock:
        room = get_room_or_404(code)
        group = get_group_or_404(room["id"], group_id)
        name = body.name.strip()
        db.execute("UPDATE groups SET name = ? WHERE id = ?", (name, group_id))
        db.commit()
        fresh = db.execute("SELECT * FROM groups WHERE id = ?", (group_id,)).fetchone()
        payload = group_public(fresh)
        # 主持人收到完整 payload（含加入码）；被改名的那一组只需要新名字。
        await publish(room["code"], room["id"], "group_renamed", payload, group_id)
        return payload


@app.delete("/api/rooms/{code}/groups/{group_id}")
async def delete_group(code: str, group_id: int):
    async with state_lock:
        room = get_room_or_404(code)
        get_group_or_404(room["id"], group_id)
        used = db.execute(
            "SELECT COUNT(*) AS c FROM cards WHERE target_group_id = ?", (group_id,)
        ).fetchone()["c"]
        if used:
            raise HTTPException(status_code=409, detail="group is referenced by cards; remove or retarget them first")
        db.execute("DELETE FROM groups WHERE id = ?", (group_id,))
        db.commit()
        payload = {"id": group_id}
        await publish(room["code"], room["id"], "group_removed", payload)
        return {"ok": True}


# ---------------------------------------------------------------- REST：卡片

class CardIn(BaseModel):
    title: str = Field(min_length=1, max_length=200)
    content: str = Field(default="", max_length=4000)
    trigger_seconds: int = Field(ge=0, le=24 * 3600)
    target_group_id: int | None = None


def resolve_card_target(room_id: int, target_group_id: int | None) -> int | None:
    if target_group_id is None:
        return None
    row = db.execute(
        "SELECT id FROM groups WHERE id = ? AND room_id = ?", (target_group_id, room_id)
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=400, detail="target group not found in this room")
    return target_group_id


@app.post("/api/rooms/{code}/cards", status_code=201)
async def add_card(code: str, body: CardIn):
    async with state_lock:
        room = get_room_or_404(code)
        if room["status"] == "running":
            raise HTTPException(status_code=409, detail="clock is running; pause before editing cards")
        target = resolve_card_target(room["id"], body.target_group_id)
        seq = db.execute(
            "SELECT COALESCE(MAX(seq), 0) + 1 AS s FROM cards WHERE room_id = ?", (room["id"],)
        ).fetchone()["s"]
        cur = db.execute(
            """INSERT INTO cards (room_id, seq, trigger_seconds, title, content, target_group_id)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (room["id"], seq, body.trigger_seconds, body.title.strip(), body.content, target),
        )
        db.commit()
        card = db.execute("SELECT * FROM cards WHERE id = ?", (cur.lastrowid,)).fetchone()
        payload = card_public(card)
        await publish(room["code"], room["id"], "card_added", payload, target)
        return payload


@app.delete("/api/rooms/{code}/cards/{card_id}")
async def delete_card(code: str, card_id: int):
    async with state_lock:
        room = get_room_or_404(code)
        if room["status"] == "running":
            raise HTTPException(status_code=409, detail="clock is running; pause before editing cards")
        card = db.execute(
            "SELECT * FROM cards WHERE id = ? AND room_id = ?", (card_id, room["id"])
        ).fetchone()
        if card is None:
            raise HTTPException(status_code=404, detail="card not found")
        db.execute("DELETE FROM cards WHERE id = ?", (card_id,))
        db.commit()
        payload = {"id": card_id}
        await publish(room["code"], room["id"], "card_removed", payload, card["target_group_id"])
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
            (elapsed, room["id"],),
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
    """重置：时钟归零、收回全部已揭示卡片；分组、加入码与卡片目标均保留。"""
    async with state_lock:
        room = get_room_or_404(code)
        if room["status"] == "running":
            raise HTTPException(status_code=409, detail="pause before reset")
        db.execute(
            "UPDATE rooms SET status = 'idle', elapsed_ms = 0, last_resume_at = NULL WHERE id = ?",
            (room["id"],),
        )
        # 只复位揭示状态，绝不动 target_group_id（卡片目标随重置保留）。
        db.execute("UPDATE cards SET revealed = 0, revealed_at_ms = NULL WHERE room_id = ?", (room["id"],))
        db.commit()
        payload = {"action": "reset", "status": "idle", "elapsed_ms": 0}
        await publish(room["code"], room["id"], "reset", payload)
        return payload


# ---------------------------------------------------------------- WebSocket

@app.websocket("/ws/{code}")
async def room_ws(websocket: WebSocket, code: str, last_event_id: int = 0):
    """重连协议：客户端带上已应用的最大事件 ID。

    路径码可以是房间码（主持人，看全部）或组加入码（参与者，仅本组）。
    服务端在锁内先登记连接、再下发按身份过滤的权威快照（快照水位不低于
    客户端水位）；之后增量事件按身份过滤下发，未命中的事件以 watermark
    推进水位。客户端丢弃 id <= 水位的消息，刷新 / 重连后状态不回退、不串组。
    """
    code = code.upper()
    await websocket.accept()
    async with state_lock:
        room = db.execute("SELECT * FROM rooms WHERE code = ?", (code,)).fetchone()
        group_id: int | None = None
        if room is None:
            grow = db.execute(
                "SELECT id, room_id FROM groups WHERE join_code = ?", (code,)
            ).fetchone()
            if grow is None:
                await websocket.send_json({"kind": "error", "message": "room not found"})
                await websocket.close(code=4404)
                return
            group_id = grow["id"]
            room = db.execute("SELECT * FROM rooms WHERE id = ?", (grow["room_id"],)).fetchone()
        snapshot = build_snapshot(room, group_id)
        # 注意：主持人用房间码连入、参与者用组加入码连入，但订阅必须统一登记在
        # 房间码下，否则按房间广播的增量事件到不了参与者连接。
        hub.add(room["code"], websocket, group_id)
        snapshot["last_event_id"] = max(snapshot["last_event_id"], last_event_id)
        await websocket.send_json(snapshot)
    try:
        while True:
            await websocket.receive_text()  # 客户端无需上行；仅用于感知断线
    except WebSocketDisconnect:
        pass
    finally:
        hub.discard(room["code"], websocket)


# ---------------------------------------------------------------- 静态资源

@app.get("/")
async def index():
    return FileResponse(STATIC_DIR / "index.html")


app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
