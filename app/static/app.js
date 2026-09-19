'use strict';

/* 情景注入台前端
 *
 * 同步协议：
 * 1. 打开 WebSocket 时带上 last_event_id（已应用的最大事件 ID）；
 * 2. 服务端先推权威快照（snapshot）——全量替换本地状态，水位 = snapshot.last_event_id；
 * 3. 之后只应用 id > 水位的增量事件，旧事件一律丢弃，状态绝不回退。
 */

const $ = (sel) => document.querySelector(sel);

const state = {
  code: null,
  role: null, // 'host' | 'player'
  roomName: '',
  status: 'idle',
  cards: [],
  lastEventId: 0,
  clock: { status: 'idle', elapsedMs: 0, syncedAt: performance.now() },
  ws: null,
  reconnectDelay: 1000,
};

const STATUS_TEXT = { idle: '待机', running: '进行中', paused: '已暂停' };

/* ---------------------------------------------------------- 视图切换 */

function showLanding() {
  $('#view-landing').classList.remove('hidden');
  $('#view-room').classList.add('hidden');
}

function showRoom() {
  $('#view-landing').classList.add('hidden');
  $('#view-room').classList.remove('hidden');
  $('#host-console').classList.toggle('hidden', state.role !== 'host');
  $('#cards-title').textContent = state.role === 'host' ? '注入卡片（全部）' : '已揭示的情景注入';
}

/* ---------------------------------------------------------- 渲染 */

function fmt(ms) {
  const s = Math.max(0, Math.floor(ms / 1000));
  const h = String(Math.floor(s / 3600)).padStart(2, '0');
  const m = String(Math.floor((s % 3600) / 60)).padStart(2, '0');
  const sec = String(s % 60).padStart(2, '0');
  return `${h}:${m}:${sec}`;
}

function renderMeta() {
  $('#room-name').textContent = state.roomName || '未命名演练';
  $('#room-code').textContent = state.code;
  const st = $('#status');
  st.textContent = STATUS_TEXT[state.status] || state.status;
  st.className = 'status' + (state.status === 'running' ? ' running' : state.status === 'paused' ? ' paused' : '');
  renderControls();
}

function renderControls() {
  if (state.role !== 'host') return;
  const s = state.status;
  $('#btn-start').disabled = s !== 'idle';
  $('#btn-pause').disabled = s !== 'running';
  $('#btn-resume').disabled = s !== 'paused';
  $('#btn-reset').disabled = s === 'idle';
  const editing = s !== 'running';
  $('#card-form').querySelectorAll('input, textarea, button').forEach((el) => { el.disabled = !editing; });
  $('#card-form-hint').textContent = editing ? '' : '时钟运行中，暂停后才能编辑卡片';
}

function sortedCards() {
  return [...state.cards].sort((a, b) => a.trigger_seconds - b.trigger_seconds || a.seq - b.seq);
}

function renderCards() {
  const list = $('#card-list');
  list.innerHTML = '';
  const all = sortedCards();
  const visible = state.role === 'host' ? all : all.filter((c) => c.revealed);
  $('#cards-empty').classList.toggle('hidden', visible.length > 0);
  if (state.role === 'player') {
    $('#cards-empty').textContent = all.some((c) => c.revealed) ? '' : '等待主持人注入情景…';
  } else {
    $('#cards-empty').textContent = '暂无卡片，请在上方添加';
  }
  for (const card of visible) {
    const li = document.createElement('li');
    li.className = 'card-item ' + (card.revealed ? 'revealed' : 'pending');

    const meta = document.createElement('div');
    meta.className = 'card-meta';
    const left = document.createElement('span');
    left.innerHTML = `<span class="card-trigger">T+${fmt(card.trigger_seconds * 1000)}</span> · #${card.seq}`;
    meta.appendChild(left);

    const right = document.createElement('span');
    if (card.revealed) {
      right.innerHTML = `<span class="badge badge-revealed">已揭示 ${fmt(card.revealed_at_ms || 0)}</span>`;
    } else {
      right.innerHTML = '<span class="badge badge-pending">待触发</span>';
    }
    if (state.role === 'host' && state.status !== 'running') {
      const del = document.createElement('button');
      del.className = 'card-del';
      del.textContent = '删除';
      del.onclick = () => api('DELETE', `/api/rooms/${state.code}/cards/${card.id}`).catch(showError);
      right.appendChild(del);
    }
    meta.appendChild(right);

    const title = document.createElement('div');
    title.className = 'card-title';
    title.textContent = card.title;

    li.appendChild(meta);
    li.appendChild(title);
    if (card.content) {
      const content = document.createElement('div');
      content.className = 'card-content';
      content.textContent = card.content;
      li.appendChild(content);
    }
    list.appendChild(li);
  }
}

function renderAll() {
  renderMeta();
  renderCards();
}

/* 本地时钟渲染：以快照/事件同步点为基准，running 时本地外推，暂停即冻结 */
setInterval(() => {
  if (!state.code) return;
  const c = state.clock;
  const elapsed = c.status === 'running' ? c.elapsedMs + (performance.now() - c.syncedAt) : c.elapsedMs;
  $('#clock').textContent = fmt(elapsed);
}, 200);

function syncClock(status, elapsedMs) {
  state.status = status;
  state.clock = { status, elapsedMs, syncedAt: performance.now() };
}

/* ---------------------------------------------------------- 状态应用 */

function applySnapshot(msg) {
  state.roomName = msg.room.name;
  state.cards = msg.cards;
  state.lastEventId = msg.last_event_id; // 权威水位
  syncClock(msg.room.status, msg.room.elapsed_ms);
  renderAll();
}

function applyEvent(msg) {
  if (msg.id <= state.lastEventId) return; // 不回退：丢弃旧事件
  state.lastEventId = msg.id;
  const p = msg.payload;
  switch (msg.type) {
    case 'card_added':
      if (!state.cards.some((c) => c.id === p.id)) state.cards.push(p);
      break;
    case 'card_removed':
      state.cards = state.cards.filter((c) => c.id !== p.id);
      break;
    case 'card_revealed': {
      const card = state.cards.find((c) => c.id === p.id);
      if (card) Object.assign(card, p);
      else state.cards.push(p);
      break;
    }
    case 'clock':
      syncClock(p.status, p.elapsed_ms);
      break;
    case 'reset':
      state.cards.forEach((c) => { c.revealed = false; c.revealed_at_ms = null; });
      syncClock('idle', 0);
      break;
  }
  renderAll();
}

/* ---------------------------------------------------------- WebSocket */

function setConn(up) {
  const el = $('#conn');
  el.textContent = up ? '已连接' : '连接断开，重连中…';
  el.className = 'conn ' + (up ? 'conn-up' : 'conn-down');
}

function connect() {
  const proto = location.protocol === 'https:' ? 'wss' : 'ws';
  const ws = new WebSocket(`${proto}://${location.host}/ws/${state.code}?last_event_id=${state.lastEventId}`);
  state.ws = ws;

  ws.onopen = () => { state.reconnectDelay = 1000; setConn(true); };
  ws.onmessage = (e) => {
    const msg = JSON.parse(e.data);
    if (msg.kind === 'snapshot') applySnapshot(msg);
    else if (msg.kind === 'event') applyEvent(msg);
    else if (msg.kind === 'error') { alert(msg.message); leaveRoom(); }
  };
  ws.onclose = () => {
    setConn(false);
    if (!state.code) return;
    setTimeout(connect, state.reconnectDelay);
    state.reconnectDelay = Math.min(state.reconnectDelay * 2, 5000);
  };
}

/* ---------------------------------------------------------- API */

async function api(method, path, body) {
  const res = await fetch(path, {
    method,
    headers: body ? { 'Content-Type': 'application/json' } : {},
    body: body ? JSON.stringify(body) : undefined,
  });
  if (!res.ok) {
    const detail = await res.json().catch(() => ({}));
    throw new Error(detail.detail || `请求失败 (${res.status})`);
  }
  return res.json();
}

function showError(err) {
  const el = $('#landing-error');
  el.textContent = err.message || String(err);
  setTimeout(() => { el.textContent = ''; }, 4000);
}

/* ---------------------------------------------------------- 流程 */

function enterRoom(code, role) {
  state.code = code;
  state.role = role;
  state.lastEventId = 0;
  state.cards = [];
  location.hash = `#/${role}/${code}`;
  localStorage.setItem(`inject-role-${code}`, role);
  showRoom();
  renderAll();
  connect();
}

function leaveRoom() {
  if (state.ws) { state.ws.onclose = null; state.ws.close(); }
  state.code = null;
  location.hash = '';
  showLanding();
}

$('#btn-create').onclick = async () => {
  try {
    const name = $('#create-name').value.trim();
    const room = await api('POST', '/api/rooms', { name });
    enterRoom(room.code, 'host');
  } catch (err) { showError(err); }
};

$('#btn-join').onclick = async () => {
  const code = $('#join-code').value.trim().toUpperCase();
  if (!code) return;
  try {
    await api('GET', `/api/rooms/${code}`);
    enterRoom(code, 'player');
  } catch (err) { showError(new Error('房间不存在，请检查房间码')); }
};

$('#room-code').onclick = () => {
  navigator.clipboard?.writeText(state.code);
  $('#room-code').textContent = '已复制';
  setTimeout(() => { $('#room-code').textContent = state.code; }, 1200);
};

$('#btn-start').onclick  = () => api('POST', `/api/rooms/${state.code}/start`).catch(showError);
$('#btn-pause').onclick  = () => api('POST', `/api/rooms/${state.code}/pause`).catch(showError);
$('#btn-resume').onclick = () => api('POST', `/api/rooms/${state.code}/resume`).catch(showError);
$('#btn-reset').onclick  = () => api('POST', `/api/rooms/${state.code}/reset`).catch(showError);

$('#card-form').onsubmit = async (e) => {
  e.preventDefault();
  const body = {
    title: $('#card-title').value.trim(),
    content: $('#card-content').value.trim(),
    trigger_seconds: parseInt($('#card-trigger').value, 10) || 0,
  };
  try {
    await api('POST', `/api/rooms/${state.code}/cards`, body);
    $('#card-title').value = '';
    $('#card-content').value = '';
    $('#card-trigger').value = '';
    $('#card-title').focus();
  } catch (err) { showError(err); }
};

/* 刷新恢复：根据 hash 与本地角色记录重进房间（走完整的快照+增量协议） */
(function boot() {
  const m = location.hash.match(/^#\/(host|player)\/([A-Z0-9]{6})$/);
  if (m) enterRoom(m[2], m[1]);
  else showLanding();
})();
