'use strict';

/* 情景注入台前端
 *
 * 同步协议：
 * 1. 打开 WebSocket 时带上 last_event_id（已应用的最大事件 ID）与入口码
 *    （房间码 = 主持人；组加入码 = 参与者）；
 * 2. 服务端先推权威快照（snapshot）——全量替换本地状态，水位 = snapshot.last_event_id；
 *    参与者快照已经过服务端过滤（仅全员 + 本组），前端拿不到他组卡片；
 * 3. 之后只应用 id > 水位的增量事件，旧事件一律丢弃；未命中的事件服务端只发
 *    watermark（仅推进水位，无任何内容），状态绝不回退、绝不串组。
 */

const $ = (sel) => document.querySelector(sel);

const state = {
  code: null,          // 当前入口码：主持人=房间码，参与者=组加入码
  role: null,          // 'host' | 'player'
  roomCode: null,      // 始终是房间码（参与者快照中也有）
  groupId: null,       // 参与者所在组 id
  roomName: '',
  status: 'idle',
  groups: [],          // 主持人=全部分组；参与者=仅本组
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
  $('#room-code-label').textContent = state.role === 'host' ? '房间码' : '组加入码';
  const self = $('#player-group');
  if (state.role === 'player') {
    const g = groupById(state.groupId);
    self.textContent = g ? `（${g.name}）` : '';
    self.classList.toggle('hidden', !g);
  } else {
    self.classList.add('hidden');
  }
}

/* ---------------------------------------------------------- 渲染 */

function fmt(ms) {
  const s = Math.max(0, Math.floor(ms / 1000));
  const h = String(Math.floor(s / 3600)).padStart(2, '0');
  const m = String(Math.floor((s % 3600) / 60)).padStart(2, '0');
  const sec = String(s % 60).padStart(2, '0');
  return `${h}:${m}:${sec}`;
}

function groupById(id) {
  return state.groups.find((g) => g.id === id) || null;
}

function targetLabel(card) {
  if (card.target_group_id == null) return '全员';
  const g = groupById(card.target_group_id);
  if (g) return g.name;
  // 参与者视图里他组信息永远不会出现（服务端已过滤）；兜底仅为标识自身。
  return state.role === 'player' ? '本组' : `#${card.target_group_id}`;
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
  $('#card-form').querySelectorAll('input, textarea, button, select').forEach((el) => { el.disabled = !editing; });
  $('#card-form-hint').textContent = editing ? '' : '时钟运行中，暂停后才能编辑卡片';
}

function renderTargetOptions() {
  if (state.role !== 'host') return;
  const sel = $('#card-target');
  const current = sel.value;
  sel.innerHTML = '';
  const optAll = document.createElement('option');
  optAll.value = '';
  optAll.textContent = '目标：全员';
  sel.appendChild(optAll);
  for (const g of state.groups) {
    const opt = document.createElement('option');
    opt.value = String(g.id);
    opt.textContent = `目标：${g.name}`;
    sel.appendChild(opt);
  }
  if (current) sel.value = current;
}

function renderGroups() {
  if (state.role !== 'host') return;
  const list = $('#group-list');
  list.innerHTML = '';
  for (const g of state.groups) {
    const li = document.createElement('li');
    li.className = 'group-item';

    const name = document.createElement('input');
    name.className = 'group-name-input';
    name.value = g.name;
    name.maxLength = 80;
    name.title = '重命名后失焦或回车即保存，参与者视图同步更新';

    const codeBtn = document.createElement('button');
    codeBtn.type = 'button';
    codeBtn.className = 'group-code';
    codeBtn.textContent = g.join_code;
    codeBtn.title = '点击复制组加入码';
    codeBtn.onclick = () => {
      navigator.clipboard?.writeText(g.join_code);
      codeBtn.textContent = '已复制';
      setTimeout(() => { codeBtn.textContent = g.join_code; }, 1200);
    };

    const actions = document.createElement('div');
    actions.className = 'group-actions';
    const save = document.createElement('button');
    save.className = 'primary';
    save.textContent = '改名';
    const doRename = async () => {
      const v = name.value.trim();
      if (!v || v === g.name) { name.value = g.name; return; }
      try {
        await api('PATCH', `/api/rooms/${state.roomCode}/groups/${g.id}`, { name: v });
      } catch (err) { showError(err); name.value = g.name; }
    };
    save.onclick = doRename;
    name.onkeydown = (e) => { if (e.key === 'Enter') { e.preventDefault(); doRename(); } };

    const del = document.createElement('button');
    del.className = 'danger';
    del.textContent = '删除';
    del.onclick = async () => {
      if (!confirm(`删除分组「${g.name}」？该组加入码立即失效。`)) return;
      try {
        await api('DELETE', `/api/rooms/${state.roomCode}/groups/${g.id}`);
      } catch (err) {
        showError(new Error(err.message?.includes('referenced') ? '该分组已被卡片引用，请先删除或改投目标卡片' : err.message));
      }
    };
    actions.appendChild(save);
    actions.appendChild(del);

    li.appendChild(name);
    li.appendChild(codeBtn);
    li.appendChild(actions);
    list.appendChild(li);
  }
  $('#group-form-hint').textContent =
    state.groups.length === 0
      ? '尚未建组：卡片默认投递给全员。建议创建 2～8 个参与组。'
      : `已创建 ${state.groups.length} 个分组（上限 8 个）；被卡片引用的分组不可删除。`;
  renderTargetOptions();
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
    const targetClass = card.target_group_id == null ? 'badge-target target-all' : 'badge-target';
    left.innerHTML =
      `<span class="${targetClass}">${targetLabel(card)}</span>` +
      `<span class="card-trigger">T+${fmt(card.trigger_seconds * 1000)}</span> · #${card.seq}`;
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
      del.onclick = () => api('DELETE', `/api/rooms/${state.roomCode}/cards/${card.id}`).catch(showError);
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
  renderGroups();
  renderCards();
  showRoom();
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
  state.roomCode = msg.room.code;
  state.cards = msg.cards;
  state.groups = msg.groups || [];
  state.groupId = msg.viewer && msg.viewer.role === 'player' ? msg.viewer.group_id : null;
  state.lastEventId = msg.last_event_id; // 权威水位（全局）
  syncClock(msg.room.status, msg.room.elapsed_ms);
  renderAll();
}

function applyEvent(msg) {
  if (msg.id <= state.lastEventId) return; // 不回退：丢弃旧事件
  state.lastEventId = msg.id;
  const p = msg.payload;
  switch (msg.type) {
    case 'group_added':
      if (state.role === 'host' && !state.groups.some((g) => g.id === p.id)) state.groups.push(p);
      break;
    case 'group_removed':
      state.groups = state.groups.filter((g) => g.id !== p.id);
      break;
    case 'group_renamed': {
      // 主持人收到完整 payload；本组参与者只收到新名字（join_code 字段同样存在）
      const g = state.groups.find((x) => x.id === p.id);
      if (g) Object.assign(g, p);
      else if (state.role === 'player' && p.id === state.groupId) state.groups.push(p);
      break;
    }
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
  const ws = new WebSocket(
    `${proto}://${location.host}/ws/${state.code}?last_event_id=${state.lastEventId}`,
  );
  state.ws = ws;

  ws.onopen = () => { state.reconnectDelay = 1000; setConn(true); };
  ws.onmessage = (e) => {
    const msg = JSON.parse(e.data);
    if (msg.kind === 'snapshot') applySnapshot(msg);
    else if (msg.kind === 'event') applyEvent(msg);
    else if (msg.kind === 'watermark') {
      // 他组事件：无内容，仅单调推进本连接的水位
      if (msg.id > state.lastEventId) state.lastEventId = msg.id;
    }
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

function enterRoom(code, role, viewer) {
  state.code = code;
  state.role = role;
  state.lastEventId = 0;
  state.cards = [];
  state.groups = [];
  state.groupId = viewer && viewer.role === 'player' ? viewer.group_id : null;
  state.roomCode = viewer ? null : code; // 快照到达前主持端先用房间码
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
    // 组加入码：REST 快照同样按组过滤，未命中卡片不会返回
    const snap = await api('GET', `/api/join/${code}`);
    enterRoom(code, 'player', snap.viewer);
  } catch (err) {
    showError(new Error('组加入码无效，请向主持人确认'));
  }
};

$('#room-code').onclick = () => {
  navigator.clipboard?.writeText(state.code);
  const el = $('#room-code');
  el.textContent = '已复制';
  setTimeout(() => { el.textContent = state.code; }, 1200);
};

$('#btn-start').onclick  = () => api('POST', `/api/rooms/${state.roomCode}/start`).catch(showError);
$('#btn-pause').onclick  = () => api('POST', `/api/rooms/${state.roomCode}/pause`).catch(showError);
$('#btn-resume').onclick = () => api('POST', `/api/rooms/${state.roomCode}/resume`).catch(showError);
$('#btn-reset').onclick  = () => api('POST', `/api/rooms/${state.roomCode}/reset`).catch(showError);

$('#group-form').onsubmit = async (e) => {
  e.preventDefault();
  const name = $('#group-name').value.trim();
  if (!name) return;
  try {
    await api('POST', `/api/rooms/${state.roomCode}/groups`, { name });
    $('#group-name').value = '';
    $('#group-name').focus();
  } catch (err) { showError(err); }
};

$('#card-form').onsubmit = async (e) => {
  e.preventDefault();
  const targetVal = $('#card-target').value;
  const body = {
    title: $('#card-title').value.trim(),
    content: $('#card-content').value.trim(),
    trigger_seconds: parseInt($('#card-trigger').value, 10) || 0,
    target_group_id: targetVal ? parseInt(targetVal, 10) : null,
  };
  try {
    await api('POST', `/api/rooms/${state.roomCode}/cards`, body);
    $('#card-title').value = '';
    $('#card-content').value = '';
    $('#card-trigger').value = '';
    $('#card-target').value = '';
    $('#card-title').focus();
  } catch (err) { showError(err); }
};

/* 刷新恢复：根据 hash 重进房间（走完整的快照+增量协议，视图不回退）。
 * 参与者的 hash 中保存的是组加入码，主持人保存房间码。 */
(async function boot() {
  const m = location.hash.match(/^#\/(host|player)\/([A-Z0-9]{6})$/);
  if (!m) { showLanding(); return; }
  const [, role, code] = m;
  try {
    if (role === 'host') {
      await api('GET', `/api/rooms/${code}`); // 校验主持入口
      enterRoom(code, 'host');
    } else {
      const snap = await api('GET', `/api/join/${code}`);
      enterRoom(code, 'player', snap.viewer);
    }
  } catch (err) {
    showError(new Error(role === 'host' ? '房间不存在' : '组加入码已失效'));
    showLanding();
  }
})();
