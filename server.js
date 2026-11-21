const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const server = http.createServer((req, res) => {
  fs.readFile(path.join(__dirname, 'public', 'index.html'), (err, data) => {
    if (err) { res.writeHead(500); return res.end('Erreur'); }
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(data);
  });
});

const wss = new WebSocketServer({ server });
const rooms = {};

function getRoom(id) {
  return rooms[id] || (rooms[id] = {
    queue: [], index: 0, playing: false,
    base: 0, ts: Date.now(), pl: 0, clients: new Set(), chat: [],
  });
}
function pushChat(r, msg) {
  r.chat.push(msg);
  if (r.chat.length > 50) r.chat.shift();
  const data = JSON.stringify({ type: 'chat', ...msg });
  r.clients.forEach((c) => c.readyState === 1 && c.send(data));
}
const position = (r) => (r.playing ? r.base + (Date.now() - r.ts) / 1000 : r.base);
const snapshot = (r) => ({
  type: 'state', queue: r.queue, index: r.index,
  playing: r.playing, pos: position(r), pl: r.pl,
});
function broadcast(r) {
  const msg = JSON.stringify(snapshot(r));
  r.clients.forEach((c) => c.readyState === 1 && c.send(msg));
}
function startItem(r, index) {
  r.index = index; r.pl = 0; r.base = 0; r.ts = Date.now(); r.playing = true;
}

wss.on('connection', (ws) => {
  let r = null;
  ws.on('message', (raw) => {
    let m;
    try { m = JSON.parse(raw); } catch { return; }

    if (m.type === 'join') {
      if (r) return; // déjà dans une salle
      r = getRoom(String(m.room).slice(0, 40));
      ws.pseudo = String(m.pseudo || '').trim().slice(0, 20) || 'Anonyme';
      r.clients.add(ws);
      ws.send(JSON.stringify(snapshot(r)));
      ws.send(JSON.stringify({ type: 'chat_history', messages: r.chat }));
      pushChat(r, { system: true, text: ws.pseudo + ' a rejoint la salle' });
      return;
    }
    if (!r) return;

    if (m.type === 'chat') {
      const text = String(m.text || '').trim().slice(0, 300);
      if (text) pushChat(r, { pseudo: ws.pseudo, text, ts: Date.now() });
      return;
    }
    if (!r) return;

    switch (m.type) {
      case 'add':
        r.queue.push({ kind: m.kind, id: String(m.id).slice(0, 80), title: String(m.title || '').slice(0, 100) });
        if (r.queue.length === 1) startItem(r, 0);
        break;
      case 'play':
        r.base = typeof m.pos === 'number' ? m.pos : position(r);
        r.ts = Date.now(); r.playing = true;
        break;
      case 'pause':
        r.base = position(r); r.playing = false;
        break;
      case 'seek':
        r.base = m.pos; r.ts = Date.now();
        break;
      case 'jump':
        if (r.queue[m.index]) startItem(r, m.index);
        break;
      case 'next':
        // "from" évite que deux clients fassent avancer deux fois
        if (m.from !== undefined && m.from !== r.index) return;
        if (r.index + 1 < r.queue.length) startItem(r, r.index + 1);
        else { r.playing = false; r.base = 0; }
        break;
      case 'prev':
        startItem(r, Math.max(0, r.index - 1));
        break;
      case 'remove':
        if (!r.queue[m.index]) return;
        r.queue.splice(m.index, 1);
        if (m.index < r.index) r.index--;
        else if (m.index === r.index) {
          if (r.queue.length) startItem(r, Math.min(r.index, r.queue.length - 1));
          else { r.playing = false; r.index = 0; r.base = 0; }
        }
        break;
      case 'plindex':
        if (!Number.isInteger(m.pl) || m.pl < 0 || m.pl === r.pl) return;
        r.pl = m.pl; r.base = 0; r.ts = Date.now(); r.playing = true;
        break;
      default:
        return;
    }
    broadcast(r);
  });
  ws.on('close', () => {
    if (!r) return;
    r.clients.delete(ws);
    pushChat(r, { system: true, text: ws.pseudo + ' a quitté la salle' });
  });
});

// Rappel de synchro toutes les 5 s pour rattraper les décalages
setInterval(() => {
  Object.values(rooms).forEach((r) => r.playing && broadcast(r));
}, 5000);

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log('Serveur sur http://localhost:' + PORT));
