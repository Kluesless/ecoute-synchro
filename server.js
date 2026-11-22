const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const server = http.createServer((req, res) => {
  fs.readFile(path.join(__dirname, 'public', 'index.html'), (err, data) => {
    if (err) { res.writeHead(500); return res.end('Erreur'); }
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
    res.end(data);
  });
});

const wss = new WebSocketServer({ server });
const rooms = {};

// Playlists officielles de YouTube (tops du moment), utilisées par le mode automatique
const AUTO_LISTS = [
  { id: 'PL4fGSI1pDJn6puJdseH2Rt9sMvt9E2M4i', name: 'Top 100 des titres (monde)' },
  { id: 'PL4fGSI1pDJn5kI81J1fYWK5eZRl1zJ5kM', name: 'Top 100 des clips (monde)' },
  { id: 'PLFgquLnL59alCl_2TQvOiD5Vgm1hCaGSI', name: 'Clips populaires' },
];

const isNum = (v) => typeof v === 'number' && Number.isFinite(v);
const isInt = (v) => Number.isInteger(v);

function getRoom(id) {
  return rooms[id] || (rooms[id] = {
    queue: [], index: 0, playing: false, ended: false, auto: false,
    base: 0, ts: Date.now(), pl: 0, uid: 0,
    clients: new Set(), chat: [], lib: [], libUid: 0,
  });
}

// Position de lecture (en secondes) dans la vidéo en cours
const position = (r) => (r.playing ? r.base + (Date.now() - r.ts) / 1000 : r.base);

const snapshot = (r, tick = false) => ({
  type: 'state', tick, queue: r.queue, index: r.index,
  playing: r.playing, pos: position(r), pl: r.pl, auto: r.auto,
});

function broadcast(r, tick = false) {
  const msg = JSON.stringify(snapshot(r, tick));
  r.clients.forEach((c) => c.readyState === 1 && c.send(msg));
}

function pushChat(r, msg) {
  r.chat.push(msg);
  if (r.chat.length > 50) r.chat.shift();
  const data = JSON.stringify({ type: 'chat', ...msg });
  r.clients.forEach((c) => c.readyState === 1 && c.send(data));
}

function broadcastLib(r) {
  const data = JSON.stringify({ type: 'lib', items: r.lib });
  r.clients.forEach((c) => c.readyState === 1 && c.send(data));
}

const validLib = (m) =>
  (m.kind === 'video' || m.kind === 'playlist') && typeof m.id === 'string' && /^[\w-]{1,80}$/.test(m.id);

const hasLib = (r, kind, ytId) => r.lib.some((x) => x.kind === kind && x.ytId === ytId);

function addLib(r, kind, ytId, title, author) {
  r.lib.push({
    id: ++r.libUid, kind, ytId, author, ts: Date.now(),
    title: String(title || '').trim().slice(0, 100) || (kind === 'playlist' ? 'Playlist' : 'Vidéo'),
  });
}

function startItem(r, index) {
  r.index = index; r.pl = 0; r.base = 0; r.ts = Date.now();
  r.playing = true; r.ended = false;
}

// Mode automatique : quand il n'y a plus rien, on lance un top du moment (à un endroit au hasard)
function autoFill(r) {
  if (!r.auto) return false;
  if (r.queue.length >= 250) { // on garde seulement les derniers morceaux joués
    r.queue.splice(0, r.queue.length - 10);
    r.index = r.queue.length - 1;
  }
  const src = AUTO_LISTS[Math.floor(Math.random() * AUTO_LISTS.length)];
  r.queue.push({ uid: ++r.uid, kind: 'playlist', id: src.id, title: '🎲 ' + src.name, auto: true });
  startItem(r, r.queue.length - 1);
  r.pl = Math.floor(Math.random() * 20);
  return true;
}

// Ajoute un morceau choisi par quelqu'un. Il prend la place d'un morceau "automatique" en cours.
function enqueue(r, item, now) {
  const cur = r.queue[r.index];
  if (cur && cur.auto && r.index === r.queue.length - 1) {
    r.queue.splice(r.index, 1);
    r.queue.push(item);
    startItem(r, r.queue.length - 1);
    return;
  }
  r.queue.push(item);
  if (r.queue.length === 1 || r.ended || now) startItem(r, r.queue.length - 1);
}

function shuffleInPlace(a) {
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}

wss.on('connection', (ws) => {
  let r = null;

  ws.on('message', (raw) => {
    let m;
    try { m = JSON.parse(raw); } catch { return; }
    if (!m || typeof m !== 'object') return;

    if (m.type === 'join') {
      if (r) return; // déjà dans une salle
      r = getRoom(String(m.room || 'salle').slice(0, 40));
      ws.pseudo = String(m.pseudo || '').trim().slice(0, 20) || 'Anonyme';
      r.clients.add(ws);
      ws.send(JSON.stringify(snapshot(r)));
      ws.send(JSON.stringify({ type: 'chat_history', messages: r.chat }));
      ws.send(JSON.stringify({ type: 'lib', items: r.lib }));
      pushChat(r, { system: true, text: ws.pseudo + ' a rejoint la salle' });
      return;
    }
    if (!r) return;

    if (m.type === 'chat') {
      const text = String(m.text || '').trim().slice(0, 300);
      if (text) pushChat(r, { pseudo: ws.pseudo, text, ts: Date.now() });
      return;
    }

    // --- Bibliothèque de playlists partagée par toute la salle ---
    if (m.type === 'libadd') {
      if (!validLib(m) || r.lib.length >= 200 || hasLib(r, m.kind, m.id)) return;
      addLib(r, m.kind, m.id, m.title, ws.pseudo);
      broadcastLib(r);
      return;
    }
    if (m.type === 'libdel') {
      // On ne peut supprimer que ses propres liens
      const i = r.lib.findIndex((x) => x.id === m.id && x.author === ws.pseudo);
      if (i < 0) return;
      r.lib.splice(i, 1);
      broadcastLib(r);
      return;
    }
    if (m.type === 'libsync') {
      // Chacun renvoie ses propres liens à la connexion : ils survivent à un redémarrage du serveur
      if (!Array.isArray(m.items)) return;
      let changed = false;
      for (const it of m.items.slice(0, 100)) {
        if (!it || !validLib(it) || r.lib.length >= 200 || hasLib(r, it.kind, it.id)) continue;
        addLib(r, it.kind, it.id, it.title, ws.pseudo);
        changed = true;
      }
      if (changed) broadcastLib(r);
      return;
    }

    switch (m.type) {
      case 'add': {
        if (m.kind !== 'video' && m.kind !== 'playlist') return;
        if (typeof m.id !== 'string' || !/^[\w-]{1,80}$/.test(m.id)) return;
        if (r.queue.length >= 300) return;
        enqueue(r, { uid: ++r.uid, kind: m.kind, id: m.id, title: String(m.title || '').slice(0, 100) }, false);
        break;
      }
      case 'play':
        if (!r.queue.length) return;
        if (r.ended && r.queue[r.index]) { startItem(r, r.index); break; }
        r.base = isNum(m.pos) ? Math.max(0, m.pos) : position(r);
        r.ts = Date.now(); r.playing = true;
        break;
      case 'pause':
        r.base = position(r); r.playing = false;
        break;
      case 'seek':
        if (!isNum(m.pos)) return;
        r.base = Math.max(0, m.pos); r.ts = Date.now();
        break;
      case 'jump':
        if (!isInt(m.index) || !r.queue[m.index]) return;
        if (m.index === r.index && !r.ended) return;
        startItem(r, m.index);
        break;
      case 'next':
        // "from" évite que plusieurs clients fassent avancer plusieurs fois
        if (m.from !== undefined && m.from !== r.index) return;
        if (!r.queue.length) return;
        if (r.index + 1 < r.queue.length) startItem(r, r.index + 1);
        else if (!autoFill(r)) { r.playing = false; r.base = 0; r.ended = true; }
        break;
      case 'prev':
        if (!r.queue.length) return;
        startItem(r, Math.max(0, r.index - 1));
        break;
      case 'remove': {
        if (!isInt(m.index) || !r.queue[m.index]) return;
        r.queue.splice(m.index, 1);
        if (!r.queue.length) {
          r.playing = false; r.index = 0; r.base = 0; r.ended = false;
          autoFill(r);
        } else if (m.index < r.index) r.index--;
        else if (m.index === r.index) {
          if (r.index < r.queue.length) startItem(r, r.index);              // le suivant prend la place
          else if (!autoFill(r)) startItem(r, r.queue.length - 1);          // sinon on revient au précédent
        }
        break;
      }
      case 'move': {
        const n = r.queue.length;
        if (!isInt(m.from) || !isInt(m.to) || m.from === m.to ||
            m.from < 0 || m.to < 0 || m.from >= n || m.to >= n) return;
        const current = r.queue[r.index];
        const [item] = r.queue.splice(m.from, 1);
        r.queue.splice(m.to, 0, item);
        if (current) r.index = r.queue.indexOf(current); // le morceau en cours ne change pas
        break;
      }
      case 'shufflequeue': {
        // Le morceau en cours et ceux déjà passés restent en place : seuls les suivants sont mélangés
        const head = r.queue.slice(0, r.index + 1);
        const rest = r.queue.slice(r.index + 1);
        if (rest.length < 2) return;
        r.queue = head.concat(shuffleInPlace(rest));
        break;
      }
      case 'plshuffle': {
        // L'ordre mélangé est calculé par un client puis partagé à tout le monde
        const it = r.queue[r.index];
        if (!it || it.kind !== 'playlist' || !Array.isArray(m.order) || m.order.length > 200 ||
            !m.order.every((v) => typeof v === 'string' && /^[\w-]{11}$/.test(v))) return;
        it.order = m.order;
        it.ver = (it.ver || 0) + 1;
        r.pl = 0; // la vidéo en cours est placée en premier, le temps de lecture est conservé
        break;
      }
      case 'plindex':
        if (!isInt(m.pl) || m.pl < 0 || m.pl === r.pl) return;
        r.pl = m.pl; r.base = 0; r.ts = Date.now(); r.playing = true; r.ended = false;
        break;
      case 'auto':
        r.auto = m.on === true;
        // Si plus rien ne joue, on lance tout de suite de la musique
        if (r.auto && (!r.queue.length || r.ended)) autoFill(r);
        break;
      case 'libplay': {
        const e = r.lib.find((x) => x.id === m.id);
        if (!e || r.queue.length >= 300) return;
        // m.now : lancer tout de suite ; sinon on ajoute à la suite (et ça démarre si rien ne jouait)
        enqueue(r, { uid: ++r.uid, kind: e.kind, id: e.ytId, title: e.title }, m.now === true);
        break;
      }
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

// Petit rappel de synchro (sans écraser ce que les gens viennent de faire) toutes les 5 s
setInterval(() => {
  Object.values(rooms).forEach((r) => r.playing && r.clients.size && broadcast(r, true));
}, 5000);

// Garde les connexions ouvertes (certains hébergeurs coupent les connexions silencieuses)
setInterval(() => {
  wss.clients.forEach((c) => { try { c.ping(); } catch {} });
}, 25000);

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log('Serveur sur http://localhost:' + PORT));
