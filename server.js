// Chat em salas — Node.js puro, sem dependências. Rode com: node server.js
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;
const DATA = process.env.DATA_DIR || path.join(__dirname, 'data');
const UP = path.join(DATA, 'uploads');
const DB = path.join(DATA, 'rooms.json');
const MAX_FILE = 100 * 1024 * 1024; // 100 MB por arquivo
const MAX_MSGS = 500; // histórico por sala
fs.mkdirSync(UP, { recursive: true });

let rooms = {};
try { rooms = JSON.parse(fs.readFileSync(DB, 'utf8')); } catch {}
let saveTimer;
function save() {
  clearTimeout(saveTimer);
  saveTimer = setTimeout(() => fs.writeFile(DB, JSON.stringify(rooms), () => {}), 300);
}

const clients = {}; // código -> Set<{res, name}>
const ALPH = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
const newCode = () => {
  let c;
  do { c = Array.from(crypto.randomBytes(6), b => ALPH[b % ALPH.length]).join(''); } while (rooms[c]);
  return c;
};
const cleanCode = s => String(s || '').toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 12);
const cleanName = s => String(s || '').replace(/[\r\n\t]/g, ' ').trim().slice(0, 24) || 'Anônimo';

function json(res, status, obj) {
  res.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8' });
  res.end(JSON.stringify(obj));
}
function readBody(req, limit = 1e6) {
  return new Promise((resolve, reject) => {
    let size = 0; const chunks = [];
    req.on('data', c => { size += c.length; if (size > limit) { reject(new Error('grande')); req.destroy(); } else chunks.push(c); });
    req.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    req.on('error', reject);
  });
}
function emit(code, payload) {
  const line = `data: ${JSON.stringify(payload)}\n\n`;
  for (const c of clients[code] || []) c.res.write(line);
}
function presence(code) {
  emit(code, { type: 'presence', users: [...(clients[code] || [])].map(c => c.name) });
}
function addMessage(code, msg) {
  const room = rooms[code];
  room.messages.push(msg);
  if (room.messages.length > MAX_MSGS) {
    const old = room.messages.splice(0, room.messages.length - MAX_MSGS);
    for (const m of old) if (m.file) fs.unlink(path.join(UP, code, m.file.id), () => {});
  }
  save();
  emit(code, { type: 'msg', msg });
}

const SAFE_INLINE = /^(image\/(png|jpe?g|gif|webp|bmp|avif)|video\/(mp4|webm|ogg|quicktime)|audio\/(mpeg|mp3|ogg|webm|wav|x-wav|mp4|aac|x-m4a|m4a))(;.*)?$/i;

function serveFile(req, res, code, id) {
  const room = rooms[code];
  const m = room && room.messages.find(x => x.file && x.file.id === id);
  const fp = path.join(UP, code, id);
  if (!m || !fs.existsSync(fp)) { res.writeHead(404); return res.end('Não encontrado'); }
  const size = fs.statSync(fp).size;
  const inline = SAFE_INLINE.test(m.file.type);
  const headers = {
    'Content-Type': inline ? m.file.type : 'application/octet-stream',
    'Content-Disposition': `${inline ? 'inline' : 'attachment'}; filename*=UTF-8''${encodeURIComponent(m.file.name)}`,
    'X-Content-Type-Options': 'nosniff',
    'Accept-Ranges': 'bytes',
    'Cache-Control': 'private, max-age=86400',
  };
  const range = /bytes=(\d*)-(\d*)/.exec(req.headers.range || '');
  if (range) {
    let start = range[1] ? parseInt(range[1]) : 0;
    let end = range[2] ? parseInt(range[2]) : size - 1;
    if (!range[1] && range[2]) { start = Math.max(0, size - parseInt(range[2])); end = size - 1; }
    end = Math.min(end, size - 1);
    if (start > end || start >= size) { res.writeHead(416, { 'Content-Range': `bytes */${size}` }); return res.end(); }
    res.writeHead(206, { ...headers, 'Content-Range': `bytes ${start}-${end}/${size}`, 'Content-Length': end - start + 1 });
    fs.createReadStream(fp, { start, end }).pipe(res);
  } else {
    res.writeHead(200, { ...headers, 'Content-Length': size });
    fs.createReadStream(fp).pipe(res);
  }
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://x');
  const p = url.pathname;
  try {
    if (req.method === 'GET' && (p === '/' || p === '/index.html')) {
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      return res.end(fs.readFileSync(path.join(__dirname, 'index.html')));
    }
    if (req.method === 'GET' && p === '/health') return json(res, 200, { ok: true });

    if (req.method === 'POST' && p === '/api/rooms') {
      const code = newCode();
      rooms[code] = { created: Date.now(), messages: [] };
      save();
      return json(res, 200, { code });
    }

    let m;
    if ((m = p.match(/^\/api\/rooms\/([A-Za-z0-9-]+)$/)) && req.method === 'GET') {
      const code = cleanCode(m[1]);
      return json(res, rooms[code] ? 200 : 404, { exists: !!rooms[code], code });
    }

    if ((m = p.match(/^\/api\/rooms\/([A-Za-z0-9-]+)\/events$/)) && req.method === 'GET') {
      const code = cleanCode(m[1]);
      if (!rooms[code]) return json(res, 404, { error: 'Sala não encontrada' });
      res.writeHead(200, {
        'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache, no-transform',
        Connection: 'keep-alive', 'X-Accel-Buffering': 'no',
      });
      const client = { res, name: cleanName(url.searchParams.get('name')) };
      (clients[code] ||= new Set()).add(client);
      res.write(`data: ${JSON.stringify({ type: 'history', messages: rooms[code].messages })}\n\n`);
      presence(code);
      const hb = setInterval(() => res.write(': ping\n\n'), 25000);
      req.on('close', () => { clearInterval(hb); clients[code].delete(client); presence(code); });
      return;
    }

    if ((m = p.match(/^\/api\/rooms\/([A-Za-z0-9-]+)\/messages$/)) && req.method === 'POST') {
      const code = cleanCode(m[1]);
      if (!rooms[code]) return json(res, 404, { error: 'Sala não encontrada' });
      const body = JSON.parse(await readBody(req));
      const text = String(body.text || '').slice(0, 4000);
      if (!text.trim()) return json(res, 400, { error: 'Mensagem vazia' });
      addMessage(code, { id: crypto.randomBytes(6).toString('hex'), time: Date.now(), name: cleanName(body.name), text });
      return json(res, 200, { ok: true });
    }

    if ((m = p.match(/^\/api\/rooms\/([A-Za-z0-9-]+)\/upload$/)) && req.method === 'POST') {
      const code = cleanCode(m[1]);
      if (!rooms[code]) return json(res, 404, { error: 'Sala não encontrada' });
      const declared = parseInt(req.headers['content-length'] || '0');
      if (declared > MAX_FILE) return json(res, 413, { error: 'Arquivo maior que 100 MB' });
      const fname = (url.searchParams.get('filename') || 'arquivo').replace(/[\\/\r\n]/g, '_').slice(0, 120);
      const type = (url.searchParams.get('type') || 'application/octet-stream').slice(0, 100);
      const id = crypto.randomBytes(10).toString('hex');
      fs.mkdirSync(path.join(UP, code), { recursive: true });
      const fp = path.join(UP, code, id);
      const out = fs.createWriteStream(fp);
      let size = 0, aborted = false;
      req.on('data', c => {
        size += c.length;
        if (size > MAX_FILE && !aborted) { aborted = true; out.destroy(); fs.unlink(fp, () => {}); json(res, 413, { error: 'Arquivo maior que 100 MB' }); req.destroy(); }
      });
      req.pipe(out);
      out.on('finish', () => {
        if (aborted) return;
        addMessage(code, {
          id: crypto.randomBytes(6).toString('hex'), time: Date.now(), name: cleanName(url.searchParams.get('name')),
          file: { id, name: fname, type, size, url: `/files/${code}/${id}` },
        });
        json(res, 200, { ok: true });
      });
      out.on('error', () => { if (!res.headersSent) json(res, 500, { error: 'Falha ao salvar' }); });
      return;
    }

    if ((m = p.match(/^\/files\/([A-Za-z0-9]+)\/([a-f0-9]+)$/)) && req.method === 'GET') {
      return serveFile(req, res, cleanCode(m[1]), m[2]);
    }

    res.writeHead(404); res.end('Não encontrado');
  } catch (e) {
    if (!res.headersSent) json(res, 400, { error: 'Requisição inválida' });
  }
});

server.listen(PORT, '0.0.0.0', () => console.log(`Chat no ar: http://localhost:${PORT}`));
