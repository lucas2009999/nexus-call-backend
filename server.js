/**
 * NEXUS Call - Servidor de sinalização + grupos
 * -----------------------------------------------
 * Responsabilidades:
 *  1. Sinalização WebRTC (áudio/tela trafegam direto entre os participantes,
 *     nunca passam por aqui).
 *  2. Persistência leve de "grupos" (nome, canais de texto e histórico de
 *     mensagens) num arquivo JSON local — sem banco de dados externo, para
 *     manter o setup simples. Um grupo continua existindo mesmo com
 *     ninguém conectado; só é removido se você apagar manualmente o arquivo.
 *
 * Sem contas de usuário: identidade (nome/foto) é local de cada cliente.
 * A lista de "conectados" mostra só quem está com o app aberto agora —
 * não dá pra mostrar "offline" sem sistema de contas.
 *
 * Dependências:
 *  - ws: servidor WebSocket, leve.
 *  - dotenv: variáveis de ambiente.
 */

require('dotenv').config();
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
const MAX_GROUP_VOICE_PARTICIPANTS = parseInt(process.env.MAX_ROOM_PARTICIPANTS || '12', 10);
const MAX_MESSAGE_SIZE = 200 * 1024; // 200KB - já cobre avatar em base64
const MAX_AVATAR_LENGTH = 150 * 1024;
const MAX_CHAT_LENGTH = 500;
const MAX_MESSAGES_PER_CHANNEL = 200;
const DATA_FILE = path.join(__dirname, 'data', 'groups.json');
const DEFAULT_CHANNELS = [
  { id: 'geral', name: 'geral' },
  { id: 'avisos', name: 'avisos' },
];

// ---------- Persistência de grupos (arquivo JSON) ----------
// groups: code -> { code, name, channels: [{id,name}], messages: {channelId: [...]}, createdAt }
let groups = new Map();

function loadGroups() {
  try {
    const raw = fs.readFileSync(DATA_FILE, 'utf-8');
    const obj = JSON.parse(raw);
    groups = new Map(Object.entries(obj));
    console.log(`[NEXUS Call] ${groups.size} grupo(s) carregado(s) de groups.json`);
  } catch {
    groups = new Map();
  }
}

let saveTimer = null;
function saveGroupsDebounced() {
  if (saveTimer) return;
  saveTimer = setTimeout(() => {
    saveTimer = null;
    const obj = Object.fromEntries(groups.entries());
    fs.mkdirSync(path.dirname(DATA_FILE), { recursive: true });
    fs.writeFileSync(DATA_FILE, JSON.stringify(obj), 'utf-8');
  }, 500);
}

loadGroups();

// ---------- Estado em memória (não persistido) ----------
// groupCode -> Map(clientId -> { ws, name, avatar, inVoice, muted, sharingScreen })
const presence = new Map();

function generateGroupCode() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let code;
  do {
    code = Array.from({ length: 6 }, () => chars[crypto.randomInt(chars.length)]).join('');
  } while (groups.has(code));
  return code;
}

function send(ws, payload) {
  if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(payload));
}

function broadcastToGroup(groupCode, payload, exceptId = null) {
  const room = presence.get(groupCode);
  if (!room) return;
  for (const [id, client] of room.entries()) {
    if (id !== exceptId) send(client.ws, payload);
  }
}

function memberList(groupCode) {
  const room = presence.get(groupCode);
  if (!room) return [];
  return Array.from(room.entries()).map(([id, c]) => ({
    id, name: c.name, avatar: c.avatar || null, inVoice: c.inVoice, muted: c.muted, sharingScreen: c.sharingScreen,
  }));
}

function isValidName(name) {
  return typeof name === 'string' && name.trim().length > 0 && name.length <= 32;
}
function isValidGroupName(name) {
  return typeof name === 'string' && name.trim().length > 0 && name.length <= 40;
}
function isValidGroupCode(code) {
  return typeof code === 'string' && /^[A-Z0-9]{4,10}$/.test(code);
}
function isValidAvatar(avatar) {
  if (avatar === null || avatar === undefined) return true;
  return typeof avatar === 'string' && avatar.startsWith('data:image/') && avatar.length <= MAX_AVATAR_LENGTH;
}
function isValidChannel(group, channelId) {
  return group.channels.some((c) => c.id === channelId);
}

function removeClient(groupCode, clientId) {
  const room = presence.get(groupCode);
  if (!room) return;
  const wasInVoice = room.get(clientId) && room.get(clientId).inVoice;
  room.delete(clientId);
  if (room.size === 0) {
    presence.delete(groupCode); // ninguém conectado agora, mas o grupo continua salvo em disco
  } else {
    broadcastToGroup(groupCode, { type: 'member-left', id: clientId });
    if (wasInVoice) broadcastToGroup(groupCode, { type: 'voice-left', id: clientId });
  }
}

const server = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'ok', groups: groups.size }));
    return;
  }
  res.writeHead(404);
  res.end();
});

const wss = new WebSocketServer({ server, maxPayload: MAX_MESSAGE_SIZE });

wss.on('connection', (ws) => {
  const clientId = crypto.randomUUID();
  let currentGroup = null;

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return; }
    if (!msg || typeof msg.type !== 'string') return;

    switch (msg.type) {
      case 'create-group': {
        if (!isValidGroupName(msg.groupName) || !isValidName(msg.name) || !isValidAvatar(msg.avatar)) {
          send(ws, { type: 'error', message: 'Nome do grupo, nome ou foto inválidos.' });
          return;
        }
        const code = generateGroupCode();
        const group = {
          code,
          name: msg.groupName.trim(),
          channels: DEFAULT_CHANNELS.map((c) => ({ ...c })),
          messages: Object.fromEntries(DEFAULT_CHANNELS.map((c) => [c.id, []])),
          createdAt: Date.now(),
        };
        groups.set(code, group);
        saveGroupsDebounced();
        joinGroupInternal(ws, clientId, code, msg.name, msg.avatar);
        currentGroup = code;
        break;
      }

      case 'join-group': {
        if (!isValidGroupCode(msg.code) || !isValidName(msg.name) || !isValidAvatar(msg.avatar)) {
          send(ws, { type: 'error', message: 'Código, nome ou foto inválidos.' });
          return;
        }
        if (!groups.has(msg.code)) {
          send(ws, { type: 'error', message: 'Grupo não encontrado.' });
          return;
        }
        joinGroupInternal(ws, clientId, msg.code, msg.name, msg.avatar);
        currentGroup = msg.code;
        break;
      }

      case 'send-chat': {
        if (!currentGroup) return;
        const group = groups.get(currentGroup);
        if (!group || !isValidChannel(group, msg.channelId)) return;
        if (typeof msg.text !== 'string') return;
        const text = msg.text.trim().slice(0, MAX_CHAT_LENGTH);
        if (!text) return;
        const room = presence.get(currentGroup);
        const self = room && room.get(clientId);
        if (!self) return;
        const chatMsg = { id: clientId, name: self.name, avatar: self.avatar, text, timestamp: Date.now() };
        group.messages[msg.channelId].push(chatMsg);
        if (group.messages[msg.channelId].length > MAX_MESSAGES_PER_CHANNEL) {
          group.messages[msg.channelId].shift();
        }
        saveGroupsDebounced();
        broadcastToGroup(currentGroup, { type: 'chat-message', channelId: msg.channelId, ...chatMsg });
        break;
      }

      case 'join-voice': {
        if (!currentGroup) return;
        const room = presence.get(currentGroup);
        const self = room && room.get(clientId);
        if (!self || self.inVoice) return;
        self.inVoice = true;
        self.muted = false;
        self.sharingScreen = false;
        broadcastToGroup(currentGroup, { type: 'voice-joined', id: clientId, name: self.name, avatar: self.avatar }, clientId);
        send(ws, { type: 'voice-state', participants: memberList(currentGroup).filter((m) => m.inVoice && m.id !== clientId) });
        break;
      }

      case 'leave-voice': {
        if (!currentGroup) return;
        const room = presence.get(currentGroup);
        const self = room && room.get(clientId);
        if (!self || !self.inVoice) return;
        self.inVoice = false;
        broadcastToGroup(currentGroup, { type: 'voice-left', id: clientId });
        break;
      }

      case 'signal': {
        if (!currentGroup || !msg.to || !msg.data) return;
        const room = presence.get(currentGroup);
        const target = room && room.get(msg.to);
        if (target) send(target.ws, { type: 'signal', from: clientId, data: msg.data });
        break;
      }

      case 'mic-state': {
        if (!currentGroup) return;
        const room = presence.get(currentGroup);
        const self = room && room.get(clientId);
        if (self) { self.muted = !!msg.muted; broadcastToGroup(currentGroup, { type: 'mic-state', id: clientId, muted: self.muted }, clientId); }
        break;
      }

      case 'speaking-state': {
        if (!currentGroup) return;
        broadcastToGroup(currentGroup, { type: 'speaking-state', id: clientId, speaking: !!msg.speaking }, clientId);
        break;
      }

      case 'screen-share-state': {
        if (!currentGroup) return;
        const room = presence.get(currentGroup);
        const self = room && room.get(clientId);
        if (self) { self.sharingScreen = !!msg.sharing; broadcastToGroup(currentGroup, { type: 'screen-share-state', id: clientId, sharing: self.sharingScreen }, clientId); }
        break;
      }

      case 'leave-group': {
        if (currentGroup) { removeClient(currentGroup, clientId); currentGroup = null; }
        break;
      }

      default: break;
    }
  });

  ws.on('close', () => { if (currentGroup) removeClient(currentGroup, clientId); });
  ws.on('error', () => { if (currentGroup) removeClient(currentGroup, clientId); });
});

function joinGroupInternal(ws, clientId, code, name, avatar) {
  const group = groups.get(code);
  if (!presence.has(code)) presence.set(code, new Map());
  const room = presence.get(code);
  room.set(clientId, { ws, name: name.trim(), avatar: avatar || null, inVoice: false, muted: false, sharingScreen: false });
  send(ws, {
    type: 'group-joined',
    code: group.code,
    name: group.name,
    channels: group.channels,
    messages: group.messages,
    selfId: clientId,
    members: memberList(code).filter((m) => m.id !== clientId),
  });
  broadcastToGroup(code, { type: 'member-joined', id: clientId, name: name.trim(), avatar: avatar || null }, clientId);
}

server.listen(PORT, () => {
  console.log(`[NEXUS Call] Servidor rodando na porta ${PORT}`);
});
