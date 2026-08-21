/**
 * NEXUS Call - Servidor de sinalização
 * -------------------------------------
 * Este servidor NÃO transporta áudio/vídeo/tela. Ele só troca mensagens
 * pequenas (JSON) entre os participantes de uma sala para que eles consigam
 * negociar conexões WebRTC diretas (peer-to-peer) entre si.
 *
 * Dependências:
 *  - ws: implementação de servidor WebSocket, leve e sem dependências pesadas.
 *  - dotenv: carrega variáveis de ambiente do arquivo .env (nunca commitar o .env real).
 */

require('dotenv').config();
const http = require('http');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
const MAX_ROOM_PARTICIPANTS = parseInt(process.env.MAX_ROOM_PARTICIPANTS || '12', 10);
const MAX_MESSAGE_SIZE = 64 * 1024; // 64KB - mensagens de sinalização são pequenas

// room code -> Map(clientId -> { ws, name, muted, sharingScreen })
const rooms = new Map();

function generateRoomCode() {
  // 6 caracteres, letras maiúsculas + números, sem caracteres ambíguos (0/O, 1/I)
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let code;
  do {
    code = Array.from({ length: 6 }, () => chars[crypto.randomInt(chars.length)]).join('');
  } while (rooms.has(code));
  return code;
}

function send(ws, payload) {
  if (ws.readyState === ws.OPEN) {
    ws.send(JSON.stringify(payload));
  }
}

function broadcastToRoom(roomCode, payload, exceptId = null) {
  const room = rooms.get(roomCode);
  if (!room) return;
  for (const [id, client] of room.entries()) {
    if (id !== exceptId) send(client.ws, payload);
  }
}

function participantsList(roomCode) {
  const room = rooms.get(roomCode);
  if (!room) return [];
  return Array.from(room.entries()).map(([id, c]) => ({
    id,
    name: c.name,
    muted: c.muted,
    sharingScreen: c.sharingScreen,
  }));
}

function removeClient(roomCode, clientId) {
  const room = rooms.get(roomCode);
  if (!room) return;
  room.delete(clientId);
  if (room.size === 0) {
    rooms.delete(roomCode); // sala é encerrada quando o último participante sai
  } else {
    broadcastToRoom(roomCode, { type: 'participant-left', id: clientId });
  }
}

// Validação simples de entrada (evita payloads maliciosos/corrompidos)
function isValidName(name) {
  return typeof name === 'string' && name.trim().length > 0 && name.length <= 32;
}
function isValidRoomCode(code) {
  return typeof code === 'string' && /^[A-Z0-9]{4,10}$/.test(code);
}

const server = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'ok', rooms: rooms.size }));
    return;
  }
  res.writeHead(404);
  res.end();
});

const wss = new WebSocketServer({ server, maxPayload: MAX_MESSAGE_SIZE });

wss.on('connection', (ws) => {
  const clientId = crypto.randomUUID();
  let currentRoom = null;

  ws.on('message', (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return; // ignora mensagens inválidas, sem derrubar a conexão
    }
    if (!msg || typeof msg.type !== 'string') return;

    switch (msg.type) {
      case 'create-room': {
        if (!isValidName(msg.name)) {
          send(ws, { type: 'error', message: 'Nome inválido.' });
          return;
        }
        const roomCode = generateRoomCode();
        rooms.set(roomCode, new Map());
        rooms.get(roomCode).set(clientId, { ws, name: msg.name.trim(), muted: false, sharingScreen: false });
        currentRoom = roomCode;
        send(ws, { type: 'room-created', roomCode, selfId: clientId, participants: participantsList(roomCode) });
        break;
      }

      case 'join-room': {
        if (!isValidName(msg.name) || !isValidRoomCode(msg.roomCode)) {
          send(ws, { type: 'error', message: 'Código de sala ou nome inválido.' });
          return;
        }
        const room = rooms.get(msg.roomCode);
        if (!room) {
          send(ws, { type: 'error', message: 'Sala não encontrada.' });
          return;
        }
        if (room.size >= MAX_ROOM_PARTICIPANTS) {
          send(ws, { type: 'error', message: 'Sala cheia.' });
          return;
        }
        currentRoom = msg.roomCode;
        room.set(clientId, { ws, name: msg.name.trim(), muted: false, sharingScreen: false });
        send(ws, { type: 'room-joined', roomCode: currentRoom, selfId: clientId, participants: participantsList(currentRoom) });
        broadcastToRoom(currentRoom, { type: 'participant-joined', id: clientId, name: msg.name.trim() }, clientId);
        break;
      }

      case 'signal': {
        // Repassa oferta/resposta/ICE candidate diretamente para o peer alvo
        if (!currentRoom || !msg.to || !msg.data) return;
        const room = rooms.get(currentRoom);
        const target = room && room.get(msg.to);
        if (target) {
          send(target.ws, { type: 'signal', from: clientId, data: msg.data });
        }
        break;
      }

      case 'mic-state': {
        if (!currentRoom) return;
        const room = rooms.get(currentRoom);
        const self = room && room.get(clientId);
        if (self) {
          self.muted = !!msg.muted;
          broadcastToRoom(currentRoom, { type: 'mic-state', id: clientId, muted: self.muted }, clientId);
        }
        break;
      }

      case 'speaking-state': {
        if (!currentRoom) return;
        broadcastToRoom(currentRoom, { type: 'speaking-state', id: clientId, speaking: !!msg.speaking }, clientId);
        break;
      }

      case 'screen-share-state': {
        if (!currentRoom) return;
        const room = rooms.get(currentRoom);
        const self = room && room.get(clientId);
        if (self) {
          self.sharingScreen = !!msg.sharing;
          broadcastToRoom(currentRoom, { type: 'screen-share-state', id: clientId, sharing: self.sharingScreen }, clientId);
        }
        break;
      }

      case 'leave-room': {
        if (currentRoom) {
          removeClient(currentRoom, clientId);
          currentRoom = null;
        }
        break;
      }

      default:
        break; // tipo desconhecido, ignora
    }
  });

  ws.on('close', () => {
    if (currentRoom) removeClient(currentRoom, clientId);
  });

  ws.on('error', () => {
    if (currentRoom) removeClient(currentRoom, clientId);
  });
});

server.listen(PORT, () => {
  console.log(`[NEXUS Call] Servidor de sinalização rodando na porta ${PORT}`);
});
