import { PingRoom } from '@pingroom/sdk';

export const NOTIFICATION_ID = '11111111-1111-4111-8111-111111111111';
export const SENDER_ID = '22222222-2222-4222-8222-222222222222';
export const FILE_ID = '33333333-3333-4333-8333-333333333333';
export const ROOM_ID = '44444444-4444-4444-8444-444444444444';
export const ROOM = 'DEMO01';
export const content = Buffer.from('Report received from a phone.\n');

export function notification() {
  return {
    id: NOTIFICATION_ID, room_id: ROOM_ID, sender_id: SENDER_ID,
    room: { id: ROOM_ID, invite_code: ROOM, type: 'standard', is_public: false },
    sender: { id: SENDER_ID, name: 'Demo account' },
    trigger_source: 'manual', is_handoff: false, question: null,
    created_at: '2026-10-17T05:00:00.000Z',
    attachments: [{ id: FILE_ID, filename: 'report.txt', mime_type: 'text/plain', size_bytes: content.length, created_at: '2026-10-17T04:59:00.000Z' }],
  };
}

export function offlineClient({ record = notification(), bytes = content, respond, onRequest } = {}) {
  const calls = [];
  const fetch = async (url, init) => {
    const target = new URL(url);
    if (target.origin !== 'https://attachment-inbox.invalid' || init.headers.Authorization !== 'Bearer synthetic-offline-token' || init.method !== 'GET') {
      throw new Error('Offline transport refused a request.');
    }
    calls.push(target.pathname);
    await onRequest?.(target.pathname, init);
    if (target.pathname === `/api/agent/notifications/${NOTIFICATION_ID}`) {
      return new Response(JSON.stringify(record), { headers: { 'content-type': 'application/json' } });
    }
    if (target.pathname.startsWith('/api/agent/attachments/') && target.pathname.endsWith('/content')) {
      if (respond) return respond(target.pathname, init);
      const pieces = [bytes.subarray(0, 5), bytes.subarray(5)];
      return new Response(new ReadableStream({
        pull(controller) { const piece = pieces.shift(); if (piece) controller.enqueue(piece); else controller.close(); },
      }), { headers: { 'content-type': 'text/plain', 'content-length': String(bytes.length) } });
    }
    throw new Error('Unexpected offline route.');
  };
  return { client: new PingRoom({ token: 'synthetic-offline-token', baseUrl: 'https://attachment-inbox.invalid', fetch, timeoutMs: 1000 }), calls };
}
