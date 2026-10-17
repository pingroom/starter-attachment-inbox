import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { receive } from './inbox.js';
import { NOTIFICATION_ID, SENDER_ID, ROOM, offlineClient } from './offline.js';

const directory = await mkdtemp(join(tmpdir(), 'pingroom-inbox-demo-'));
try {
  const { client } = offlineClient();
  const options = { client, notificationId: NOTIFICATION_ID, room: ROOM, senderId: SENDER_ID, connectionKey: 'offline', directory };
  const first = await receive(options);
  const replay = await receive(options);
  console.log(`Offline demo: ${first.status}; ${first.receipt.files.length} private .bin file and SHA-256 receipt.`);
  console.log(`Replay: ${replay.status}; original batch preserved after local and remote hash verification.`);
  console.log('No credentials, network requests, or live pings. Temporary demo files removed.');
} finally { await rm(directory, { recursive: true, force: true }); }
