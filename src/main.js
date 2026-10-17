import { PingRoom } from '@pingroom/sdk';
import { loadConnection, loadEnvironment, inboxDirectory, requireId, sha256 } from './config.js';
import { receive } from './inbox.js';
import { errorSummary } from './format.js';
import { shutdownSignal } from './signals.js';

const shutdown = shutdownSignal();
try {
  if (process.argv.length !== 3) throw new Error('Usage: npm start -- NOTIFICATION_UUID');
  const notificationId = requireId(process.argv[2]);
  loadEnvironment();
  const senderId = requireId(process.env.TRUSTED_SENDER_ID);
  const connection = await loadConnection();
  const client = new PingRoom({ token: connection.token, baseUrl: 'https://api.pingroom.io', timeoutMs: 5_000 });
  const result = await receive({ client, notificationId, room: connection.room, senderId, connectionKey: sha256(connection.token), directory: inboxDirectory(), signal: shutdown.signal });
  console.log(JSON.stringify({ status: result.status, directory: result.directory, files: result.receipt.files.length }, null, 2));
} catch (error) { console.error(errorSummary(error)); process.exitCode ||= 1; }
finally { shutdown.dispose(); }
