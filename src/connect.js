import { PingRoom } from '@pingroom/sdk';
import { credentialDirectory, requestedRoom, saveConnection, selectRoom } from './config.js';
import { errorSummary } from './format.js';
import { shutdownSignal } from './signals.js';

const shutdown = shutdownSignal();
try {
  const requested = requestedRoom(process.argv.slice(2));
  console.log('Install or open PingRoom and sign in: https://pingroom.io/i');
  console.log('Connect this file inbox to an existing standard private room.');
  const client = new PingRoom({ baseUrl: 'https://api.pingroom.io', timeoutMs: 5_000 });
  const pairing = await client.auth.startPairing({ agent_label: 'Attachment inbox' });
  console.log(`Approve this connection and grant the room: ${pairing.pair_browser_url ?? pairing.pair_url}`);
  const active = await client.auth.waitForPairing(pairing, { signal: shutdown.signal });
  shutdown.signal.throwIfAborted();
  const room = selectRoom(active, await client.rooms.list(), requested);
  shutdown.signal.throwIfAborted();
  await saveConnection(credentialDirectory, { token: active.credential, room });
  console.log('Saved .pingroom/credentials.json. Set TRUSTED_SENDER_ID in .env, then supply a notification UUID.');
} catch (error) { console.error(errorSummary(error)); process.exitCode ||= 1; }
finally { shutdown.dispose(); }
