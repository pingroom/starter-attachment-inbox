import { createHash } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { join, resolve } from 'node:path';
import { atomicJson, readJson } from './storage.js';

export const projectRoot = fileURLToPath(new URL('../', import.meta.url));
export const credentialDirectory = join(projectRoot, '.pingroom');
export const sha256 = (bytes) => createHash('sha256').update(bytes).digest('hex');
export const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
export function requireId(value) {
  if (typeof value !== 'string' || !UUID.test(value)) throw new Error('A valid UUID is required.');
  return value.toLowerCase();
}
export function roomCode(value) {
  if (typeof value !== 'string' || !/^[A-Za-z0-9_-]{1,64}$/.test(value.trim())) throw new Error('A valid room code is required.');
  return value.trim().toUpperCase();
}
export function loadEnvironment() {
  try { process.loadEnvFile(join(projectRoot, '.env')); }
  catch (error) { if (error.code !== 'ENOENT') throw error; }
}
export function requestedRoom(args) {
  if (args.length === 0) return undefined;
  if (args.length !== 2 || args[0] !== '--room') throw new Error('Usage: npm run connect -- --room ROOM_CODE');
  return roomCode(args[1]);
}
export function selectRoom(active, rooms, requested) {
  const code = requested ?? roomCode((active.home_room ?? active.room)?.invite_code);
  if (!Array.isArray(rooms)) throw new Error('Could not inspect granted rooms.');
  const matching = rooms.filter((room) => room?.invite_code === code);
  if (matching.length !== 1 || matching[0].type !== 'standard' || matching[0].is_public !== false) {
    throw new Error('Selected room is not a granted standard private room. Use npm run connect -- --room ROOM_CODE and grant that room.');
  }
  return code;
}
export function inboxDirectory(env = process.env) {
  if (env.INBOX_DIRECTORY !== undefined && !env.INBOX_DIRECTORY.trim()) throw new Error('INBOX_DIRECTORY cannot be blank.');
  return env.INBOX_DIRECTORY ? resolve(projectRoot, env.INBOX_DIRECTORY) : join(projectRoot, '.quarantine');
}
export async function saveConnection(directory, connection) {
  if (typeof connection.token !== 'string' || !connection.token.trim()) throw new Error('Pairing did not return a credential.');
  await atomicJson(directory, 'credentials.json', { token: connection.token.trim(), room: roomCode(connection.room) });
}
export async function loadConnection(directory = credentialDirectory, env = process.env) {
  let connection;
  if (env.PINGROOM_TOKEN !== undefined || env.PINGROOM_ROOM !== undefined) {
    if (!env.PINGROOM_TOKEN?.trim() || !env.PINGROOM_ROOM?.trim()) throw new Error('Set both PINGROOM_TOKEN and PINGROOM_ROOM; partial overrides are refused.');
    connection = { token: env.PINGROOM_TOKEN, room: env.PINGROOM_ROOM };
  } else connection = await readJson(join(directory, 'credentials.json'));
  if (!connection || typeof connection.token !== 'string' || !connection.token.trim()) throw new Error('Run npm run connect, or set both PINGROOM_TOKEN and PINGROOM_ROOM.');
  return { token: connection.token.trim(), room: roomCode(connection.room) };
}
