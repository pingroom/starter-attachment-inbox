import { constants } from 'node:fs';
import { chmod, lstat, mkdir, open, rename, rm, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { randomUUID } from 'node:crypto';

export async function privateDirectory(path) {
  await mkdir(path, { recursive: true, mode: 0o700 });
  const stat = await lstat(path);
  if (!stat.isDirectory() || stat.isSymbolicLink()) throw new Error('Private directory must be a real directory, not a symlink.');
  await chmod(path, 0o700);
}

export async function readBytes(path, maximum) {
  const file = await open(path, constants.O_RDONLY | constants.O_NONBLOCK | constants.O_NOFOLLOW);
  try {
    const stat = await file.stat();
    if (!stat.isFile() || stat.size < 1 || stat.size > maximum) throw new Error('Local file is not a bounded regular file.');
    const result = Buffer.alloc(stat.size + 1);
    let length = 0;
    while (length < result.length) {
      const { bytesRead } = await file.read(result, length, result.length - length, null);
      if (bytesRead === 0) break;
      length += bytesRead;
    }
    if (length !== stat.size) throw new Error('Local file changed while reading.');
    return result.subarray(0, length);
  } finally { await file.close(); }
}

export async function readJson(path) {
  let bytes;
  try { bytes = await readBytes(path, 64 * 1024); }
  catch (error) { if (error.code === 'ENOENT') return undefined; throw error; }
  const value = JSON.parse(bytes.toString('utf8'));
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('Invalid local JSON object.');
  return value;
}

export async function atomicJson(directory, name, value) {
  await privateDirectory(directory);
  const bytes = `${JSON.stringify(value, null, 2)}\n`;
  if (Buffer.byteLength(bytes) > 64 * 1024) throw new Error('Local JSON exceeds 64 KiB.');
  const temporary = join(directory, `.write-${randomUUID()}`);
  try {
    await writeFile(temporary, bytes, { flag: 'wx', mode: 0o600, flush: true });
    await rename(temporary, join(directory, name));
  } finally { await rm(temporary, { force: true }); }
}

export async function acquireLock(directory) {
  await privateDirectory(directory);
  const path = join(directory, '.lock');
  try { await mkdir(path, { mode: 0o700 }); }
  catch (error) {
    if (error.code === 'EEXIST') throw new Error('Inbox lock exists. Confirm its owner process stopped before removing .lock.');
    throw error;
  }
  const owner = { pid: process.pid, nonce: randomUUID(), started_at: new Date().toISOString() };
  try { await atomicJson(path, 'owner.json', owner); }
  catch (error) { await rm(path, { recursive: true, force: true }); throw error; }
  return async () => {
    if ((await readJson(join(path, 'owner.json')))?.nonce === owner.nonce) await rm(path, { recursive: true, force: true });
  };
}
