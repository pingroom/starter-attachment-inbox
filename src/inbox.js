import { lstat, mkdir, readdir, rename, rm, writeFile } from 'node:fs/promises';
import { randomUUID } from 'node:crypto';
import { join } from 'node:path';
import { performance } from 'node:perf_hooks';
import { setImmediate as yieldToEventLoop } from 'node:timers/promises';
import { acquireLock, atomicJson, readBytes, readJson } from './storage.js';
import { requireId, roomCode, sha256 } from './config.js';

export const MAX_FILE_BYTES = 5 * 1024 * 1024;
export const MAX_BATCHES = 100;
const MIMES = new Set(['text/plain', 'text/markdown', 'text/html', 'application/pdf', 'image/jpeg', 'image/png', 'application/zip', 'application/x-zip-compressed']);
const digest = (value) => sha256(JSON.stringify(value));
const date = (value) => typeof value === 'string' && value.length <= 64 && Number.isFinite(Date.parse(value));

export function snapshotNotification(notification, { notificationId, room, senderId }) {
  const n = notification;
  if (!n || requireId(n.id) !== requireId(notificationId)
    || n.room?.invite_code !== roomCode(room) || n.room?.is_public !== false || n.room?.type !== 'standard'
    || requireId(n.room_id) !== requireId(n.room?.id)
    || requireId(n.sender_id) !== requireId(senderId) || requireId(n.sender?.id) !== requireId(senderId)
    || n.trigger_source !== 'manual' || n.origin_agent != null || n.is_handoff !== false || n.question != null) {
    throw new Error('Notification does not match the trusted account, manual source, and private room policy.');
  }
  if (!date(n.created_at) || !Array.isArray(n.attachments) || n.attachments.length < 1 || n.attachments.length > 4) {
    throw new Error('Expected a dated notification with 1–4 attachments.');
  }
  const ids = new Set();
  const attachments = n.attachments.map((a) => {
    const id = requireId(a?.id);
    if (ids.has(id) || typeof a.filename !== 'string' || [...a.filename].length < 1 || [...a.filename].length > 255
      || !/\.(?:md|pdf|html|txt|jpe?g|png|zip)$/i.test(a.filename)
      || !MIMES.has(a.mime_type) || !Number.isSafeInteger(a.size_bytes) || a.size_bytes < 1 || a.size_bytes > MAX_FILE_BYTES
      || !date(a.created_at)) throw new Error('Attachment metadata is invalid, duplicated, or over the size limit.');
    ids.add(id);
    return { id, filename: a.filename, mime_type: a.mime_type, size_bytes: a.size_bytes, created_at: a.created_at };
  });
  return { id: requireId(n.id), room: roomCode(room), room_id: requireId(n.room_id), sender_id: requireId(senderId), created_at: n.created_at, attachments };
}

async function abortableOperation(signal, operation) {
  signal.throwIfAborted();
  let onAbort;
  const aborted = new Promise((_, reject) => {
    onAbort = () => reject(signal.reason);
    signal.addEventListener('abort', onAbort, { once: true });
  });
  try {
    // Install both rejection handlers before invoking a potentially synchronous
    // operation. Each settled operation releases its own abort wait.
    return await Promise.race([Promise.resolve().then(() => {
      signal.throwIfAborted();
      return operation();
    }), aborted]);
  } finally {
    signal.removeEventListener('abort', onAbort);
  }
}

// The SDK bounds the headers request. This deadline also covers the entire body.
export async function downloadAttachment(client, attachment, { timeoutMs = 15_000, signal } = {}) {
  if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 60_000) throw new Error('Invalid download timeout.');
  signal?.throwIfAborted();
  const controller = new AbortController();
  const combined = signal ? AbortSignal.any([signal, controller.signal]) : controller.signal;
  const deadline = performance.now() + timeoutMs;
  const timeOut = () => controller.abort(new Error('Attachment download timed out.'));
  const checkActive = () => {
    if (performance.now() >= deadline) timeOut();
    combined.throwIfAborted();
  };
  const timer = setTimeout(timeOut, timeoutMs);
  let reader;
  let response;
  try {
    response = await abortableOperation(combined, () => client.attachments.content(attachment.id, combined));
    checkActive();
    if (response.status !== 200 || response.headers.get('content-length') !== String(attachment.size_bytes)
      || response.headers.get('content-type')?.split(';')[0].trim().toLowerCase() !== attachment.mime_type
      || !response.body) throw new Error('Attachment response headers do not match its metadata.');
    reader = response.body.getReader();
    const chunks = [];
    let length = 0;
    let reads = 0;
    while (true) {
      const { value, done } = await abortableOperation(combined, () => reader.read());
      checkActive();
      if (done) break;
      if (!(value instanceof Uint8Array)) throw new Error('Invalid attachment body chunk.');
      length += value.byteLength;
      if (length > attachment.size_bytes || length > MAX_FILE_BYTES) throw new Error('Attachment body exceeds its declared size.');
      if (value.byteLength) chunks.push(value);
      // Ready reads can otherwise starve timers and external cancellation.
      // Empty chunks still count toward yielding, but consume no saved space.
      if (++reads % 64 === 0) {
        await yieldToEventLoop();
        checkActive();
      }
    }
    if (length !== attachment.size_bytes) throw new Error('Attachment body is truncated.');
    return Buffer.concat(chunks, length);
  } finally {
    clearTimeout(timer);
    if (reader) void reader.cancel().catch(() => {});
    else if (response?.body) void response.body.cancel().catch(() => {});
  }
}

async function verifyStored(directory, snapshot, binding) {
  const stat = await lstat(directory);
  if (!stat.isDirectory() || stat.isSymbolicLink()) throw new Error('Stored batch must be a real directory.');
  const receipt = await readJson(join(directory, 'receipt.json'));
  if (!receipt || receipt.version !== 1 || receipt.binding !== binding || receipt.snapshot_hash !== digest(snapshot)
    || digest(receipt.notification) !== digest(snapshot) || !date(receipt.saved_at)
    || !Array.isArray(receipt.files) || receipt.files.length !== snapshot.attachments.length) {
    throw new Error('Stored receipt or remote attachment metadata changed; no files were replaced.');
  }
  const expected = ['receipt.json'];
  for (const [i, a] of snapshot.attachments.entries()) {
    const f = receipt.files[i];
    if (f?.id !== a.id || f.path !== `${a.id}.bin` || f.size_bytes !== a.size_bytes || !/^[0-9a-f]{64}$/.test(f.sha256 ?? '')) {
      throw new Error('Stored file receipt is invalid.');
    }
    expected.push(f.path);
    const bytes = await readBytes(join(directory, f.path), MAX_FILE_BYTES);
    if (bytes.length !== a.size_bytes || sha256(bytes) !== f.sha256) throw new Error('Local attachment bytes changed; no files were replaced.');
  }
  if (JSON.stringify((await readdir(directory)).sort()) !== JSON.stringify(expected.sort())) throw new Error('Unexpected files in stored batch.');
  return receipt;
}

export async function receive({ client, notificationId, room, senderId, connectionKey, directory, signal, timeoutMs }) {
  notificationId = requireId(notificationId);
  senderId = requireId(senderId);
  room = roomCode(room);
  if (typeof connectionKey !== 'string' || !connectionKey) throw new Error('Connection identity is required.');
  signal?.throwIfAborted();
  const release = await acquireLock(directory);
  let staging;
  try {
    const entries = await readdir(directory);
    if (entries.some((name) => name.startsWith('.pending-') || name.startsWith('.write-'))) {
      throw new Error('Interrupted staging exists. Follow README recovery before downloading more files.');
    }
    const names = entries.filter((name) => name !== '.lock');
    if (names.some((name) => { try { return requireId(name) !== name; } catch { return true; } })) throw new Error('Unexpected entry in inbox directory.');
    const exists = names.includes(notificationId);
    if (!exists && names.length >= MAX_BATCHES) throw new Error('Inbox reached its 100-batch limit. Choose a new private directory.');
    const snapshot = snapshotNotification(await client.notifications.getNotification(notificationId), { notificationId, room, senderId });
    signal?.throwIfAborted();
    const binding = digest({ connectionKey, room, senderId });
    const destination = join(directory, notificationId);
    const previous = exists ? await verifyStored(destination, snapshot, binding) : null;
    staging = join(directory, `.pending-${notificationId}-${randomUUID()}`);
    await mkdir(staging, { mode: 0o700 });
    const files = [];
    for (const a of snapshot.attachments) {
      signal?.throwIfAborted();
      const bytes = await downloadAttachment(client, a, { signal, timeoutMs });
      const hash = sha256(bytes);
      if (previous && previous.files.find((f) => f.id === a.id).sha256 !== hash) throw new Error('Remote attachment bytes changed; no files were replaced.');
      const path = `${a.id}.bin`;
      await writeFile(join(staging, path), bytes, { flag: 'wx', mode: 0o600, flush: true });
      files.push({ id: a.id, path, size_bytes: bytes.length, sha256: hash });
    }
    signal?.throwIfAborted();
    if (previous) return { status: 'already_saved', directory: destination, receipt: previous };
    const receipt = { version: 1, binding, notification: snapshot, snapshot_hash: digest(snapshot), files, saved_at: new Date().toISOString() };
    await atomicJson(staging, 'receipt.json', receipt);
    signal?.throwIfAborted();
    await rename(staging, destination);
    staging = undefined;
    return { status: 'saved', directory: destination, receipt };
  } finally {
    if (staging) await rm(staging, { recursive: true, force: true });
    await release();
  }
}
