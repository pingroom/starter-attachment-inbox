import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, readFile, readdir, rm, stat, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { receive, MAX_FILE_BYTES } from '../src/inbox.js';
import { content, notification, offlineClient, NOTIFICATION_ID, FILE_ID, SENDER_ID, ROOM } from '../src/offline.js';
import { loadConnection, requestedRoom, saveConnection, selectRoom } from '../src/config.js';

async function setup(t, transport = {}) {
  const directory = await mkdtemp(join(tmpdir(), 'pingroom-inbox-test-'));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const mock = offlineClient(transport);
  return { ...mock, directory, options: { client: mock.client, notificationId: NOTIFICATION_ID, room: ROOM, senderId: SENDER_ID, connectionKey: 'test', directory } };
}

test('saves streamed bytes atomically with actual SDK routes, private modes, and receipt', async (t) => {
  const { options, calls, directory } = await setup(t);
  const result = await receive(options);
  assert.equal(result.status, 'saved');
  assert.deepEqual(await readdir(directory), [NOTIFICATION_ID]);
  assert.deepEqual(await readFile(join(result.directory, `${FILE_ID}.bin`)), content);
  assert.equal((await stat(result.directory)).mode & 0o777, 0o700);
  assert.equal((await stat(join(result.directory, `${FILE_ID}.bin`))).mode & 0o777, 0o600);
  assert.match(result.receipt.files[0].sha256, /^[a-f0-9]{64}$/);
  assert.deepEqual(calls, [`/api/agent/notifications/${NOTIFICATION_ID}`, `/api/agent/attachments/${FILE_ID}/content`]);
});

test('replay checks remote bytes and preserves original receipt with no duplicate batch', async (t) => {
  const { options, calls, directory } = await setup(t);
  const first = await receive(options);
  const original = await readFile(join(first.directory, 'receipt.json'));
  assert.equal((await receive(options)).status, 'already_saved');
  assert.deepEqual(await readFile(join(first.directory, 'receipt.json')), original);
  assert.equal(calls.length, 4);
  assert.deepEqual(await readdir(directory), [NOTIFICATION_ID]);
});

for (const [name, change] of [
  ['wrong sender', (n) => { n.sender_id = FILE_ID; }],
  ['sender object mismatch', (n) => { n.sender.id = FILE_ID; }],
  ['wrong room', (n) => { n.room.invite_code = 'OTHER'; }],
  ['public room', (n) => { n.room.is_public = true; }],
  ['personal room', (n) => { n.room.type = 'personal'; }],
  ['room ID mismatch', (n) => { n.room_id = FILE_ID; }],
  ['agent origin', (n) => { n.origin_agent = { id: FILE_ID }; }],
  ['nonmanual source', (n) => { n.trigger_source = 'webhook'; }],
  ['handoff', (n) => { n.is_handoff = true; }],
  ['question', (n) => { n.question = { id: FILE_ID }; }],
  ['wrong notification', (n) => { n.id = FILE_ID; }],
  ['empty files', (n) => { n.attachments = []; }],
  ['duplicated file', (n) => { n.attachments.push(n.attachments[0]); }],
  ['oversized metadata', (n) => { n.attachments[0].size_bytes = MAX_FILE_BYTES + 1; }],
  ['invalid file ID', (n) => { n.attachments[0].id = '../../escape'; }],
]) {
  test(`rejects ${name} before any download`, async (t) => {
    const record = notification(); change(record);
    const { options, calls, directory } = await setup(t, { record });
    await assert.rejects(receive(options));
    assert.equal(calls.length, 1);
    assert.deepEqual(await readdir(directory), []);
  });
}

test('original filenames never become filesystem paths', async (t) => {
  const record = notification(); record.attachments[0].filename = '../../outside.txt';
  const { options } = await setup(t, { record });
  const result = await receive(options);
  assert.deepEqual((await readdir(result.directory)).sort(), [`${FILE_ID}.bin`, 'receipt.json']);
  assert.equal(result.receipt.notification.attachments[0].filename, '../../outside.txt');
});

for (const [name, respond] of [
  ['HTTP denial', () => new Response('{"message":"Unauthorized"}', { status: 403, headers: { 'content-type': 'application/json' } })],
  ['wrong length header', () => new Response(content, { headers: { 'content-type': 'text/plain', 'content-length': '1' } })],
  ['missing length header', () => new Response(content, { headers: { 'content-type': 'text/plain' } })],
  ['wrong MIME header', () => new Response(content, { headers: { 'content-type': 'text/html', 'content-length': String(content.length) } })],
  ['truncated bytes', () => new Response(content.subarray(1), { headers: { 'content-type': 'text/plain', 'content-length': String(content.length) } })],
  ['extra bytes', () => new Response(Buffer.concat([content, content]), { headers: { 'content-type': 'text/plain', 'content-length': String(content.length) } })],
]) {
  test(`${name} leaves no completed or partial batch`, async (t) => {
    const { options, directory } = await setup(t, { respond });
    await assert.rejects(receive(options));
    assert.deepEqual(await readdir(directory), []);
  });
}

test('a stalled body is stopped by the full download deadline', async (t) => {
  const { options, directory } = await setup(t, { respond: () => new Response(new ReadableStream({ pull() {} }), { headers: { 'content-type': 'text/plain', 'content-length': String(content.length) } }) });
  await assert.rejects(receive({ ...options, timeoutMs: 20 }), /timed out/);
  assert.deepEqual(await readdir(directory), []);
});

test('external abort cancels an active body and removes staging', async (t) => {
  const controller = new AbortController();
  const { options, directory } = await setup(t, { respond: () => new Response(new ReadableStream({ pull() { setTimeout(() => controller.abort(new Error('cancelled')), 10); } }), { headers: { 'content-type': 'text/plain', 'content-length': String(content.length) } }) });
  await assert.rejects(receive({ ...options, signal: controller.signal }), /cancelled/);
  assert.deepEqual(await readdir(directory), []);
});

test('a failed second file never publishes the first file', async (t) => {
  const record = notification();
  record.attachments.push({ ...record.attachments[0], id: '55555555-5555-4555-8555-555555555555' });
  const { options, directory } = await setup(t, { record, respond: (path) => path.includes(FILE_ID)
    ? new Response(content, { headers: { 'content-type': 'text/plain', 'content-length': String(content.length) } })
    : new Response('{}', { status: 404, headers: { 'content-type': 'application/json' } }) });
  await assert.rejects(receive(options));
  assert.deepEqual(await readdir(directory), []);
});

test('remote metadata changes are refused on replay', async (t) => {
  const record = notification();
  const { options, calls } = await setup(t, { record });
  await receive(options);
  record.attachments[0].filename = 'changed.txt';
  await assert.rejects(receive(options), /metadata changed/);
  assert.equal(calls.length, 3);
});

test('remote byte changes with identical metadata are refused on replay', async (t) => {
  let bytes = content;
  const { options } = await setup(t, { respond: () => new Response(bytes, { headers: { 'content-type': 'text/plain', 'content-length': String(content.length) } }) });
  const first = await receive(options);
  bytes = Buffer.from(content); bytes[0] = 120;
  await assert.rejects(receive(options), /Remote attachment bytes changed/);
  assert.deepEqual(await readFile(join(first.directory, `${FILE_ID}.bin`)), content);
});

test('local file edits fail before any download', async (t) => {
  const { options, calls } = await setup(t);
  const first = await receive(options);
  await writeFile(join(first.directory, `${FILE_ID}.bin`), 'edited');
  await assert.rejects(receive(options), /Local attachment bytes changed/);
  assert.equal(calls.length, 3);
});

test('stored file symlinks are refused', async (t) => {
  const { options } = await setup(t);
  const first = await receive(options);
  const path = join(first.directory, `${FILE_ID}.bin`);
  await rm(path); await symlink('/etc/hosts', path);
  await assert.rejects(receive(options));
});

test('interrupted staging and active locks are preserved and refused', async (t) => {
  const { options, directory, calls } = await setup(t);
  const pending = join(directory, '.pending-interrupted');
  await mkdir(pending);
  await assert.rejects(receive(options), /Interrupted staging/);
  assert.equal(calls.length, 0);
  assert.deepEqual(await readdir(directory), ['.pending-interrupted']);
  await rm(pending, { recursive: true });
  await mkdir(join(directory, '.lock'));
  await assert.rejects(receive(options), /lock exists/);
});

test('binding change and corrupted receipt never reset stored data', async (t) => {
  const { options } = await setup(t);
  const result = await receive(options);
  await assert.rejects(receive({ ...options, connectionKey: 'another-token' }), /receipt/);
  await writeFile(join(result.directory, 'receipt.json'), 'null');
  await assert.rejects(receive(options), /JSON object/);
});

test('connection overrides require the full pair and save private credentials', async (t) => {
  const { directory } = await setup(t);
  await saveConnection(directory, { token: 'saved', room: 'abc123' });
  assert.deepEqual(await loadConnection(directory, {}), { token: 'saved', room: 'ABC123' });
  assert.deepEqual(await loadConnection(directory, { PINGROOM_TOKEN: 'override', PINGROOM_ROOM: 'other' }), { token: 'override', room: 'OTHER' });
  await assert.rejects(loadConnection(directory, { PINGROOM_TOKEN: 'override' }), /both/);
  assert.equal((await stat(join(directory, 'credentials.json'))).mode & 0o777, 0o600);
});

test('100 completed batch names prevent another download', async (t) => {
  const { options, directory, calls } = await setup(t);
  for (let i = 0; i < 100; i += 1) await mkdir(join(directory, `${i.toString(16).padStart(8, '0')}-aaaa-4aaa-8aaa-aaaaaaaaaaaa`));
  await assert.rejects(receive(options), /100-batch limit/);
  assert.equal(calls.length, 0);
});

test('a rejected response cancels its unread body', async (t) => {
  let cancelled = false;
  const { options } = await setup(t, { respond: () => new Response(new ReadableStream({ cancel() { cancelled = true; } }), { headers: { 'content-type': 'text/plain', 'content-length': '999' } }) });
  await assert.rejects(receive(options), /headers/);
  assert.equal(cancelled, true);
});

test('explicit room selection overrides a personal home room after grant preflight', () => {
  const active = { home_room: { invite_code: 'HOME01' } };
  const rooms = [
    { invite_code: 'HOME01', type: 'personal', is_public: false },
    { invite_code: ROOM, type: 'standard', is_public: false },
  ];
  assert.equal(selectRoom(active, rooms, requestedRoom(['--room', ROOM.toLowerCase()])), ROOM);
  assert.throws(() => selectRoom(active, rooms), /standard private room/);
});

test('room selection refuses ungranted/public rooms and invalid arguments', () => {
  const active = { home_room: { invite_code: ROOM } };
  assert.throws(() => selectRoom(active, []), /granted/);
  assert.throws(() => selectRoom(active, [{ invite_code: ROOM, type: 'standard', is_public: true }]), /private/);
  assert.throws(() => requestedRoom(['--room']), /Usage/);
  assert.throws(() => requestedRoom(['--room', '../escape']), /room code/);
  assert.equal(selectRoom(active, [{ invite_code: ROOM, type: 'standard', is_public: false }]), ROOM);
});
