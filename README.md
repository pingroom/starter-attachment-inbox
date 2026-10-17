# PingRoom attachment inbox

Save files shared in PingRoom to a private local directory for your import workflow. Supply one notification UUID. The starter checks the room, sender account, and attachment metadata, then downloads the files with the official SDK.

The result is a folder of generated `.bin` filenames and a receipt with original names, sizes, and SHA-256 hashes. Files stay in quarantine: the starter does not open, extract, parse, or execute them. It sends no pings.

## Try it offline

Requires Node 22 or newer. Create a repository with **Use this template**, clone it, then:

```sh
npm ci
npm test
npm run demo
```

The demo uses the real SDK with an in-memory transport, saves a sample file, verifies a repeated import, and removes its temporary directory. It needs no account, credentials, or network after installation.

## Connect

1. Install or open [PingRoom](https://pingroom.io/i) and sign in. Use an existing **standard private room**. The account sharing files needs Pro to upload attachments; this starter's read operations do not require Pro.
2. Run `npm run connect`, open the printed approval link, grant the inbox room, and choose it as the private delivery room. To select a different room you already granted, use `npm run connect -- --room ROOM_CODE`. The command checks the selected room is standard and private, then stores the token and room in ignored `.pingroom/credentials.json` with private permissions. It does not send a test ping. Reuse the pending link while completing approval.
3. Copy `.env.example` to `.env`. Set `TRUSTED_SENDER_ID` to the exact UUID of the account whose files you accept. Verify the account independently; a display name is not an identity check.
4. Pass the UUID of a notification with 1–4 attached files:

```sh
npm start -- 11111111-1111-4111-8111-111111111111
```

The UUID above is illustrative. Use the real notification's `id` from your existing integration or a PingRoom SDK history response. To inspect recent visible file notifications after connecting:

```sh
node --input-type=module <<'JS'
import { PingRoom } from '@pingroom/sdk';
import { loadConnection, loadEnvironment } from './src/config.js';
loadEnvironment();
const { token, room } = await loadConnection();
const pr = new PingRoom({ token, timeoutMs: 5000 });
const page = await pr.notifications.list({ limit: 25 });
for (const n of page.data) {
  if (n.room?.invite_code === room && n.attachments?.length) {
    console.log({ notification: n.id, sender: n.sender?.id, name: n.sender?.name, files: n.attachments.length });
  }
}
JS
```

This lists one visible history page. `sender.id` supplies the UUID for the account you independently verified. For an existing credential, set **both** `PINGROOM_TOKEN` and `PINGROOM_ROOM`; a partial override is refused. Otherwise, the saved pairing room is used. Production requests go only to `https://api.pingroom.io`.

Successful output identifies `.quarantine/<notification-id>/`. The immutable `receipt.json` maps each `<attachment-id>.bin` to its original filename and hash. A successful return means the bytes were saved locally, not that their contents are safe or that an import has run.

## Policy and bounds

- The notification must be visible to the credential and match the configured room and sender UUID. The room must currently be standard and private. The source must be `manual`; agent-origin pings, Questions, and handoffs are refused. This checks API account provenance, not proof that a particular person operated the phone.
- Maximum four attachments, 5 MiB per file, 20 MiB per batch. Metadata and response length/type must agree. Each streamed download has a 15-second deadline, including its body. There are no automatic network retries. The SDK metadata request has a 5-second timeout.
- Each file is stored under its attachment UUID with a `.bin` extension. Original names are metadata only. Files are written with mode `0600`; directories use `0700`. Keep the directory outside web-served or automatically imported folders.
- All files are staged before one atomic directory rename publishes the batch. A normal failure removes that attempt's staging and leaves existing batches unchanged. A process crash can leave private staging that needs the recovery steps below.
- A repeated ID rechecks current remote metadata, local hashes, and downloaded remote hashes. It returns `already_saved` only if they agree, without replacing the receipt or batch. Current API access is required even for a replay; a 403/404 is a failure, not permission to trust an old copy automatically.
- Each receipt is bound to the credential fingerprint, room, and trusted account. A credential or policy change needs a new directory. A directory holds at most 100 completed batches: up to 2,000 MiB of file bytes plus metadata. Nothing is automatically pruned.

SHA-256 records the bytes observed by this download. PingRoom does not supply an independent signed file digest. The check detects changes relative to the stored receipt; it does not protect against someone who can rewrite both your files and receipt. Directory rename provides atomic visibility, not a guarantee against storage-device failure.

Uses `@pingroom/sdk@0.6.0`, pinned with a lockfile and a 14-day npm release-age policy. CI tests Node 22 and 24 using mocks.

## Recovery and adaptation

An existing `.quarantine/.lock` blocks concurrent processes. Read its `owner.json` and confirm that process has stopped before removing the lock directory. If `.pending-*` staging remains after a crash, inspect it and move it **outside** the inbox directory or remove that incomplete attempt; then rerun the same notification ID. Never rename partial staging into a completed batch yourself.

For a changed file, mismatched receipt, or denied API read, investigate the source and preserve the original batch. The starter will not repair or overwrite it. To start a separate import under a new credential or policy, set `INBOX_DIRECTORY` to a new private directory. Preserve receipts for deduplication; deleting a completed folder makes that ID eligible for import again.

Call `receive()` from `src/inbox.js` with an explicit notification ID in your own workflow. After it succeeds, perform your own file scanning and format validation before copying approved bytes into an importer. Use `receipt.files[].path`, not the original filename, to locate the downloaded bytes. No deployment, external ticket updates, or import actions are included.

This starter receives files from PingRoom. [Artifact delivery](https://github.com/pingroom/starter-artifact-delivery) sends local report files into a room; [room archive](https://github.com/pingroom/starter-room-archive) exports visible feed history for offline search.

MIT licensed; see [LICENSE](LICENSE).
