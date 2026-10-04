---
name: cloudkit-sync
description: How CKSyncEngine and SQLiteData's SyncEngine behave in practice (scheduling, batching, throttling, saved state, error codes, why the engine goes idle) and what this fork changed. Use when touching Sources/SQLiteData/CloudKit, debugging stalled or slow sync, large backlogs, memory blowups, or sync status UI.
---

# CloudKit sync: CKSyncEngine, SQLiteData, this fork

Facts are marked **[Apple]** (documented), **[measured]** (observed on a real device) or **[assumed]**.

## Layers

app → `SyncEngine` (this repo: triggers, metadata DB, record building, conflicts; is the `CKSyncEngineDelegate`) → `CKSyncEngine` (state, scheduling, batching, transient retries, push fetch) → CloudKit servers.

## CKSyncEngine rules [Apple]

- The app owns the queue: `state.add(pendingRecordZoneChanges:)`; the engine pulls records via `nextRecordZoneChangeBatch` until it returns `nil`.
- Scheduling is automatic and conditional (battery, network, account); adding changes only schedules a sync if none is scheduled. `sendChanges()`/`fetchChanges()` are the immediate, user-initiated calls.
- A batch is one request, at most 250 records. A request carries at most 400 items and 2 MB of record data (`limitExceeded`); 250 x 20 KB was rejected.
- The engine retries transient errors itself (`notAuthenticated`, `accountTemporarilyUnavailable`, `networkFailure/Unavailable`, `requestRateLimited`, `serviceUnavailable`, `zoneBusy`). App-specific errors (`serverRecordChanged`) are the app's job: merge and re-queue.
- State is opaque; the app must persist it (`stateUpdate` events). Fetching is push-driven (needs CloudKit + remote-notification entitlements).

## What SQLiteData adds

- Triggers on every synced table call `sqlitedata_icloud_didUpdate/didDelete` per write. Created `IF NOT EXISTS`, so changing `tables:` vs `privateTables:` does not replace them.
- Metadata DB (attached as `sqlitedata_icloud`): `sqlitedata_icloud_metadata` (one row per record, two server-record blobs of ~8 KB total: 1.1 GB at 204k rows), `_stateSerialization` (one row per scope: 2 private, 3 shared), `_pendingRecordZoneChanges` (stock: only while engine stopped; fork: overflow queue), `_recordTypes`, `_unsyncedRecordIDs`.
- `handleSentRecordZoneChanges`: `serverRecordChanged` merges server record into row+metadata and re-queues; `zoneNotFound` re-saves the zone; `unknownItem` clears the stored server record and re-queues.
- Sign-in runs `enqueueUnknownRecordsForCloudKit` (touches every metadata row without a server record).
- `privateTables:` (no `record.parent`) vs `tables:` (parent set, needed only for CKShare): parent costs about 2x per save, verified (2.10 s vs 4.22 s per 250 records). Use `privateTables:` when nothing is shared. Do not quote the earlier "3-5x"; it mixed in time-of-day variance.

## Problems found and fixes (all in this fork)

1. **Unbounded pending changes crash the app.** CKSyncEngine re-archives its whole pending list on every state update: ~50 KB transient memory per change, ~40k pending ≈ 2 GB → `EXC_RESOURCE`. Persisted state reached 45 MB. Fix: cap 1,000 in the engine (`maxInMemoryPendingChanges`), overflow into the pending table with batched inserts, top up after each sent batch (serialize top-ups; two concurrent ones exceeded the bound). `start()` no longer loads the whole table.
2. **Oversized saved state hangs `CKSyncEngine.init`** (45 MB took 10+ min to decode). Fix: `restoredState` drops states over 16 MB (normal ~350 KB; 13.6 MB still loaded fine) and `rebuildPendingChangesFromMetadata` rebuilds the queue (119k changes in ~5.5 min). Cost: CloudKit forgets change tokens, so the next fetch is full.
3. **Engine idle after a failed batch.** Re-queueing with `state.add` inside the `sentRecordZoneChanges` callback leaves the engine silent (state: `needsToFetchDatabaseChanges = true`, changes pending) for minutes until relaunch or manual `sendChanges()`. Same as Apple forum thread 829402. Fix: hold failed changes and add them after `didSendChanges` from `Task.detached` (~300 ms later the engine fetches and sends within a second). Do not use a restart watchdog.
4. **Awaiting CKSyncEngine inside a delegate callback traps** ("BUG IN CLIENT OF CLOUDKIT"): the Task inherits CloudKit's task-local. Use `Task.detached`.

5. **A whole-table `UPDATE` blocks the app's database.** Start (new synced table) and sign-in queued every row in one write transaction; on a large table it held the writer for minutes, and in GRDB each new `ValueObservation` keeps a pool reader until it gets the writer, so the app ran out of readers. Fix: `touchRows`, 1,000 rows per transaction (keyset on primary key / rowid). Never queue a whole table in one write.

## Throttling and the engine going quiet [measured]

- CloudKit throttles by request rate over a window: ~750-1,750 records within 20-40 s trips it; roughly under 1,200 records/min passes (20 requests of 250 paced 12-15 s apart were never throttled; the engine's 5-7 s cadence is refused after 3-13 batches).
- Refusal arrives as `serviceUnavailable` (6) or `requestRateLimited` (7) with `retryAfterSeconds` (observed 11-76 s). The refused request returns in 0.3-0.5 s.
- The engine then logs `scheduling sync with earliest start date` and hands the retry to the iOS activity scheduler, not a timer. It does **not** follow the server's retry-after. Quiet times seen: 32 s to 12+ min, cause unknown.
- Retrying ourselves at the retry-after (`resumesSendingAfterThrottle`) made it worse: 12 consecutive refusals, 0 records saved. Two retriers double the request rate. Leave it off; a better version would back off beyond the retry-after or only fire after minutes of silence.
- Engine requests run at `qos=Utility` through a "container throttle queue". Record size up to 3 KB does not change batch time; fewer/bigger records cut request count.

## Saved state format

`stateSerialization.data` is JSON `{"data": "<base64 NSKeyedArchiver plist>"}`; root is a `CKSyncEngineState`. Key fields: `pendingRecordModifications` (members stored as `NS.object.<n>`; re-archived on every update), `needsToFetchDatabaseChanges`, `serverChangeTokensByZoneID`, `lastFetchDatabaseChangesDate`, `lastAccount`. Decode offline:

```python
import json, base64, plistlib
p = plistlib.loads(base64.b64decode(json.load(open('state.json'))['data']))
objs = p['$objects']; root = objs[p['$top']['root'].data]
members = [v for k, v in objs[root['pendingRecordModifications'].data].items() if k.startswith('NS.object.')]
print(len(members), root['needsToFetchDatabaseChanges'])
```

## Error codes

| Code | Name | Handling |
|---|---|---|
| 14 | `serverRecordChanged` | Not retried by the engine. Library merges, re-queues after `didSendChanges`. Natural cause: server has the record but metadata lacks the change tag (lost ack, or queue rebuilt from metadata). "record to insert already exists" |
| 6 | `serviceUnavailable` | transient, throttle; see above |
| 7 | `requestRateLimited` | transient, honors retry-after |
| 25 | `quotaExceeded` | Engine drops the change. Fork re-queues saves after `quotaRetryDelay` (30 s, guess); deletes fine. `SendOutcome.isQuotaExceeded` |
| 27 | `limitExceeded` | more than 400 items / 2 MB per request |
| 22 | `batchRequestFailed` | failure elsewhere in an atomic batch; re-queued |
| 23/3/4/9 | `zoneBusy`, network, `notAuthenticated` | transient |

## Investigating

- **CloudKit's own log in-process:** `OSLogStore(scope: .currentProcessIdentifier)`, predicate subsystem `com.apple.cloudkit` (categories `Engine`, `OP`, `CK`, `Scheduler`). Shows `failed sending changes`, `scheduling sync...`, `Retry after N seconds`. No device-log tool needed.
- **Engine event timeline:** temporary `dbg()` in `handleEvent` appending to a file in Documents (never commit); pull with `xcrun devicectl device copy from --domain-type appDataContainer ...`.
- **Why a cycle started:** raw `CKSyncEngine.Event` has `context.reason` (`.scheduled`/`.manual`) and `context.options.scope`.
- **Assertion text from CloudKit:** lldb `breakpoint set -r _assertionFailure`, message at `$x3`/`$x4`.
- **Metadata DB size/counts:** pull with the app stopped, copy `-wal` and `-shm` too.
- Do not run the app from Xcode while testing with devicectl/Appium; Xcode installs its own build and attaches a debugger.

## Status UI hooks (fork API)

`SyncEngine.lastSendOutcome` (observable `SendOutcome`: date, savedCount, failedCount, errorCodes, retryAfterSeconds, `isThrottled`), `pendingChangeCount()` (engine state + buffer + overflow table), `isSendingChanges`/`isFetchingChanges`. Derive "stalled": pending > 0, not sending, last result and time. Reserve UI space so status text does not shift layout.

## Open questions

- Metadata `_isDeleted` filter was fast (0.12 s) on a Mac copy; the 5.5 min rebuild on the phone may be a never-checkpointed 1.4 GB WAL or cold flash. Test WAL checkpoint / `journal_size_limit` before touching the schema.
- How long iOS waits after `serviceUnavailable`: unknown. Speed in other CloudKit environments: not measured.
- `MockSyncEngine` fidelity: most gaps are closed behind opt-in switches (realistic cycle, cost/throttle profile, fuzzing); what is modeled, why, and what is not is in `Docs/MockFidelity.md`. Full reference: `Docs/CloudKitSyncInternals.md`.
- The reads/writes in `nextRecordZoneChangeBatch` are batched now (one metadata read, one read per table, one write per batch); the gain was not timed on a device.

## Sources

Apple CKSyncEngine docs; WWDC23 10188; TN3162 (throttles); Apple forum threads 829402, 771941, 772887; `apple/sample-cloudkit-sync-engine`.
