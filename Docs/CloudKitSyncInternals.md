# CloudKit sync internals: CKSyncEngine, SQLiteData and this fork

_Reference written 2026-10-03/04 from work on a real app with a large backlog, using sqlite-data plus this fork. Facts are marked **[Apple]** (Apple documentation), **[measured]** (observed on a real device), **[forum]** (reported by other developers) or **[assumed]** (not verified). How the test mock differs from all of this: [MockFidelity.md](MockFidelity.md)._

## 1. The layers

```
app UI (status line, "sync now")       calls sendChanges/fetchChanges, reads SyncEngine.lastSendOutcome
        |
SQLiteData SyncEngine  (this repo)     triggers on synced tables, metadata DB, record building, conflicts
        | is the CKSyncEngineDelegate
CKSyncEngine  (CloudKit.framework)     state, scheduling, batching, transient retries, push-driven fetch
        |
CloudKit servers
```

In the reference app all tables are registered as `privateTables:` (one record per row, one custom zone `co.pointfree.SQLiteData.defaultZone` in the user's private database).

## 2. What CKSyncEngine does (and does not do) **[Apple]**

Source: Apple's CKSyncEngine reference, [developer.apple.com/documentation/cloudkit/cksyncengine-5sie5](https://developer.apple.com/documentation/cloudkit/cksyncengine-5sie5) (also the WWDC23 session [10188, Sync to iCloud with CKSyncEngine](https://wwdcnotes.com/notes/wwdc23/10188)).

- **The app owns the queue.** It tells the engine what to send with `state.add(pendingRecordZoneChanges:)` / `add(pendingDatabaseChanges:)`. The engine asks for the actual records through the delegate's `nextRecordZoneChangeBatch`, repeatedly, until the delegate returns `nil`.
- **Scheduling is automatic and indeterminate.** "If there are no scheduled sync operations when you invoke these methods, the engine automatically schedules one." Syncs "depend on good system conditions": battery, network, signed-in account, load; they "might be delayed". For an immediate sync the app calls `sendChanges()` / `fetchChanges()` (meant for things like pull-to-refresh or "back up now").
- **Batches:** each batch is one network request, at most **250 records** (saves plus deletes). Our batches are exactly 250 for that reason. The engine dispatches one `sentRecordZoneChanges` event per batch, then `didSendChanges` when the whole send operation ends.
- **Transient errors are handled by the engine:** `notAuthenticated`, `accountTemporarilyUnavailable`, `networkFailure`, `networkUnavailable`, `requestRateLimited`, `serviceUnavailable`, `zoneBusy`. It "waits for the system to be in a good state, and tries again", and respects the retry-after of a rate-limit error.
- **App-specific errors are not:** e.g. `serverRecordChanged` (the app merges and re-queues). The failed change is reported in the event; it is the app's job to put it back.
- **State is opaque and the app must persist it** (`stateUpdate` events). It is what makes pending changes and server change tokens survive a relaunch.
- **Fetching** is driven by silent pushes from a database subscription the engine creates (needs the CloudKit and Remote notifications entitlements) plus scheduled fetches.
- **Accounts:** dormant without an account; `accountChange` events for sign-in/out.
- Do not use it for the public database.

## 3. What SQLiteData adds (`Sources/SQLiteData/CloudKit`)

- **Triggers** on every synced table fire the SQL functions `sqlitedata_icloud_didUpdate` / `didDelete` (Swift `SyncEngine.didUpdate`/`didDelete`) on every write. They are created `IF NOT EXISTS`, so they are not replaced when the registration (`tables:` vs `privateTables:`) changes.
- **Metadata database** `.<App>.metadata-iCloud.<container>.sqlite`, attached to the app database as schema `sqlitedata_icloud`:
  - `sqlitedata_icloud_metadata`: one row per record (`recordPrimaryKey`, `recordType`, `zoneName`, `ownerName`, parent, `lastKnownServerRecord` blob, `_lastKnownServerRecordAllFields` blob, `hasLastKnownServerRecord` (generated), `_isDeleted`, `userModificationTime`). Two blobs per row make it large: **1.1 GB at 204k rows [measured]**, with a 2.25 GB WAL. `_isDeleted` is the last column, behind the blobs, so any filter on it reads the whole table.
  - `sqlitedata_icloud_stateSerialization`: one row per scope (2 = private, 3 = shared) with CKSyncEngine's state.
  - `sqlitedata_icloud_pendingRecordZoneChanges`: in stock SQLiteData only a holding area for changes made while the engine is stopped; in our fork the overflow queue (section 5).
  - `sqlitedata_icloud_recordTypes`, `sqlitedata_icloud_unsyncedRecordIDs`.
- **Record building** (`nextRecordZoneChangeBatch`): per record, one metadata read (the blob), the local row, then a `CKRecord` (reusing the stored server system fields so the change tag is right). Tables that are not `privateTables` get `record.parent` set (for CKShare); ours are private, so no parent.
- **Conflict handling** (`handleSentRecordZoneChanges`): `serverRecordChanged` merges the server record into the local row and the metadata (`upsertFromServerRecord`) and re-queues the save; `zoneNotFound` re-saves the zone; `unknownItem` clears the stored server record and re-queues; referenceViolation/permission errors have their own branches.
- **Account sign-in** calls `enqueueUnknownRecordsForCloudKit` (touch every metadata row without a server record).

## 4. The saved state (what CloudKit stores about its own queue)

`stateSerialization.data` is JSON `{"data": "<base64 of an NSKeyedArchiver binary plist>"}`. Decoded, the root is a `CKSyncEngineState` **[measured]**:

| Field | Meaning (as observed) |
|---|---|
| `pendingRecordModifications` | the pending record changes (`CKSyncEnginePendingRecordZoneChange`, `type` 0 = save, `recordID`). **This is the list that is re-archived on every update.** Members are stored as `NS.object.<n>` keys, not `NS.objects` |
| `pendingZoneChanges`, `pendingAssetSyncs` | pending zone saves/deletes, assets |
| `inFlightRecordModifications`, `inFlightZoneChanges`, `inFlightAssetSyncs` | changes of the batch being sent (were empty in every snapshot we took) |
| `needsToFetchDatabaseChanges` | **flips to True right after a batch with failures** (state before: False, after: True; nothing else changed). The engine fetches before it sends again |
| `zoneIDsNeedingToFetchChanges`, `zoneState`, `serverChangeTokensByZoneID`, `serverChangeTokenForDatabase` | per-zone fetch bookkeeping and change tokens |
| `lastFetchDatabaseChangesDate`, `lastPushReceivedDate` | timestamps |
| `lastAccount`, `lastKnownUserRecordID`, `existingDatabaseSubscriptionID`, `hasPendingUntrackedChanges`, `hasInFlightUntrackedChanges` | account and subscription |

Size: about 350 KB with 1,000 pending changes, 13.6 MB at 41k, 45 MB at about 120k **[measured]**. Decoding script (Python, offline, on a row copied out of the metadata DB):

```python
import json, base64, plistlib
p = plistlib.loads(base64.b64decode(json.load(open('state.json'))['data']))
objs = p['$objects']; root = objs[p['$top']['root'].data]
members = [v for k, v in objs[root['pendingRecordModifications'].data].items() if k.startswith('NS.object.')]
print(len(members), root['needsToFetchDatabaseChanges'])
```

## 5. What we changed, and why

| Change (fork `pfriesch/sqlite-data`) | Problem it solves | Evidence |
|---|---|---|
| Bound the changes in the engine to 1,000; overflow into `sqlitedata_icloud_pendingRecordZoneChanges`, batched inserts, top-up after each sent batch | `CKSyncEngine` archives its whole pending list on every state update; about 40k pending cost about 2 GB and killed the app | lldb: `EXC_RESOURCE` in the state coalescer; 45 MB state |
| Drop a saved state over 16 MB before CloudKit decodes it; rebuild the queue from the metadata | `CKSyncEngine.init` took 10+ minutes decoding 45 MB (the app never finished starting) | thread dump inside `CKSyncEngine.init`; the rebuild found 119,284 changes in 5.5 min |
| Re-queue failed changes **after** `didSendChanges`, from a detached task | After a failed batch the engine stayed idle with the re-queued changes pending, for minutes, until the app was relaunched | section 6 |
| App registers tables as `privateTables:` | No `parent` reference per record (only needed for sharing, which the app does not do) | batches went from 31-43 s to 6-13 s per 250 records (may include CloudKit variance) |

We tried a watchdog that restarts the engine when it goes idle. It worked, but it hides the cause; it was reverted (`7fb3fa6`).

## 6. Why the engine went idle after a failed batch **[measured]**

Reproduced on demand with a temporary harness (kept as `~/Work/sqlite-data-backup/repro-harness.patch`: launch with `SQLITEDATA_FORCE_CONFLICTS=<n>`; it re-queues n already-synced records and sends them without change tags, so CloudKit answers `serverRecordChanged`, text "record to insert already exists").

Observed sequence with stock behavior:
1. batch of 250 returns code 14 for the records; `sentRecordZoneChanges`; the library merges the server record and re-queues the saves with `state.add` **inside that callback**; `didSendChanges`;
2. the state now has `needsToFetchDatabaseChanges = True` and 1,000 pending changes, but **no further fetch or send happens** for 5-8+ minutes (`nextRecordZoneChangeBatch` is never called);
3. `sendChanges()`, re-adding the pending changes, or `fetchChanges()` from our side did nothing (empty cycles); a **user's "sync now" tap minutes later** and **a relaunch** both made it send.

With the same re-queue done 300 ms later from a detached task (after `didSendChanges`): about 1 s later `willFetchChanges`, `didFetchChanges`, `willSendChanges`, next batch. Repeated for dozens of forced failures without stalling.

Conclusion: adding changes from inside a `sentRecordZoneChanges` callback does not make the engine send again; adding them after the operation ended does. This matches an Apple Developer Forums report ([thread 829402](https://developer.apple.com/forums/thread/829402): changes added with `add(pendingRecordZoneChanges:)` while handling `sentRecordZoneChanges`/`fetchedRecordZoneChanges` are not sent until the app is relaunched; reported on iOS 26.5; Apple asked for a Feedback report, no conclusion there) and the documented rule that adding only schedules a sync when none is scheduled (an operation that is still running counts). We see it on current iOS too.

Related gotcha: awaiting a `CKSyncEngine` call from a task created inside a delegate callback traps ("BUG IN CLIENT OF CLOUDKIT: Cannot await a call into CKSyncEngine from within a delegate callback ...") because the task inherits CloudKit's task-local. Use `Task.detached`.

## 7. Error codes we have seen or expect

| Code | Name | Seen | What happens |
|---|---|---|---|
| 14 | `serverRecordChanged` | yes (natural: 250 at once; forced) | not retried by the engine; library merges and re-queues (now after `didSendChanges`); engine fetches, then sends again. Natural cause: the server already has the record but our metadata has no change tag (an acknowledgement was lost when the app was killed, or the queue was rebuilt from metadata). Text: "record to insert already exists" |
| 6 | `serviceUnavailable` | yes (22:20, 22:39, 22:40) | transient: the engine retries "when the system is in a good state"; in our runs it stayed quiet for 12+ minutes because the retry is handed to the iOS activity scheduler, not a timer (see the addendum at the end). The error may carry a retry-after (`CKError.retryAfterSeconds`); we have not logged it yet |
| 7 | `requestRateLimited` | yes (HTTP 429 after an earlier refusal) | transient, carries a retry-after (19-40 s observed); `CKErrorRetryAfterKey` in `userInfo`, read with `retryAfterSeconds` |
| 27 | `limitExceeded` | only in tests | more than 400 items or 2 MB of record data in one request (the real engine sends at most 250 records per batch) |
| 22 | `batchRequestFailed` | no | a failure elsewhere in an atomic batch; library re-queues |
| 23 | `zoneBusy`, 3/4 network, 9 `notAuthenticated` | no | transient |
| 25 | `quotaExceeded` | no | not retried by the engine, and the change leaves its queue [forum/Selig]. The fork re-queues the failed saves 5 minutes later from a detached task (`SyncEngine.quotaRetryDelay`), not through the 100 ms after-send path, so a full account is not hammered. Deletes are not affected. Unit test only (`RealisticMockTests`); not seen on a device |

Code 6 is CloudKit throttling (retry-after 11-76 s), also on plain uploads without any harness; see section 11. **[measured]**

**Shape of the two throttle errors [Apple, TN3162]:** code 6 is the *server* refusing (HTTP 503; `userInfo` has `CKErrorShouldThrottleClient`, `CKRetryAfter`; underlying `CKInternalErrorDomain` 2022). Code 7 (`7/2008`) is the *device* refusing: CloudKit answers locally without sending anything, "rate limited due to an earlier error: ... 503", with its own retry-after. **[assumed]** This explains why forced `sendChanges()` calls after a 6 came back as 7 with fresh retry-afters (section 11, resume option): they were refused on the device, and each one may extend the wait. Not verified; check the in-process log for whether a request left the device.

Apple's TN3162 also says: a retried request may be throttled again with a new retry-after; throttles can be triggered by many devices spiking together (our shared Development container); a **low battery** throttle is separate and ends only when the battery is high again; and turning iCloud off and on does not reset the retry interval and may cause more throttling (Oakley, via Tsai).

## 8. How to investigate (toolkit)

- **Read CloudKit's own log in-process:** `OSLogStore(scope: .currentProcessIdentifier)` with a predicate on subsystem `com.apple.cloudkit` returns the engine's lines (`Engine`, `OP`, `CK`, `Scheduler`), including `failed sending changes`, `scheduling sync with earliest start date` and `Operation ... finished with error ... Retry after N seconds`. No device-log tool needed.
- **Timing/probe patch** for the fork (batch build / CloudKit / result handling, pending count every 15 s, error codes per sent event): a temporary local patch (never commit it).
- **Timeline of engine events**: in the fork, a temporary `dbg()` helper that appends lines to `Documents/<file>.txt` from `handleEvent` (`willSendChanges`, `sentRecordZoneChanges` with failure code counts and `inState`, `didSendChanges`, fetches) and `nextRecordZoneChangeBatch` (batch timing). Read it while the app runs: `xcrun devicectl device copy from --domain-type appDataContainer --domain-identifier <bundle id> --source Documents/<file>.txt ...`. Never commit it.
- **Why a send cycle started**: the raw `CKSyncEngine.Event` has `context.reason` (`.scheduled` / `.manual`) and `context.options.scope` (iOS 17.2+).
- **Engine state**: dump `stateSerialization` (section 4) at `handleStateUpdate`, decode offline.
- **Run the app with a switch**: `xcrun devicectl device process launch -e '{"NAME":"value"}' ...`.
- **Assertion messages from CloudKit**: lldb `breakpoint set -r _assertionFailure`, message String at `$x3` (count) / `$x4` (pointer), `memory read --force --size 1 --format char --count 300 $x4`.
- **Sizes and counts**: pull the metadata DB with the app stopped (copy the `-wal` and `-shm` too; 1.1 GB plus 2.25 GB WAL takes minutes).
- Do not run the app from Xcode while testing with devicectl/Appium (Xcode installs its own build and attaches a debugger). .

## 9. Making the state visible to the user (built 2026-10-04)

What the engine knows that the user cannot see today, and where to get it:

| User-visible fact | Source |
|---|---|
| number of changes waiting to upload | `state.pendingRecordZoneChanges.count` plus the overflow table count (cheap `COUNT(*)` on the small table) |
| a send is running / idle | `SyncEngine.isSendingChanges` / `isFetchingChanges` (public, observable) |
| last attempt: how many saved / failed, which error | `sentRecordZoneChanges` event: counts and `CKError.code` |
| "iCloud asked us to wait N seconds" | `CKError.retryAfterSeconds` on failed saves (transient errors); logged as `retryAfter` in the sent event; observed 11-76 s |
| "waiting for a good moment" | transient codes (6, 7, 23, 3/4, 9) with no retry-after; Apple: the engine waits for good system conditions |
| why nothing is happening although changes are pending | derive: pending > 0, not sending, last result and time |

Built: the fork (commit `a2333e7`) exposes `SyncEngine.lastSendOutcome` (observable; `SendOutcome`: date, savedCount, failedCount, errorCodes, retryAfterSeconds, `isThrottled`) and `pendingChangeCount()` (engine state + buffer + overflow table). Apps can show a status line from it (reserve its height so nothing shifts), e.g. "Last upload 2 min ago: 250 records saved.", "...N saved, M failed (error 14 x M)." or, when throttled, "iCloud asked the app to slow down (asked us to wait 25 s)... Sync now retries immediately." A unit test covers the outcome and the pending count (`PendingChangeBoundTests`).

## 10. Open questions

- Is the post-callback re-queue the intended usage? Partly answered: Apple's sample (`SyncedDatabase.handleSentRecordZoneChanges`) calls `state.add` *inside* the callback, so that is the documented pattern, but nothing says it triggers another send. That matches our measurement and forum thread 829402. A Feedback with the reproduction recipe would still settle it.
- How long does the engine wait after `serviceUnavailable`? Answered in part: it does not follow the server's retry-after (25 s); it schedules a system activity and stayed silent 12+ minutes in the foreground. How long until iOS runs it is still unknown.
- Upload speed: 6-13 s per 250 records ; other CloudKit environments not measured.
- The bounded queue is outside the stock design (engine state = whole queue); Apple documents no limit on pending changes, but 40k+ failed for us.
- Record count dominates upload time (about 1,200 records/min is the rough ceiling); packing many small rows into one record cuts the load but is at odds with the typed-columns rule.

## 11. Measurements

- **One engine cycle, 250 records:** build the batch 1.4 s (0.8-1.9; per record one metadata read, one user-DB read and one write transaction), CloudKit 3.6 s (2.4-4.4), handle the result 0.4 s; about 6.3 s per cycle, 2,360 records/min. A raw `CKDatabase.modifyRecords` of the same 250 records took 1.7-2.4 s. The engine's operations run at `qos=Utility` through a "container throttle queue" (in-process log). **[measured]**
- **Claims we did not reproduce:** Selig reports uploads are split into 1 MB batches and a single `CKRecord` may be at most 1 MB. That differs from our 250 records / 2 MB request (250 x 3 KB = 750 KB passed, 5 MB was rejected), so treat his 1 MB batch figure as unconfirmed. The 1 MB per-record limit is the one to respect. **[forum]**
- **Record size does not matter below ~3 KB:** 250 records of 40 B, 200 B and 3 KB all took 1.8-2.2 s raw; 250 x 20 KB (5 MB) was rejected (request size limit; Apple: 400 items and 2 MB per request). Count, not size, is the cost. **[measured]**
- **`record.parent` doubles the save time:** 2.10 s vs 4.22 s per 250 records (10 alternating pairs, paced, no throttling). Tables registered with `tables:` (shareable) get a parent on child records; use `privateTables:` when nothing is shared. **[measured]**
- **Throttling:** refusals arrive as `serviceUnavailable` (6) or `requestRateLimited` (7), retry-after 11-76 s; the refused request returns in 0.3-0.5 s. The engine sent 3-13 batches (750-1,750 records, 20-80 s) before the first refusal. Raw requests of 250 records, one every 12-15 s (about 1,200 records/min), passed 20 of 20; one every 5-7 s trips it. Apple (TN3162): limits are unpublished and not configurable; respect the retry-after. **[measured]**
- **After a refusal the engine goes quiet** for 32 s to 12+ minutes: it logs `scheduling sync with earliest start date` and hands the retry to the system activity scheduler, not a timer, and does not follow the server's retry-after. The refusal is reported as one `sentRecordZoneChanges` with every record of the request in `failedRecordSaves` (code 6), then `didSendChanges`. What decides the quiet time is unknown. Forcing `sendChanges()` after the retry-after (`resumesSendingAfterThrottle`) made it worse. **[measured]**
- **Real event order** (from a timing log, one launch): `stateUpdate`, `willFetchChanges`, `stateUpdate`, `didFetchChanges`, `willSendChanges`, `stateUpdate`, `sentDatabaseChanges`, then per batch `nextRecordZoneChangeBatch` (called once per batch), `stateUpdate` (twice), `sentRecordZoneChanges`, `stateUpdate`, repeated until the pending list is empty or a request fails, then one `didSendChanges` and a last `stateUpdate`. A cycle contains many batches. **[measured]**
- **Metadata DB:** `lastKnownServerRecord` averages 2.4 KB, `_lastKnownServerRecordAllFields` 7.9 KB; 232,553 rows hold 250 MB + 813 MB of blobs in a 1.6 GB file next to a 1.4 GB WAL. A filter on `_isDeleted` took 0.12 s on a Mac copy, so reordering columns is not shown to help; test WAL checkpointing first. **[measured]**
- **Full fetch:** after the saved state was dropped, 39 pages of 200 records at about 3.4 s per page. **[measured]**

## Sources

- [CKSyncEngine, Apple documentation](https://developer.apple.com/documentation/cloudkit/cksyncengine-5sie5)
- [WWDC23 10188: Sync to iCloud with CKSyncEngine (notes)](https://wwdcnotes.com/notes/wwdc23/10188)
- [Apple sample: sample-cloudkit-sync-engine](https://github.com/apple/sample-cloudkit-sync-engine): the README says little, the code is the reference. Handles `serverRecordChanged`, `zoneNotFound`, `unknownItem` by re-queueing inside `sentRecordZoneChanges`; treats `requestRateLimited` as an unknown error (we treat it as transient); tests run two real engines against real CloudKit with `sendChanges()`/`fetchChanges()`
- [TN3162: Understanding CloudKit throttles](https://developer.apple.com/documentation/technotes/tn3162-understanding-cloudkit-throttles): error shapes (6 vs 7), retry-after, low-battery throttle. It says `CKSyncEngine` "automatically re-schedules after the retry-after time"; our measurement (section 11) disagrees
- [Christian Selig, CKSyncEngine questions and answers (2026)](https://christianselig.com/2026/01/cksyncengine/): `quotaExceeded` drops the change, zone deletion reasons, one engine per database, 1 MB record limit
- [Michael Tsai, CloudKit throttles and debugging](https://mjtsai.com/blog/2024/05/29/cloudkit-throttles-and-debugging) (links Oakley: do not toggle iCloud to clear a throttle)
- Apple Developer Forums: [CKSyncEngine doesn't send changes until app restarts (829402)](https://developer.apple.com/forums/thread/829402), [CKSyncEngine API design problems and maintenance status (771941)](https://developer.apple.com/forums/thread/771941) (an Apple engineer confirms batches are sent serially, undocumented), [CKSyncEngine keeps attempting to sync the same record (772887)](https://developer.apple.com/forums/thread/772887)
- [SQLiteData](https://github.com/pointfreeco/sqlite-data) (the library we fork)

## Addendum 2026-10-03: why the engine goes quiet after a throttle

After a refused request (`serviceUnavailable` with retry-after, or `partialFailure`) the engine logs `scheduling sync with earliest start date` and hands the retry to the system activity scheduler (no timer). With the app in the foreground it stayed silent for 12+ minutes in two runs while the server's retry-after was 25 s. **Later correction (2026-10-04):** a third run recovered on its own after 32-130 s (the engine's own scheduled retries), so the wait varies from about half a minute to 12+ minutes; what decides it is not known. Engine requests run at `qos=Utility` through a "container throttle queue".

## Option: resume sending after a throttle (opt-in, off by default)

**What:** `SyncEngine.resumesSendingAfterThrottle` (fork, default `false`). When a send is refused with `serviceUnavailable` or `requestRateLimited` (`SendOutcome.isThrottled`), the fork waits the server's retry-after (at least 5 s, plus up to 5 s jitter, 30 s if the server gave none) and then calls `CKSyncEngine.sendChanges()`, the same call a user-initiated "sync now" makes. A new throttle schedules the next attempt; only one attempt is pending at a time.

**Why it exists:** without it the engine follows Apple's documented behavior and hands the retry to the iOS activity scheduler. Measured here: silent for 12+ minutes while the server's wait was 25 s.

**Trade-offs (why it is not the default):**
- It is a deliberate step away from "the engine knows best". Apple's TN3162 says to respect the retry-after and avoid many requests in a short time; this respects the retry-after but keeps asking at its end, and CloudKit may answer with a fresh, possibly longer throttle (observed 11-76 s).
- It bypasses the system's own conditions for background work (battery, network quality, thermal state); a long backlog can keep the radio busy.
- More requests while the server says it is loaded; the Development container is shared by all our test traffic.
- Not unit-testable with the mock engine (it only acts on a real `CKSyncEngine`); verified on the device only.

**In the app:** Settings > iCloud details > "Retry right after iCloud's wait time" (`UserDefaults` key `cloudSyncResumeAfterThrottle`, applied when the engine is attached and when toggled).

**Result of the device test (2026-10-04): it did not help, it made it worse.** With the option on, 390 s of running: after the first refusal every forced `sendChanges()` (fired 21-42 s later, at or after the stated retry-after) was refused again (`serviceUnavailable` 34-43 s, then `requestRateLimited` 19-40 s) and **0 records were saved**; 12 consecutive refusals, each with a fresh retry-after. The same build with the option off (same conditions) went from a refusal at 78 s to successful 250-record batches at 155 s and 300 s: the engine's own scheduled retries got through (gaps of 44 s, 32 s and 130 s). Two retriers (the engine's own and ours) double the request rate while the server is already refusing. One run each, so not conclusive, but the direction is clear: **leave it off**. A better version would back off beyond the retry-after (for example 2x, growing) or only fire when the engine has been silent longer than a few minutes.

