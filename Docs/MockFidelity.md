# Making `MockSyncEngine` and `MockCloudDatabase` behave like CloudKit

_2026-10-04. Files: `Sources/SQLiteData/CloudKit/Internal/MockSyncEngine.swift`, `MockCloudDatabase.swift`, `MockCloudContainer.swift`. Reference for what the real thing does: [CloudKitSyncInternals.md](CloudKitSyncInternals.md). "Real" below means CKSyncEngine and CloudKit as measured on an iPhone (iOS 27.0.1, Development environment)._

## Why this matters

Two fixes in this fork could not be tested with the mock and were found only on a device: (1) re-queueing failed changes inside the `sentRecordZoneChanges` callback leaves the real engine idle, (2) transient throttling hands the retry to the system scheduler and the engine goes quiet for minutes. The mock cannot produce either situation, so the existing 317 tests pass whether or not the fixes are present. Tests also had to use a bound of 100 instead of the real 250/1,000 because the mock rejects 200 or more records.

Principle for every change below: the mock should be **as strict as the real thing** (fail the way CloudKit fails) and **as slow-to-restart as the real engine**, so a test can no longer pass on behavior the real engine does not have.

## Deviations, ordered by how much they hide

_This is the original gap list (the "Mock today" column is the state before 2026-10-04). What is closed, and how, is in "What the mock does now, and why" below._

| # | Real | Mock today | Where | Consequence |
|---|---|---|---|---|
| 1 | A send **cycle** has `willSendChanges`, then N batches (each: `nextRecordZoneChangeBatch`, request, `sentRecordZoneChanges`), then one `didSendChanges`. Fetch has `willFetchChanges`/`didFetchChanges` (also `willFetchRecordZoneChanges`/`didFetchRecordZoneChanges`). `stateUpdate` is posted after nearly every state change | Posts only `sentRecordZoneChanges`, `sentDatabaseChanges`, `fetchedRecordZoneChanges`. Never `will/didSendChanges`, `will/didFetch*`, `stateUpdate` | `MockSyncEngine.sendChanges/fetchChanges`, `SyncEngine.sendPendingRecordZoneChanges` | `isSendingChanges`/`isFetchingChanges` never go true; the deferral after `didSendChanges` never runs (the code path is guarded `syncEngine is CKSyncEngine`), `lastSendOutcome`-based UI cannot be tested end to end |
| 2 | **Changes added inside a `sentRecordZoneChanges` callback do not start another send**; added after `didSendChanges` they schedule one (forum thread 829402, measured) | `state.add` is instantaneous and the test drives sending by hand, so there is no notion of "the engine will not send again" | whole mock engine | The bug fixed in `f0e8fb5` is invisible; reverting the fix still passes |
| 3 | A batch holds **at most 250 records** (saves + deletes) | `recordZoneChangeBatch(pendingChanges:recordProvider:)` takes every pending change | `MockSyncEngine.recordZoneChangeBatch` | Tests with hundreds of changes send one huge batch, then hit the 200 limit below |
| 4 | A request may carry **400 items and 2 MB** of record data, else `limitExceeded` (27) | `< 200` items, no byte limit | `MockCloudDatabase.modifyRecords` (`guard ... < 200`), `records(for:)` (`ids.count < 200`) | The mock is stricter than the server in count, blind in bytes |
| 5 | **Transient refusals**: `serviceUnavailable` (6) / `requestRateLimited` (7) with `CKErrorRetryAfterKey`; the whole request fails and is reported as one `sentRecordZoneChanges` with every record in `failedRecordSaves`; then the engine waits for the scheduler (32 s to 12+ min), not for the retry-after | No way to make a request fail; `modifyRecords` only throws account/size errors | `MockCloudDatabase` | Throttle handling, `SendOutcome.isThrottled`, `resumesSendingAfterThrottle` and "engine quiet" are untestable |
| 6 | Failed changes stay **in flight** until the request ends; on whole-request failure the engine reports them as failed | `recordZoneChangeBatch` removes the saves from pending when it builds the batch, `sendPendingRecordZoneChanges` removes saved and failed ones | `MockSyncEngine.recordZoneChangeBatch`, `SyncEngine.sendPendingRecordZoneChanges` | A crash or cancel between build and result loses changes in the mock; real state keeps `inFlightRecordModifications` and moves them back to pending on init |
| 7 | **Fetches are paged** (200 records per page, `moreComing`) with per-zone change tokens that expire (`changeTokenExpired`); a dropped state means a full fetch | One `fetchedRecordZoneChanges` with everything newer than a single integer tag | `MockSyncEngine.fetchChanges` | Large-fetch behavior, paging order and token loss (the 16 MB state drop) are untested |
| 8 | The state is archived on every update (`stateUpdate` with a serialization); its size grows with pending changes (about 50 KB transient per change, 45 MB at 120k) | `MockSyncEngineState` is two in-memory ordered sets, no serialization | `MockSyncEngineState` | The bounded queue and the oversized-state drop (`restoredState`) can only be tested by counting, not by size or restore |
| 9 | Zone-level errors a server can send: `zoneNotFound`, `userDeletedZone`, `quotaExceeded`, `zoneBusy`, `changeTokenExpired`, `networkFailure` | `zoneNotFound`, `referenceViolation`, `permissionFailure`, `serverRecordChanged`, `unknownItem`, `batchRequestFailed`, `invalidArguments`, `serverRejectedRequest` | `MockCloudDatabase.modifyRecords` | No test for quota, zone purge or busy |
| 10 | `savePolicy` `.ifServerRecordUnchanged` for engine saves; the server merges per field with change tags that are opaque strings | `.allKeys` / `.changedKeys` hit `fatalError()`; a save replaces the stored record wholesale (`// TODO: This should merge`); tags are one global integer | `MockCloudDatabase` | Fine for now; the integer tag makes cross-zone tag comparison work by accident |
| 11 | Latency and a request rate the server tolerates (about 1,200 records/min; 250 records take ~2 s raw, ~6 s through the engine) | Instant | whole mock | Time-based logic (pacing, backoff, retry-after) cannot be tested |
| 12 | `automaticallySync`: the engine schedules sends/fetches by itself after `state.add` | Tests call `processPendingRecordZoneChanges` by hand; `MockSyncEngine.sendChanges` calls the same | `MockSyncEngine` | Nothing checks that adding a change *causes* a send |

## Proposed design

Keep the existing hand-driven API (existing tests keep working) and add an **opt-in realistic mode** on the mock, switched per test with a configuration value (for example `MockSyncEngine.Behavior`), default off until the existing tests pass with it on.

1. **Events and cycle structure (fixes 1, 6).** Introduce one function, `SyncEngine.runSendCycle(scope:)`, that does what the real engine does: post `willSendChanges`; loop { build a batch (`nextRecordZoneChangeBatch`), mark its records in flight, call the database, post `sentRecordZoneChanges`, remove from in-flight }; stop when the batch is `nil` or a request failed; post `didSendChanges`. `processPendingRecordZoneChanges` becomes "run one batch" or "run the cycle" depending on a parameter, so old tests keep one batch per call. Same for fetch with `willFetchChanges` / `didFetchChanges`. Post `stateUpdate` with a serialization of the pending lists (can be the same JSON the metadata DB stores) so `handleStateUpdate`, `restoredState` and the 16 MB guard can run for real.
2. **Batch size and limits (fixes 3, 4).** Constants `maxBatchRecords = 250` in the mock engine, `maxItemsPerRequest = 400` and `maxBytesPerRequest = 2_000_000` in the database; bytes estimated from `CKRecord.allKeys()` values and `encodedSystemFields`. Both throw `CKError(.limitExceeded)` like the server. Update `PendingChangeBoundTests` to use 1,000 and drop the per-instance bound workaround afterwards.
3. **Scheduling rule (fix 2, 12).** Model the documented/measured rule explicitly: the mock engine has `isSendCycleRunning` and `isSendScheduled`. `state.add` while a cycle is running does **not** schedule another; `state.add` outside a cycle schedules one (the test awaits it with `await syncEngine.idle()` or advances a test clock). A change added during a callback therefore stays pending until something outside schedules: exactly the real failure. This single rule makes the `f0e8fb5` regression testable (test: fail a batch, assert the engine sends again without a manual nudge; it fails on the stock library and passes with the deferral).
4. **Error and throttle injection (fixes 5, 9).** A small hook on `MockCloudDatabase`, for example `failNextRequests(_ count: Int, with: CKError)` and `errorInjector: (ModifyRequest) -> CKError?`, plus a helper `CKError.throttled(retryAfter:)` that builds `serviceUnavailable` with `CKErrorRetryAfterKey` in `userInfo`. When a request fails as a whole, every record goes to `failedRecordSaves` with that error (as observed), `didSendChanges` follows and the mock engine enters **scheduler wait**: it will not send again until the test calls `advanceScheduler(by:)` or `syncEngine.sendChanges()` (modeling "quiet for 32 s to 12+ minutes"). This makes `SendOutcome.isThrottled`, `lastSendOutcome`, `resumesSendingAfterThrottle` (point it at the mock engine instead of `CKSyncEngine`) and the status UI testable.
5. **Fetch paging and tokens (fix 7).** Return records in pages of 200 as separate `fetchedRecordZoneChanges` events inside one `willFetchChanges`/`didFetchChanges` pair; keep a per-zone integer token; support `expireChangeToken(zoneID:)` to answer with `changeTokenExpired` and force a full fetch.
6. **Clock and rate limit (fix 11), optional.** Inject a `Clock` (swift-dependencies `continuousClock`, already used elsewhere) and a token bucket in the database (about 1,200 records/min, burst 750-1,750) that fails with `serviceUnavailable` + retry-after when drained. Lets pacing/backoff logic be tested without sleeping.
7. **Cheap fidelity fixes that need no new machinery:** make `savePolicy` explicit and support `.allKeys`/`.changedKeys` instead of `fatalError()`; implement the `TODO` (merge changed fields on save); give `_recordChangeTag` a per-zone counter; make `cancelOperations` clear in-flight work; keep `OrderedSet` dedup and document that real state also dedups (**[assumed]**, verify).

## What the mock does now, and why (2026-10-04)

Everything below is covered by `Tests/SQLiteDataTests/CloudKitTests/RealisticMockTests.swift` unless noted. **Always on** changes affect every test; **opt-in** ones need a switch, so the 317 older tests (which drive one batch by hand and assert exact snapshots) keep their behavior.

### Always on

| Change | Why |
|---|---|
| A batch holds at most 250 records (`SyncEngine.maxBatchRecords`); a request at most 400 items and 2 MB (`MockCloudDatabase.maxItemsPerRequest`/`maxBytesPerRequest`), else `limitExceeded` | Deviations 3 and 4. The old limit was "under 200", so tests with real sizes failed, and 2 MB was never checked. The byte size is estimated (strings, data), enough to trip the limit |
| A built batch leaves pending and stays in `state.inFlightRecordZoneChanges` until its result; `cancelOperations()` puts it back | Deviation 6. The old mock dropped saves when it built the batch, so a refused request lost them. A refused request now returns its changes to pending, as the real engine does |
| `.changedKeys` and `.allKeys` save policies; the default policy and `.changedKeys` keep unchanged keys from the stored record, `.allKeys` replaces it, neither of those two checks change tags | Deviation 10. They were `fatalError()`, and a save replaced the record (a `TODO`). The merge runs from the incoming record (keeping its `share`/`parent`) because `share` is not a changed key |
| Change tags count per zone, fetch tokens are per zone | Deviation 10. One global counter made cross-zone tag comparison work by accident |
| Zone failures: `database.state.isQuotaExceeded` (saves fail `quotaExceeded`, deletes work), `database.state.userDeletedZones` (`userDeletedZone`) | Deviation 9: no test could produce them |
| `nextRecordZoneChangeBatch` reads metadata in one query and rows in one query per table for the first 250 changes, and writes the last-known server records in one transaction (`SyncEngine.swift`, not mock) | Measured: the per-record reads and writes were about 1.4 s of a 6.3 s cycle. Records outside the prefetch fall back to per-record reads. Not timed since |

### Opt-in: behave like `CKSyncEngine` (`MockSyncEngine.state.isRealistic = true`)

| What | Why |
|---|---|
| `SyncEngine.runSendCycle(scope:)`: `willSendChanges`, batches until nothing is left, `didSendChanges`. `sendChanges()` runs it. A cycle ends at the first failed batch | Deviation 1: `isSendingChanges` never went true, `didSendChanges` never came. Ending at a failure is the measured case (a refused request fails everything); whether the real engine continues after a partial failure is **unverified** |
| Failed changes are re-queued after `didSendChanges` (the `f0e8fb5` deferral now also runs on the mock) | Deviation 2: the deferral was guarded by `syncEngine is CKSyncEngine`, so reverting it still passed. The regression test fails without it (checked) |
| `state.isSendScheduled` is set only by changes added outside a cycle; `runScheduledSend(scope:)` runs it | Deviations 2 and 12: the real engine does not send again for changes added inside a callback |
| Refusals: `database.failNextRequests(_:with:)`, `CKError.throttled(retryAfter:)`. Every record fails with that error and stays pending; the engine then waits until `advanceScheduler()` or a manual `sendChanges()` | Deviation 5: throttling, `SendOutcome.isThrottled` and "engine quiet" were untestable. The error set that fails a whole request is `isRequestRefusal` |
| `expireChangeToken(zoneID:)`; fetches in pages of 200 (`deliveredFetchPages`) with will/did fetch events | Deviation 7. Outside realistic mode a fetch is still one event |
| `SyncEngine.throttleClock` (default `ContinuousClock`) drives the wait of `resumesSendingAfterThrottle` | The option waited at least 5 s of real time; a `TestClock` makes it testable |
| `state.stateBytesPerPendingChange` (for example 375, the device value) posts `stateUpdate` with a stand-in serialization (JSON around a property list, sized by the pending changes) | Deviation 8. `CKSyncEngine.State.Serialization` cannot be built by us, so this is not CloudKit's archive: it exercises `handleStateUpdate`, the stored size and `SyncEngine.restoredState` with its 16 MB drop, not CloudKit's decoding |

### Opt-in: cost and throttle profile (`database.profile.setValue(.measuredDevelopment)`)

Purpose: run a workload through the mock and compare configurations (or against device measurements) without a device. Time is **simulated**: it advances only through requests and `advanceScheduler()`, never by sleeping, so runs stay fast and repeatable. Read it from `database.simulatedSeconds`.

| Setting | Value | Source |
|---|---|---|
| Time per record | 8.4 ms (2.10 s per 250) | measured, raw `modifyRecords` |
| Record with `parent` (`tables:`, not `privateTables:`) | x2.01 (4.22 s vs 2.10 s per 250) | measured; the profile picks it up from `record.parent`, so registering a table as private or shared changes the result by itself |
| Throttle | token bucket, burst 1,000 records, 20 records/s, retry-after at least 11 s | measured: trips at about 750-1,750 records within 20-40 s, passes about 1,200 records/min, retry-after 11-76 s |
| Refused request | 0.4 s | measured 0.3-0.5 s |
| Scheduler wait after a refusal | 60 s default (`schedulerWaitSeconds`) | observed 32 s to 12+ min, cause unknown, so a knob |
| `environmentMultiplier` | 1.0 | Production vs Development is **not measured** |

Other registration and engine settings were checked and have no measurement, so they are not modeled: record size (3 KB did not change batch time), assets, the shared database, several zones, `atomicByZone`. Add a field to `Profile` when you measure one.

### Opt-in: fuzzing unexpected iCloud behavior (`database.setFuzz(.init(seed:intensity:faults:))`)

Why: real accounts and networks fail in ways nobody scripts (signed out mid-sync, restricted, rate limits). The fuzzer rolls per request, and per send cycle for the account status, and injects a fault with probability `intensity` (0 never, 1 always; the caller chooses, default is off).

- `faults`: `.refusals` (6, 7, 23, 3, 4), `.accountErrors` (9, 36), `.accountStatus` (the account flips to `noAccount`, `restricted`, `temporarilyUnavailable` or `couldNotDetermine` when a cycle starts; the cycle sends nothing and waits; `advanceScheduler()` brings the account back).
- Reproducible: SplitMix64 from `seed`; `fuzzLog` lists what was injected. A failing run replays with the same seed and the same requests.
- Test: for seeds 1-3 at intensity 0.6, 300 changes still all reach the server and pending ends empty (nothing lost).
- Not fuzzed: sign-out and switch-account events (without a delegate they delete local data, so they need their own scenario), per-record errors (an `unknownItem` on a record the server has would re-save without a change tag and fail in the mock in a way the real server would not).

### Not done

- Per-field server merge beyond changed keys: there is no further behavior to model that the two policies above do not already cover.
- `OrderedSet` dedup: whether the real state dedups pending changes stays **[assumed]**.
- Latency per request in real time, and a swift-dependencies `Clock` for the database (the simulated clock replaces it).

## Order of work and risk

1. Events + cycle (1) and batch/limit constants (2): mechanical, no behavior change for old tests when the realistic mode is off. Enables testing `isSendingChanges`, the deferral and the bound with real sizes.
2. Scheduling rule (3) with the regression test for `f0e8fb5`: first confirm the test fails on stock behavior (revert the deferral locally), then keep it.
3. Error/throttle injection (4): enables the status API and resume option tests.
4. Paging/tokens (5), clock (6), small fixes (7).

Risks: the realistic mode changes the order of side effects, and many tests assert exact `assertInlineSnapshot` output of the mock database after a manual process call; keep those on the old mode and add new tests for the new one. Do not copy the real engine's timing, only its **ordering and limits**: tests stay deterministic.

## Test list the improved mock should make possible

- failed batch, changes re-queued in the callback: engine does not send again (old behavior, documents the trap); re-queued after `didSendChanges`: it does (current fix).
- 250-record batches, 1,000-change bound, top-up after each `sentRecordZoneChanges`, with real sizes.
- `lastSendOutcome`: saved, failed, error codes, `retryAfterSeconds`, `isThrottled`; `pendingChangeCount()` across engine state, buffer and overflow table.
- throttle: all records fail with code 6 + retry-after, `didSendChanges`, engine silent until the scheduler advance; with `resumesSendingAfterThrottle` it sends again after the retry-after; a second refusal reschedules.
- oversized saved state (over 16 MB) dropped at start and the queue rebuilt from metadata, using real `stateUpdate` serialization.
- `limitExceeded` for 401 items and for 2 MB, and a paged fetch of 450 records.

## Evidence for the real behavior

The timing log, error codes and measurements behind the "Real" column are in [CloudKitSyncInternals.md](CloudKitSyncInternals.md), section 11 and sections 5-7. Nothing here was verified against Apple source; where marked **[assumed]** it needs a device check.
