# CLAUDE.md

Fork of `pointfreeco/sqlite-data` (`pfriesch/sqlite-data`), used by an iOS app for CloudKit sync.

- **No pull requests to pointfreeco/sqlite-data.** Changes live only in this fork.
- Tests: `swift test` (333 pass). `MockSyncEngine` and `MockCloudDatabase` are faithful to CloudKit only where `Docs/MockFidelity.md` says so; the realistic behavior (send cycle, scheduling rule, throttle and cost profile, `stateUpdate`, fuzzing) is opt-in per test. Behavior that depends on real timing (deferred re-queue, throttle resume, scheduler wait length) can only be verified on a real device.
- Before touching anything under `Sources/SQLiteData/CloudKit`, load the `cloudkit-sync` skill (`.claude/skills/cloudkit-sync/SKILL.md`). It records what CKSyncEngine really does, what this fork changed and why, and the traps.

## Fork changes (do not undo without reading the skill)

| Commit | Change |
|---|---|
| `bounded-pending-changes` branch | At most 1,000 changes in `CKSyncEngine` state; overflow goes to `sqlitedata_icloud_pendingRecordZoneChanges`; top-up after each sent batch; saved state over 16 MB is dropped and the queue rebuilt from metadata |
| `f0e8fb5` | Failed changes are re-queued after `didSendChanges`, from a detached task, not inside `sentRecordZoneChanges` |
| `a2333e7` | Public `lastSendOutcome` and `pendingChangeCount()` for status UI |
| `5726285` | `nextRecordZoneChangeBatch` prefetches metadata and rows for the first 250 changes and writes last-known server records once per batch (was per record); not timed on a device |
| "Queue whole tables in short write transactions" | `touchRows`: queueing a new table (start) or unknown records (sign-in) runs in 1,000-row write transactions instead of one; the single transaction stalled the app behind the writer |
| `613a056` | Opt-in `resumesSendingAfterThrottle` (default off; measured harmful) |

A watchdog that restarted the engine when idle (`3f5cb14`) was reverted (`7fb3fa6`). Do not bring it back: it hid the cause.

## Rules

- Never debug-log or commit temporary probes (timing logs, `dbg()` file writers, `SQLITEDATA_FORCE_CONFLICTS` harness). Keep them as local patches outside the repo.
- Never await a `CKSyncEngine` call from a Task created inside a delegate callback; use `Task.detached`.
- Do not reorder the metadata table schema on current evidence (see skill, "Open questions").

## Docs

- `Docs/CloudKitSyncInternals.md`: how CKSyncEngine and SQLiteData sync work, what this fork changed, error codes, measurements, how to investigate. Read before touching `Sources/SQLiteData/CloudKit`.
- `Docs/MockFidelity.md`: what the mocks do now and why (limits, in-flight, realistic send cycle, cost and throttle profile, `stateUpdate`, seeded fuzzing), what is still unmodeled, and the original gap list. Read before writing sync tests or changing the mocks.
- `Tests/SQLiteDataTests/CloudKitTests/RealisticMockTests.swift`: working examples of every opt-in mock feature.
