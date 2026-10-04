# CLAUDE.md

Fork of `pointfreeco/sqlite-data` (`pfriesch/sqlite-data`), used by an iOS app for CloudKit sync.

- **No pull requests to pointfreeco/sqlite-data.** Changes live only in this fork.
- Tests: `swift test` (317 pass). The mock sync engine (`MockSyncEngine`, `MockCloudDatabase`) is not faithful to CloudKit: it never posts `willSendChanges`/`didSendChanges`, takes every pending change in one batch, and rejects 200+ records (real: 250 per engine batch, 400 per request). Behavior that depends on those events (deferred re-queue, throttle resume) can only be verified on a real device.
- Before touching anything under `Sources/SQLiteData/CloudKit`, load the `cloudkit-sync` skill (`.claude/skills/cloudkit-sync/SKILL.md`). It records what CKSyncEngine really does, what this fork changed and why, and the traps.

## Fork changes (do not undo without reading the skill)

| Commit | Change |
|---|---|
| `bounded-pending-changes` branch | At most 1,000 changes in `CKSyncEngine` state; overflow goes to `sqlitedata_icloud_pendingRecordZoneChanges`; top-up after each sent batch; saved state over 16 MB is dropped and the queue rebuilt from metadata |
| `f0e8fb5` | Failed changes are re-queued after `didSendChanges`, from a detached task, not inside `sentRecordZoneChanges` |
| `a2333e7` | Public `lastSendOutcome` and `pendingChangeCount()` for status UI |
| `613a056` | Opt-in `resumesSendingAfterThrottle` (default off; measured harmful) |

A watchdog that restarted the engine when idle (`3f5cb14`) was reverted (`7fb3fa6`). Do not bring it back: it hid the cause.

## Rules

- Never debug-log or commit temporary probes (timing logs, `dbg()` file writers, `SQLITEDATA_FORCE_CONFLICTS` harness). Keep them as local patches outside the repo.
- Never await a `CKSyncEngine` call from a Task created inside a delegate callback; use `Task.detached`.
- Do not reorder the metadata table schema on current evidence (see skill, "Open questions").

## Docs

- `Docs/CloudKitSyncInternals.md`: how CKSyncEngine and SQLiteData sync work, what this fork changed, error codes, measurements, how to investigate. Read before touching `Sources/SQLiteData/CloudKit`.
- `Docs/MockFidelity.md`: where `MockSyncEngine` / `MockCloudDatabase` differ from CloudKit and the plan to close the gaps. Read before writing sync tests or changing the mocks.

