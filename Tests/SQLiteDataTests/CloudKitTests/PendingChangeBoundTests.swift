#if canImport(CloudKit)
  import CloudKit
  import SQLiteData
  import SQLiteDataTestSupport
  import Testing

  extension BaseCloudKitTests {
    @MainActor
    final class PendingChangeBoundTests: BaseCloudKitTests, @unchecked Sendable {
      @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
      @Test func writesBeyondTheBoundWaitInTheTableAndDrainAsTheEngineSends() async throws {
        let bound = 100
        syncEngine.maxInMemoryPendingChanges.setValue(bound)
        let total = bound * 2 + 50

        try await userDatabase.userWrite { db in
          try db.seed {
            RemindersList(id: 1, title: "Personal")
            for id in 1...total {
              Reminder(id: id, title: "Reminder \(id)", remindersListID: 1)
            }
          }
        }
        try await Task.sleep(for: .seconds(2))

        // The engine holds at most the bound; the rest waits in the table.
        #expect(syncEngine.private.state.pendingRecordZoneChanges.count <= bound)
        let queued = try await syncEngine.metadatabase.read { db in
          try PendingRecordZoneChange.count().fetchOne(db) ?? 0
        }
        #expect(queued >= total + 1 - bound)

        // Sending frees room, which tops the engine up from the table, until everything is out.
        for _ in 0..<20 where !syncEngine.private.state.pendingRecordZoneChanges.isEmpty {
          try await syncEngine.processPendingRecordZoneChanges(scope: .private)
          try await Task.sleep(for: .milliseconds(300))
          #expect(syncEngine.private.state.pendingRecordZoneChanges.count <= bound)
        }

        let synced = try await syncEngine.metadatabase.read { db in
          try SyncMetadata.where { $0.hasLastKnownServerRecord }.count().fetchOne(db) ?? 0
        }
        let stillQueued = try await syncEngine.metadatabase.read { db in
          try PendingRecordZoneChange.count().fetchOne(db) ?? 0
        }
        #expect(synced == total + 1)
        #expect(stillQueued == 0)
      }

      @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
      @Test func anOversizedStateIsSpilledToTheTableOnStart() async throws {
        let bound = 100
        syncEngine.maxInMemoryPendingChanges.setValue(bound)
        let total = bound * 3
        syncEngine.stop()
        try await userDatabase.userWrite { db in
          try db.seed {
            RemindersList(id: 1, title: "Personal")
            for id in 1...total {
              Reminder(id: id, title: "Reminder \(id)", remindersListID: 1)
            }
          }
        }
        try await Task.sleep(for: .seconds(2))
        try await syncEngine.start()
        try await Task.sleep(for: .seconds(2))

        #expect(syncEngine.private.state.pendingRecordZoneChanges.count <= bound)
        let queued = try await syncEngine.metadatabase.read { db in
          try PendingRecordZoneChange.count().fetchOne(db) ?? 0
        }
        // Nothing was dropped: engine plus table hold every record.
        #expect(syncEngine.private.state.pendingRecordZoneChanges.count + queued >= total + 1)

        // Restarting queues the default zone, which must exist before records are sent.
        try await syncEngine.processPendingDatabaseChanges(scope: .private)
        // The mock engine must end empty: drain it.
        for _ in 0..<30 where !syncEngine.private.state.pendingRecordZoneChanges.isEmpty {
            try await syncEngine.processPendingRecordZoneChanges(scope: .private)
          try await Task.sleep(for: .milliseconds(300))
        }
      }
    }
  }
#endif
