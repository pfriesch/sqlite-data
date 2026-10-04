#if canImport(CloudKit)
  import CloudKit
  import ConcurrencyExtras
  import ConcurrencyExtrasTestSupport
  import CustomDump
  import InlineSnapshotTesting
  import SQLiteData
  import SnapshotTestingCustomDump
  import SQLiteDataTestSupport
  import Testing

  extension BaseCloudKitTests {
    // A row deleted and inserted again with the same primary key before the next send
    // (upstream pointfreeco/sqlite-data#421).
    @MainActor
    final class ReinsertTests: BaseCloudKitTests, @unchecked Sendable {
      private var serverTitle: String? {
        syncEngine.private.database.state.withValue {
          $0.storage.values
            .compactMap { $0.records[RemindersList.recordID(for: 1)] }
            .first?.encryptedValues["title"] as? String
        }
      }

      private func listMetadata(_ id: RemindersList.ID) async throws -> SyncMetadata? {
        try await syncEngine.metadatabase.read {
          try SyncMetadata.find(RemindersList.recordID(for: id)).fetchOne($0)
        }
      }

      /// The stored server record matches what the server holds.
      private func expectMetadataServerRecord(
        _ metadata: SyncMetadata,
        matchesContainerRecord recordID: CKRecord.ID
      ) throws {
        let containerRecord = try container.privateCloudDatabase.record(for: recordID)
        var containerRecordDump = ""
        var metadataServerRecordDump = ""
        customDump(containerRecord, to: &containerRecordDump)
        customDump(metadata._lastKnownServerRecordAllFields, to: &metadataServerRecordDump)
        expectNoDifference(metadataServerRecordDump, containerRecordDump)
      }

      // * A synced list is deleted, then inserted again with the same id.
      // => The new row stays local and replaces the record on the server.
      @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
      @Test func deleteThenReinsertSyncedRow() async throws {
        try await userDatabase.userWrite { db in
          try db.seed { RemindersList(id: 1, title: "Personal") }
        }
        try await syncEngine.processPendingRecordZoneChanges(scope: .private)
        #expect(serverTitle == "Personal")

        try await withDependencies {
          $0.currentTime.now += 1
        } operation: {
          try await userDatabase.userWrite { db in
            try RemindersList.find(1).delete().execute(db)
            try db.seed { RemindersList(id: 1, title: "Work") }
          }
        }
        try await syncEngine.processPendingRecordZoneChanges(scope: .private)

        try await userDatabase.read { db in
          try #expect(RemindersList.all.fetchAll(db) == [RemindersList(id: 1, title: "Work")])
        }
        #expect(serverTitle == "Work")
      }

      // * The same, in two separate writes.
      @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
      @Test func deleteThenReinsertSyncedRowInSeparateWrites() async throws {
        try await userDatabase.userWrite { db in
          try db.seed { RemindersList(id: 1, title: "Personal") }
        }
        try await syncEngine.processPendingRecordZoneChanges(scope: .private)

        try await withDependencies {
          $0.currentTime.now += 1
        } operation: {
          try await userDatabase.userWrite { db in
            try RemindersList.find(1).delete().execute(db)
          }
        }
        try await withDependencies {
          $0.currentTime.now += 2
        } operation: {
          try await userDatabase.userWrite { db in
            try db.seed { RemindersList(id: 1, title: "Work") }
          }
        }
        try await syncEngine.processPendingRecordZoneChanges(scope: .private)

        try await userDatabase.read { db in
          try #expect(RemindersList.all.fetchAll(db) == [RemindersList(id: 1, title: "Work")])
        }
        #expect(serverTitle == "Work")
      }

      // * A list that never reached the server is deleted, then inserted again.
      // => The new row uploads.
      @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
      @Test func deleteThenReinsertUnsyncedRow() async throws {
        try await userDatabase.userWrite { db in
          try db.seed { RemindersList(id: 1, title: "Personal") }
        }
        try await withDependencies {
          $0.currentTime.now += 1
        } operation: {
          try await userDatabase.userWrite { db in
            try RemindersList.find(1).delete().execute(db)
            try db.seed { RemindersList(id: 1, title: "Work") }
          }
        }
        try await syncEngine.processPendingRecordZoneChanges(scope: .private)

        try await userDatabase.read { db in
          try #expect(RemindersList.all.fetchAll(db) == [RemindersList(id: 1, title: "Work")])
        }
        #expect(serverTitle == "Work")
      }

      // * A synced list is deleted and inserted again; the engine's queue is then lost (saved state
      //   over 16 MB is dropped) and rebuilt from metadata.
      // => The rebuilt queue saves the re-inserted row.
      @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
      @Test func reinsertedRowSurvivesQueueRebuild() async throws {
        try await userDatabase.userWrite { db in
          try db.seed { RemindersList(id: 1, title: "Personal") }
        }
        try await syncEngine.processPendingRecordZoneChanges(scope: .private)

        try await withDependencies {
          $0.currentTime.now += 1
        } operation: {
          try await userDatabase.userWrite { db in
            try RemindersList.find(1).delete().execute(db)
            try db.seed { RemindersList(id: 1, title: "Work") }
          }
        }
        syncEngine.private.state.remove(
          pendingRecordZoneChanges: syncEngine.private.state.pendingRecordZoneChanges
        )
        syncEngine.stop()
        syncEngine.needsPendingRebuild.setValue(true)
        try await syncEngine.start()
        try await syncEngine.processPendingDatabaseChanges(scope: .private)
        #expect(
          syncEngine.private.state.pendingRecordZoneChanges
            == [.saveRecord(RemindersList.recordID(for: 1))]
        )

        try await syncEngine.processPendingRecordZoneChanges(scope: .private)
        #expect(serverTitle == "Work")
      }

      // The cases below are from upstream #421's ChangeSupersessionTests, unchanged.

      @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
      @Test func deleteThenReinsertThenDelete_deletes() async throws {
        try await userDatabase.userWrite { db in
          try db.seed { RemindersList(id: 1, title: "Original") }
        }
        try await syncEngine.processPendingRecordZoneChanges(scope: .private)

        try await userDatabase.userWrite { db in
          try RemindersList.find(1).delete().execute(db)
          try RemindersList.insert { RemindersList(id: 1, title: "Reinserted") }.execute(db)
          try RemindersList.find(1).delete().execute(db)
        }
        
        #expect(try #require(await listMetadata(1))._pendingStatus == .deleted)
        
        try await syncEngine.processPendingRecordZoneChanges(scope: .private)
        
        #expect(try await listMetadata(1) == nil)

        assertInlineSnapshot(of: container, as: .customDump) {
          """
          MockCloudContainer(
            privateCloudDatabase: MockCloudDatabase(
              databaseScope: .private,
              storage: []
            ),
            sharedCloudDatabase: MockCloudDatabase(
              databaseScope: .shared,
              storage: []
            )
          )
          """
        }
      }

      @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
      @Test(.taskLocal(CKRecord._$printTimestamps, true))
      func deleteThenReinsertWithSameValue_savesWithUpdatedTimestamps()
        async throws
      {
        try await userDatabase.userWrite { db in
          try db.seed { RemindersList(id: 1, title: "Original") }
        }
        try await syncEngine.processPendingRecordZoneChanges(scope: .private)

        try await withDependencies {
          $0.currentTime.now += 1
        } operation: {
          try await userDatabase.userWrite { db in
            try RemindersList.find(1).delete().execute(db)
            try RemindersList.insert { RemindersList(id: 1, title: "Original") }.execute(db)
          }
          
          #expect(try #require(await listMetadata(1))._pendingStatus == .reinserted)

          try await syncEngine.processPendingRecordZoneChanges(scope: .private)
        }
        
        let metadata = try #require(await listMetadata(1))
        #expect(metadata._pendingStatus == nil)

        try expectMetadataServerRecord(metadata, matchesContainerRecord: RemindersList.recordID(for: 1))

        assertInlineSnapshot(of: container, as: .customDump) {
          """
          MockCloudContainer(
            privateCloudDatabase: MockCloudDatabase(
              databaseScope: .private,
              storage: [
                [0]: CKRecord(
                  recordID: CKRecord.ID(1:remindersLists/zone/__defaultOwner__),
                  recordType: "remindersLists",
                  parent: nil,
                  share: nil,
                  id: 1,
                  id🗓️: 1,
                  title: "Original",
                  title🗓️: 1,
                  🗓️: 1
                )
              ]
            ),
            sharedCloudDatabase: MockCloudDatabase(
              databaseScope: .shared,
              storage: []
            )
          )
          """
        }
      }

      @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
      @Test(.taskLocal(CKRecord._$printTimestamps, true))
      func reinsertedRecord_staleServerUpdate_localWins() async throws {
        try await userDatabase.userWrite { db in
          try db.seed { RemindersList(id: 1, title: "Original") }
        }
        try await syncEngine.processPendingRecordZoneChanges(scope: .private)

        try await withDependencies {
          $0.currentTime.now += 1
        } operation: {
          try await userDatabase.userWrite { db in
            try RemindersList.find(1).delete().execute(db)
            try RemindersList.insert { RemindersList(id: 1, title: "Reinserted") }.execute(db)
          }
          
          #expect(try #require(await listMetadata(1))._pendingStatus == .reinserted)

          let record = try syncEngine.private.database.record(for: RemindersList.recordID(for: 1))
          record.setValue("Server", forKey: "title", at: 0)
          try await syncEngine.modifyRecords(scope: .private, saving: [record]).notify()
          
          let metadata = try #require(await listMetadata(1))
          #expect(metadata._pendingStatus == nil)
          #expect(metadata.userModificationTime == 1)

          let row = try await userDatabase.read { db in
            try RemindersList.find(1).fetchOne(db)
          }
          #expect(row?.title == "Reinserted")

          try await syncEngine.processPendingRecordZoneChanges(scope: .private)
        }
        
        let metadata = try #require(await listMetadata(1))
        #expect(metadata._pendingStatus == nil)
        #expect(metadata.userModificationTime == 1)

        try expectMetadataServerRecord(metadata, matchesContainerRecord: RemindersList.recordID(for: 1))

        assertInlineSnapshot(of: container, as: .customDump) {
          """
          MockCloudContainer(
            privateCloudDatabase: MockCloudDatabase(
              databaseScope: .private,
              storage: [
                [0]: CKRecord(
                  recordID: CKRecord.ID(1:remindersLists/zone/__defaultOwner__),
                  recordType: "remindersLists",
                  parent: nil,
                  share: nil,
                  id: 1,
                  id🗓️: 0,
                  title: "Reinserted",
                  title🗓️: 1,
                  🗓️: 1
                )
              ]
            ),
            sharedCloudDatabase: MockCloudDatabase(
              databaseScope: .shared,
              storage: []
            )
          )
          """
        }
      }

      @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
      @Test(.taskLocal(CKRecord._$printTimestamps, true))
      func reinsertedRecord_freshServerUpdate_serverWins() async throws {
        try await userDatabase.userWrite { db in
          try db.seed { RemindersList(id: 1, title: "Original") }
        }
        try await syncEngine.processPendingRecordZoneChanges(scope: .private)

        try await withDependencies {
          $0.currentTime.now += 1
        } operation: {
          try await userDatabase.userWrite { db in
            try RemindersList.find(1).delete().execute(db)
            try RemindersList.insert { RemindersList(id: 1, title: "Reinserted") }.execute(db)
          }
          
          #expect(try #require(await listMetadata(1))._pendingStatus == .reinserted)

          let record = try syncEngine.private.database.record(for: RemindersList.recordID(for: 1))
          record.setValue("Server", forKey: "title", at: 2)
          try await syncEngine.modifyRecords(scope: .private, saving: [record]).notify()
          
          #expect(try #require(await listMetadata(1))._pendingStatus == nil)

          let row = try await userDatabase.read { db in
            try RemindersList.find(1).fetchOne(db)
          }
          #expect(row?.title == "Server")

          try await syncEngine.processPendingRecordZoneChanges(scope: .private)
        }
        
        let metadata = try #require(await listMetadata(1))
        #expect(metadata._pendingStatus == nil)

        try expectMetadataServerRecord(metadata, matchesContainerRecord: RemindersList.recordID(for: 1))

        assertInlineSnapshot(of: container, as: .customDump) {
          """
          MockCloudContainer(
            privateCloudDatabase: MockCloudDatabase(
              databaseScope: .private,
              storage: [
                [0]: CKRecord(
                  recordID: CKRecord.ID(1:remindersLists/zone/__defaultOwner__),
                  recordType: "remindersLists",
                  parent: nil,
                  share: nil,
                  id: 1,
                  id🗓️: 0,
                  title: "Server",
                  title🗓️: 2,
                  🗓️: 2
                )
              ]
            ),
            sharedCloudDatabase: MockCloudDatabase(
              databaseScope: .shared,
              storage: []
            )
          )
          """
        }
      }
    }
  }
#endif
