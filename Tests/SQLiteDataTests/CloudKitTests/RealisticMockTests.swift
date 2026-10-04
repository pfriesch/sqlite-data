#if canImport(CloudKit)
  import CloudKit
  import SQLiteData
  import SQLiteDataTestSupport
  import Testing

  extension BaseCloudKitTests {
    @MainActor
    final class RealisticMockTests: BaseCloudKitTests, @unchecked Sendable {
      private func seed(_ count: Int) async throws {
        try await userDatabase.userWrite { db in
          try db.seed {
            RemindersList(id: 1, title: "Personal")
            for id in 1...count { Reminder(id: id, title: "R\(id)", remindersListID: 1) }
          }
        }
        try await Task.sleep(for: .seconds(1))
      }

      private var serverRecordCount: Int {
        syncEngine.private.database.state.withValue {
          $0.storage.values.reduce(0) { $0 + $1.records.count }
        }
      }

      @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
      @Test func aBatchHolds250RecordsAndACycleSendsAll() async throws {
        syncEngine.maxInMemoryPendingChanges.setValue(1_000)
        try await seed(599)  // 600 changes with the list

        try await syncEngine.processPendingRecordZoneChanges(scope: .private)
        #expect(serverRecordCount == 250)

        syncEngine.private.state.isRealistic.setValue(true)
        try await syncEngine.private.sendChanges(CKSyncEngine.SendChangesOptions())
        #expect(serverRecordCount == 600)
        #expect(!syncEngine.isSendingChanges)
        #expect(syncEngine.lastSendOutcome?.failedCount == 0)
      }

      @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
      @Test func requestsOver400ItemsOr2MBAreRefused() async throws {
        let database = syncEngine.private.database
        let zoneID = try #require(database.state.withValue { $0.storage.keys.first })
        let records = (0..<401).map { CKRecord(recordType: "T", recordID: .init(recordName: "\($0)", zoneID: zoneID)) }
        #expect(throws: CKError.self) { try database.modifyRecords(saving: records) }

        let big = CKRecord(recordType: "T", recordID: .init(recordName: "big", zoneID: zoneID))
        big["blob"] = Data(count: 2_000_001)
        #expect(throws: CKError.self) { try database.modifyRecords(saving: [big]) }
      }

      @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
      @Test func aThrottledCycleFailsEveryRecordAndTheEngineWaitsForTheScheduler() async throws {
        syncEngine.private.state.isRealistic.setValue(true)
        try await seed(3)
        let state = syncEngine.private.state
        #expect(state.isSendScheduled.value)

        syncEngine.private.database.failNextRequests(with: .throttled(retryAfter: 25))
        try await syncEngine.runSendCycle(scope: .private)

        let outcome = try #require(syncEngine.lastSendOutcome)
        #expect(outcome.isThrottled)
        #expect(outcome.failedCount == 4)
        #expect(outcome.retryAfterSeconds == 25)
        #expect(serverRecordCount == 0)

        // Failed changes come back after `didSendChanges`, but nothing sends until the scheduler fires.
        try await Task.sleep(for: .milliseconds(300))
        #expect(state.pendingRecordZoneChanges.count == 4)
        #expect(state.isSchedulerWaiting.value)
        #expect(try await syncEngine.runScheduledSend(scope: .private) == false)

        syncEngine.private.advanceScheduler()
        #expect(try await syncEngine.runScheduledSend(scope: .private))
        #expect(serverRecordCount == 4)
      }

      /// Re-queueing inside `sentRecordZoneChanges` leaves the real engine idle; after
      /// `didSendChanges` it sends again (`f0e8fb5`). Reverting the deferral fails this test.
      @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
      @Test func failedChangesAreRequeuedAfterTheCycleSoTheEngineSendsAgain() async throws {
        syncEngine.private.state.isRealistic.setValue(true)
        try await seed(3)
        let database = syncEngine.private.database
        let zoneID = try #require(database.state.withValue { $0.storage.keys.first })
        _ = try database.modifyRecordZones(deleting: [zoneID])  // records now fail: zoneNotFound

        try await syncEngine.runSendCycle(scope: .private)
        try await Task.sleep(for: .milliseconds(300))

        let state = syncEngine.private.state
        #expect(state.pendingRecordZoneChanges.count == 4)
        #expect(state.isSendScheduled.value)

        try await syncEngine.processPendingDatabaseChanges(scope: .private)
        #expect(try await syncEngine.runScheduledSend(scope: .private))
        #expect(serverRecordCount == 4)
      }
    }
  }
#endif
