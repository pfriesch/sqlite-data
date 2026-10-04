#if canImport(CloudKit)
  import Clocks
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

      // MARK: Cost and throttle profile

      @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
      @Test func recordsWithAParentCostTwiceAsMuchAsPrivateOnes() async throws {
        let database = syncEngine.private.database
        database.profile.setValue(
          { var profile = MockCloudDatabase.Profile(); profile.throttle = nil; return profile }()
        )
        syncEngine.maxInMemoryPendingChanges.setValue(1_000)

        // 1 list + 100 reminders (`tables:`, so each has a parent).
        try await seed(100)
        try await syncEngine.processPendingRecordZoneChanges(scope: .private)
        let withParent = database.simulatedSeconds.value / 101

        // 100 lists: no parent.
        let before = database.simulatedSeconds.value
        try await userDatabase.userWrite { db in
          try db.seed { for id in 2...101 { RemindersList(id: id, title: "L\(id)") } }
        }
        try await Task.sleep(for: .seconds(1))
        try await syncEngine.processPendingRecordZoneChanges(scope: .private)
        let withoutParent = (database.simulatedSeconds.value - before) / 100

        #expect(withParent / withoutParent > 1.9 && withParent / withoutParent < 2.1)
      }

      @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
      @Test func aFastUploadTripsTheThrottleAndTheSchedulerRefillsIt() async throws {
        syncEngine.private.state.isRealistic.setValue(true)
        syncEngine.private.database.profile.setValue(.measured)
        syncEngine.maxInMemoryPendingChanges.setValue(2_000)
        try await seed(1_499)

        try await syncEngine.runSendCycle(scope: .private)
        let outcome = try #require(syncEngine.lastSendOutcome)
        #expect(outcome.isThrottled)
        #expect((outcome.retryAfterSeconds ?? 0) >= 11)
        #expect(serverRecordCount >= 1_000 && serverRecordCount < 1_500)

        for _ in 0..<10 where !syncEngine.private.state.pendingRecordZoneChanges.isEmpty {
          try await Task.sleep(for: .milliseconds(300))
          syncEngine.private.advanceScheduler()
          try await syncEngine.runScheduledSend(scope: .private)
        }
        #expect(serverRecordCount == 1_500)
      }

      @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
      @Test func afterAServerRefusalTheDeviceRefusesLocallyWithCode7UntilTheRetryAfter() async throws {
        let database = syncEngine.private.database
        let zoneID = try #require(database.state.withValue { $0.storage.keys.first })
        database.profile.setValue(.measured)
        func records(_ prefix: String) -> [CKRecord] {
          (0..<400).map { CKRecord(recordType: "T", recordID: .init(recordName: "\(prefix)\($0)", zoneID: zoneID)) }
        }
        func refusal(_ prefix: String) -> CKError? {
          do { _ = try database.modifyRecords(saving: records(prefix)); return nil }
          catch { return error as? CKError }
        }
        #expect(refusal("a") == nil)
        #expect(refusal("b") == nil)
        let server = try #require(refusal("c"))  // burst of 1,000 is used up
        #expect(server.code == .serviceUnavailable)
        let retryAfter = try #require(server.retryAfterSeconds)

        let local = try #require(refusal("d"))
        #expect(local.code == .requestRateLimited)
        #expect((local.retryAfterSeconds ?? .infinity) <= retryAfter)

        database.advanceSimulatedTime(by: retryAfter)
        #expect(refusal("e") == nil)
      }

      // MARK: resumesSendingAfterThrottle

      @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
      @Test func resumingAfterAThrottleSendsAgainWhenTheClockPassesTheRetryAfter() async throws {
        let clock = TestClock<Duration>()
        syncEngine.throttleClock.setValue(clock)
        syncEngine.resumesSendingAfterThrottle = true
        syncEngine.private.state.isRealistic.setValue(true)
        try await seed(3)

        syncEngine.private.database.failNextRequests(with: .throttled(retryAfter: 25))
        try await syncEngine.runSendCycle(scope: .private)
        #expect(serverRecordCount == 0)

        await clock.advance(by: .seconds(10))
        try await Task.sleep(for: .milliseconds(300))
        #expect(serverRecordCount == 0)  // still waiting for the retry-after

        await clock.advance(by: .seconds(30))  // past retry-after plus jitter
        try await Task.sleep(for: .milliseconds(500))
        #expect(serverRecordCount == 4)
      }

      // MARK: stateUpdate

      @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
      @Test func aSmallSavedStateIsRestored() async throws {
        let state = syncEngine.private.state
        state.isRealistic.setValue(true)
        state.stateBytesPerPendingChange.setValue(375)
        try await seed(3)
        syncEngine.private.database.failNextRequests(with: .throttled())
        try await syncEngine.runSendCycle(scope: .private)

        let restored = SyncEngine.restoredState(
          isPrivate: true, metadatabase: syncEngine.metadatabase, syncEngine: syncEngine
        )
        #expect(restored != nil)
        #expect(!syncEngine.needsPendingRebuild.value)

        // The mock engine must end empty: drain it.
        try await syncEngine.private.sendChanges(CKSyncEngine.SendChangesOptions())
      }

      @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
      @Test func anOversizedSavedStateIsDroppedAndTheQueueRebuilt() async throws {
        let state = syncEngine.private.state
        state.isRealistic.setValue(true)
        state.stateBytesPerPendingChange.setValue(100_000)  // 200 changes: about 20 MB
        syncEngine.maxInMemoryPendingChanges.setValue(1_000)
        try await seed(199)
        syncEngine.private.database.failNextRequests(with: .throttled())
        try await syncEngine.runSendCycle(scope: .private)

        let restored = SyncEngine.restoredState(
          isPrivate: true, metadatabase: syncEngine.metadatabase, syncEngine: syncEngine
        )
        #expect(restored == nil)
        #expect(syncEngine.needsPendingRebuild.value)

        // The mock engine must end empty: drain it.
        try await syncEngine.private.sendChanges(CKSyncEngine.SendChangesOptions())
      }

      // MARK: Save policies, change tags, paged fetch

      @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
      @Test func savePoliciesMergeOrReplaceAndSkipTheTagCheck() async throws {
        let database = syncEngine.private.database
        let zoneID = try #require(database.state.withValue { $0.storage.keys.first })
        let recordID = CKRecord.ID(recordName: "x", zoneID: zoneID)
        let original = CKRecord(recordType: "T", recordID: recordID)
        original["a"] = 1
        original["b"] = 2
        _ = try database.modifyRecords(saving: [original])

        // '.changedKeys': only 'a' is sent, 'b' keeps its value.
        let edited = try database.record(for: recordID)
        edited["a"] = 3
        _ = try database.modifyRecords(saving: [edited], savePolicy: .changedKeys)
        var stored = try database.record(for: recordID)
        #expect(stored["a"] as? Int == 3 && stored["b"] as? Int == 2)

        // '.ifServerRecordUnchanged' merges too.
        let edited2 = try database.record(for: recordID)
        edited2["b"] = 5
        _ = try database.modifyRecords(saving: [edited2])
        stored = try database.record(for: recordID)
        #expect(stored["a"] as? Int == 3 && stored["b"] as? Int == 5)

        // '.allKeys': no tag needed, the record is replaced.
        let replacement = CKRecord(recordType: "T", recordID: recordID)
        replacement["a"] = 9
        _ = try database.modifyRecords(saving: [replacement], savePolicy: .allKeys)
        stored = try database.record(for: recordID)
        #expect(stored["a"] as? Int == 9 && stored["b"] == nil)

        // The same stale save fails with the default policy.
        let again = CKRecord(recordType: "T", recordID: recordID)
        #expect(try database.modifyRecords(saving: [again]).saveResults[recordID].map { (try? $0.get()) == nil } == true)
      }

      @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
      @Test func changeTagsCountPerZone() async throws {
        let database = syncEngine.private.database
        let otherZone = CKRecordZone(zoneName: "other")
        _ = try database.modifyRecordZones(saving: [otherZone])
        let zoneID = try #require(database.state.withValue { $0.storage.keys.first { $0 != otherZone.zoneID } })
        let one = CKRecord(recordType: "T", recordID: .init(recordName: "1", zoneID: zoneID))
        let two = CKRecord(recordType: "T", recordID: .init(recordName: "2", zoneID: otherZone.zoneID))
        let results = try database.modifyRecords(saving: [one, two])
        #expect(try results.saveResults[one.recordID]?.get()._recordChangeTag == 1)
        #expect(try results.saveResults[two.recordID]?.get()._recordChangeTag == 1)
        _ = try database.modifyRecords(deleting: [one.recordID, two.recordID])
        try await syncEngine.private.fetchChanges(CKSyncEngine.FetchChangesOptions())
      }

      @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
      @Test func fetchesArePagedAndAnExpiredTokenForcesAFullFetch() async throws {
        let engine = syncEngine.private
        engine.state.isRealistic.setValue(true)
        syncEngine.maxInMemoryPendingChanges.setValue(1_000)
        try await seed(449)  // 450 records
        try await engine.sendChanges(CKSyncEngine.SendChangesOptions())
        #expect(serverRecordCount == 450)

        try await engine.fetchChanges(CKSyncEngine.FetchChangesOptions())
        #expect(engine.state.deliveredFetchPages.value == [200, 200, 50])

        // The token advanced: nothing new.
        engine.state.deliveredFetchPages.setValue([])
        try await engine.fetchChanges(CKSyncEngine.FetchChangesOptions())
        #expect(engine.state.deliveredFetchPages.value == [])

        let zoneID = try #require(engine.database.state.withValue { $0.storage.keys.first })
        engine.expireChangeToken(zoneID: zoneID)
        try await engine.fetchChanges(CKSyncEngine.FetchChangesOptions())
        #expect(engine.state.deliveredFetchPages.value == [200, 200, 50])
        try await Task.sleep(for: .milliseconds(500))
        try await engine.sendChanges(CKSyncEngine.SendChangesOptions())
      }

      // MARK: In flight, zone errors, fuzzing

      @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
      @Test func aBatchStaysInFlightUntilItsResultAndCancellingRequeuesIt() async throws {
        let engine = syncEngine.private
        try await seed(2)  // 3 changes
        let batch = await syncEngine.nextRecordZoneChangeBatch(syncEngine: engine)
        #expect(batch?.recordsToSave.count == 3)
        #expect(engine.state.pendingRecordZoneChanges.isEmpty)
        #expect(engine.state.inFlightRecordZoneChanges.count == 3)

        await engine.cancelOperations()
        #expect(engine.state.pendingRecordZoneChanges.count == 3)
        #expect(engine.state.inFlightRecordZoneChanges.isEmpty)

        try await syncEngine.processPendingRecordZoneChanges(scope: .private)
        #expect(engine.state.inFlightRecordZoneChanges.isEmpty)
        #expect(serverRecordCount == 3)
      }

      @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
      @Test func quotaExceededAndUserDeletedZoneFailSaves() async throws {
        let database = syncEngine.private.database
        let zoneID = try #require(database.state.withValue { $0.storage.keys.first })
        syncEngine.private.state.isRealistic.setValue(true)
        try await seed(2)

        database.state.withValue { $0.isQuotaExceeded = true }
        try await syncEngine.runSendCycle(scope: .private)
        #expect(syncEngine.lastSendOutcome?.errorCodes[CKError.Code.quotaExceeded.rawValue] == 3)
        #expect(syncEngine.lastSendOutcome?.isQuotaExceeded == true)

        database.state.withValue { $0.isQuotaExceeded = false; $0.userDeletedZones = [zoneID] }
        syncEngine.private.state.add(pendingRecordZoneChanges: [.saveRecord(Reminder.recordID(for: 1))])
        try await syncEngine.runSendCycle(scope: .private)
        #expect(syncEngine.lastSendOutcome?.errorCodes[CKError.Code.userDeletedZone.rawValue] == 1)
        try await Task.sleep(for: .milliseconds(300))
        database.state.withValue { $0.userDeletedZones = [] }
        syncEngine.private.state.remove(
          pendingRecordZoneChanges: syncEngine.private.state.pendingRecordZoneChanges
        )
      }

      @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
      @Test func quotaExceededSavesAreRequeuedAfterADelayAndThenUpload() async throws {
        SyncEngine.setQuotaRetryDelay(.milliseconds(300))
        defer { SyncEngine.setQuotaRetryDelay(.seconds(300)) }
        let database = syncEngine.private.database
        syncEngine.private.state.isRealistic.setValue(true)
        try await seed(2)  // 3 changes

        database.state.withValue { $0.isQuotaExceeded = true }
        try await syncEngine.runSendCycle(scope: .private)
        #expect(serverRecordCount == 0)
        #expect(syncEngine.private.state.pendingRecordZoneChanges.isEmpty)  // the engine dropped them

        database.state.withValue { $0.isQuotaExceeded = false }
        try await Task.sleep(for: .seconds(1))
        #expect(syncEngine.private.state.pendingRecordZoneChanges.count == 3)  // we put them back
        try await syncEngine.runSendCycle(scope: .private)
        #expect(serverRecordCount == 3)
      }

      @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
      @Test func fuzzIsOffByDefaultAndReproducibleBySeed() async throws {
        let database = syncEngine.private.database
        #expect(database.fuzz.value == nil)

        func run(seed: UInt64) -> [String] {
          database.setFuzz(.init(seed: seed, intensity: 0.5))
          let zoneID = database.state.withValue { $0.storage.keys.first! }
          for index in 0..<40 {
            _ = try? database.modifyRecords(
              saving: [CKRecord(recordType: "T", recordID: .init(recordName: "\(seed)-\(index)", zoneID: zoneID))]
            )
          }
          return database.fuzzLog.value
        }
        let first = run(seed: 7)
        #expect(!first.isEmpty && first.count < 40)
        #expect(run(seed: 7) == first)
        #expect(run(seed: 8) != first)
        database.setFuzz(.init(intensity: 0))
        #expect(database.fuzzLog.value.isEmpty)
        _ = try database.modifyRecords(saving: [])
        #expect(database.fuzzLog.value.isEmpty)
        database.setFuzz(nil)
      }

      @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
      @Test(arguments: [UInt64(1), 2, 3])
      func everythingArrivesDespiteFuzzedFailures(fuzzSeed: UInt64) async throws {
        let engine = syncEngine.private
        engine.state.isRealistic.setValue(true)
        syncEngine.maxInMemoryPendingChanges.setValue(1_000)
        try await seed(299)  // 300 changes: two batches
        engine.database.setFuzz(.init(seed: fuzzSeed, intensity: 0.6))

        var attempts = 0
        while !engine.state.pendingRecordZoneChanges.isEmpty, attempts < 200 {
          attempts += 1
          engine.advanceScheduler()
          try await engine.sendChanges(CKSyncEngine.SendChangesOptions())
          try await Task.sleep(for: .milliseconds(150))  // deferred re-queues
        }
        #expect(!engine.database.fuzzLog.value.isEmpty)
        #expect(engine.state.pendingRecordZoneChanges.isEmpty)
        #expect(serverRecordCount == 300)

        engine.database.setFuzz(nil)
        engine.advanceScheduler()
      }
    }
  }
#endif
