#if canImport(CloudKit)
  package import ConcurrencyExtras
  package import CloudKit
  import IssueReporting
  package import OrderedCollections

  @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
  package final class MockSyncEngine: SyncEngineProtocol {
    package let database: MockCloudDatabase
    package let parentSyncEngine: SyncEngine
    package let state: MockSyncEngineState
    package let _fetchChangesScopes = LockIsolated<[CKSyncEngine.FetchChangesOptions.Scope]>([])
    package let _acceptedShareMetadata = LockIsolated<Set<ShareMetadata>>([])

    package init(
      database: MockCloudDatabase,
      parentSyncEngine: SyncEngine,
      state: MockSyncEngineState
    ) {
      self.database = database
      self.parentSyncEngine = parentSyncEngine
      self.state = state
    }

    package var scope: CKDatabase.Scope {
      database.databaseScope
    }

    package func acceptShare(metadata: ShareMetadata) {
      _ = _acceptedShareMetadata.withValue { $0.insert(metadata) }
    }

    package func fetchChanges(_ options: CKSyncEngine.FetchChangesOptions) async throws {
      let zoneIDs: [CKRecordZone.ID]
      switch options.scope {
      case .all:
        zoneIDs = Array(database.state.storage.keys)
      case .allExcluding(let excludedZoneIDs):
        zoneIDs = Array(Set(database.state.storage.keys).subtracting(excludedZoneIDs))
      case .zoneIDs(let includedZoneIDs):
        zoneIDs = includedZoneIDs
      @unknown default:
        fatalError()
      }

      typealias Page = (
        zoneID: CKRecordZone.ID,
        modifications: [CKRecord],
        deletions: [(recordID: CKRecord.ID, recordType: CKRecord.RecordType)]
      )
      var pages: [Page] = []
      var expiredZoneIDs: [CKRecordZone.ID] = []
      for zoneID in zoneIDs {
        if state.expiredZoneIDs.withValue({ $0.remove(zoneID) != nil }) {
          // NB: The server refuses the stale token; the engine drops it and fetches everything.
          expiredZoneIDs.append(zoneID)
          state.changeTags.withValue { $0[zoneID] = nil }
        }
        let token = state.changeTags.value[zoneID] ?? 0
        let modifications = database.state.withValue { state in
          ((state.storage[zoneID]?.records.values).map { Array($0) } ?? [])
            .map { $0.copy() as! CKRecord }
            .filter {
              precondition(
                $0._recordChangeTag != nil,
                "Records stored in database should have their 'recordChangeTag' assigned."
              )
              return $0._recordChangeTag! > token
            }
            .sorted { $0._recordChangeTag! < $1._recordChangeTag! }
        }
        let deletions = database.state.withValue {
          let records = $0.deletedRecords.filter { recordID, _ in recordID.zoneID == zoneID }
          $0.deletedRecords.removeAll { recordID, _ in recordID.zoneID == zoneID }
          return records
        }
        guard !modifications.isEmpty || !deletions.isEmpty else { continue }
        // NB: Real fetches return 200 records per page ('moreComing'); the token advances per page.
        let chunks = stride(from: 0, to: max(modifications.count, 1), by: Self.maxRecordsPerFetchPage)
          .map { Array(modifications[$0..<min($0 + Self.maxRecordsPerFetchPage, modifications.count)]) }
        for (index, chunk) in chunks.enumerated() {
          pages.append((zoneID, chunk, index == 0 ? deletions : []))
        }
      }

      if state.isRealistic.value {
        await parentSyncEngine.handleEvent(.willFetchChanges, syncEngine: self)
        for zoneID in expiredZoneIDs {
          await parentSyncEngine.handleEvent(.willFetchRecordZoneChanges(zoneID: zoneID), syncEngine: self)
          await parentSyncEngine.handleEvent(
            .didFetchRecordZoneChanges(zoneID: zoneID, error: CKError(.changeTokenExpired)),
            syncEngine: self
          )
        }
        for page in pages {
          await parentSyncEngine.handleEvent(
            .willFetchRecordZoneChanges(zoneID: page.zoneID), syncEngine: self
          )
          await deliver(page.modifications, page.deletions, zoneID: page.zoneID)
          await parentSyncEngine.handleEvent(
            .didFetchRecordZoneChanges(zoneID: page.zoneID, error: nil), syncEngine: self
          )
        }
        await parentSyncEngine.handleEvent(.didFetchChanges, syncEngine: self)
      } else if !pages.isEmpty {
        for page in pages {
          advanceToken(page.modifications, zoneID: page.zoneID)
        }
        await parentSyncEngine.handleEvent(
          .fetchedRecordZoneChanges(
            modifications: pages.flatMap(\.modifications),
            deletions: pages.flatMap(\.deletions)
          ),
          syncEngine: self
        )
      }
    }

    /// The most records one fetch page returns.
    package static let maxRecordsPerFetchPage = 200

    /// Makes the next fetch of the zone fail with `changeTokenExpired`, then fetch everything.
    package func expireChangeToken(zoneID: CKRecordZone.ID) {
      state.expiredZoneIDs.withValue { _ = $0.insert(zoneID) }
    }

    private func advanceToken(_ modifications: [CKRecord], zoneID: CKRecordZone.ID) {
      state.changeTags.withValue { tags in
        tags[zoneID] = modifications.compactMap(\._recordChangeTag).max() ?? tags[zoneID]
      }
    }

    private func deliver(
      _ modifications: [CKRecord],
      _ deletions: [(recordID: CKRecord.ID, recordType: CKRecord.RecordType)],
      zoneID: CKRecordZone.ID
    ) async {
      advanceToken(modifications, zoneID: zoneID)
      state.deliveredFetchPages.withValue { $0.append(modifications.count) }
      guard !modifications.isEmpty || !deletions.isEmpty else { return }
      await parentSyncEngine.handleEvent(
        .fetchedRecordZoneChanges(modifications: modifications, deletions: deletions),
        syncEngine: self
      )
    }

    /// Lets the system scheduler fire after a transient refusal, scheduling a send if work is left.
    ///
    /// With a ``MockCloudDatabase/Profile`` the simulated clock advances by its
    /// `schedulerWaitSeconds`, so the throttle bucket refills.
    package func advanceScheduler() {
      if let profile = database.profile.value {
        database.advanceSimulatedTime(by: profile.schedulerWaitSeconds)
      }
      database.restoreFuzzedAccountStatus()
      state.isSchedulerWaiting.setValue(false)
      if !state.pendingRecordZoneChanges.isEmpty { state.isSendScheduled.setValue(true) }
    }

    package func sendChanges(_ options: CKSyncEngine.SendChangesOptions) async throws {
      if state.isRealistic.value {
        // A user-initiated send bypasses the scheduler.
        state.isSchedulerWaiting.setValue(false)
        if !state.pendingDatabaseChanges.isEmpty {
          try await parentSyncEngine.processPendingDatabaseChanges(scope: database.databaseScope)
        }
        try await parentSyncEngine.runSendCycle(scope: database.databaseScope)
        return
      }

      if !parentSyncEngine.syncEngine(for: database.databaseScope).state.pendingDatabaseChanges
        .isEmpty
      {

        try await parentSyncEngine.processPendingDatabaseChanges(scope: database.databaseScope)
      }
      if !parentSyncEngine.syncEngine(for: database.databaseScope).state.pendingRecordZoneChanges
        .isEmpty
      {

        try await parentSyncEngine.processPendingRecordZoneChanges(scope: database.databaseScope)
      }
    }

    package func recordZoneChangeBatch(
      pendingChanges: [CKSyncEngine.PendingRecordZoneChange],
      recordProvider: @Sendable (CKRecord.ID) async -> CKRecord?
    ) async -> CKSyncEngine.RecordZoneChangeBatch? {
      var recordsToSave: [CKRecord] = []
      var recordIDsSkipped: [CKRecord.ID] = []
      var recordIDsToDelete: [CKRecord.ID] = []
      for pendingChange in pendingChanges.prefix(SyncEngine.maxBatchRecords) {
        switch pendingChange {
        case .saveRecord(let recordID):
          guard let record = await recordProvider(recordID)
          else {
            recordIDsSkipped.append(recordID)
            continue
          }
          recordsToSave.append(record)
        case .deleteRecord(let recordID):
          recordIDsToDelete.append(recordID)
        @unknown default:
          fatalError()
        }
      }

      // NB: Like the real engine, the batch's changes leave the pending list and stay in flight
      //     until the request ends.
      state.markInFlight(
        recordsToSave.map { .saveRecord($0.recordID) } + recordIDsToDelete.map { .deleteRecord($0) }
      )
      return CKSyncEngine.RecordZoneChangeBatch(
        recordsToSave: recordsToSave,
        recordIDsToDelete: recordIDsToDelete
      )
    }

    /// Cancelling (or a crash) between building a batch and its result puts the batch's changes
    /// back in the pending list; nothing is lost.
    package func cancelOperations() async {
      state.requeueInFlight()
    }
  }

  @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
  package final class MockSyncEngineState: CKSyncEngineStateProtocol {
    /// Per-zone fetch tokens: the highest change tag already delivered.
    package let changeTags = LockIsolated<[CKRecordZone.ID: Int]>([:])
    /// Sizes of the pages `fetchChanges` delivered, in order (realistic mode).
    package let deliveredFetchPages = LockIsolated<[Int]>([])
    package let expiredZoneIDs = LockIsolated<Set<CKRecordZone.ID>>([])
    /// Opt-in: behave like `CKSyncEngine` about when work happens. Failed changes are re-queued
    /// after `didSendChanges`, `sendChanges()` runs a whole cycle with `willSendChanges`/
    /// `didSendChanges`, and a send is only scheduled by changes added outside a cycle. Off by
    /// default so tests that drive one batch by hand keep working.
    package let isRealistic = LockIsolated(false)
    package let isSendCycleRunning = LockIsolated(false)
    /// Opt-in: post `stateUpdate` events during a send cycle, with a serialization whose size
    /// grows with the pending changes (about 375 bytes each; 45 MB at 120k changes on device).
    /// The serialization is a stand-in (JSON around a property list) that the library stores
    /// and restores like the real one, so `restoredState` and its 16 MB guard run for real.
    /// `nil` posts none.
    package let stateBytesPerPendingChange = LockIsolated<Int?>(nil)
    /// Set when changes are added outside a send cycle (the real engine then schedules a send);
    /// consumed by ``SyncEngine/runScheduledSend(scope:)``.
    package let isSendScheduled = LockIsolated(false)
    /// Set after a send cycle ended on a transient refusal: the real engine hands the retry to the
    /// system scheduler and sends nothing until then. Cleared by ``MockSyncEngine/advanceScheduler()``.
    package let isSchedulerWaiting = LockIsolated(false)
    package let _inFlightRecordZoneChanges = LockIsolated<
      OrderedSet<CKSyncEngine.PendingRecordZoneChange>
    >([])
    package var inFlightRecordZoneChanges: [CKSyncEngine.PendingRecordZoneChange] {
      _inFlightRecordZoneChanges.withValue { Array($0) }
    }

    package func markInFlight(_ changes: [CKSyncEngine.PendingRecordZoneChange]) {
      remove(pendingRecordZoneChanges: changes)
      _inFlightRecordZoneChanges.withValue { $0.append(contentsOf: changes) }
    }

    package func finishInFlight(_ changes: [CKSyncEngine.PendingRecordZoneChange]) {
      _inFlightRecordZoneChanges.withValue { $0.subtract(changes) }
    }

    package func requeueInFlight() {
      let changes = _inFlightRecordZoneChanges.withValue { inFlight -> [CKSyncEngine.PendingRecordZoneChange] in
        defer { inFlight.removeAll() }
        return Array(inFlight)
      }
      add(pendingRecordZoneChanges: changes)
    }
    package let _pendingRecordZoneChanges = LockIsolated<
      OrderedSet<CKSyncEngine.PendingRecordZoneChange>
    >([]
    )
    package let _pendingDatabaseChanges = LockIsolated<
      OrderedSet<CKSyncEngine.PendingDatabaseChange>
    >([])
    private let fileID: StaticString
    private let filePath: StaticString
    private let line: UInt
    private let column: UInt

    package init(
      fileID: StaticString = #fileID,
      filePath: StaticString = #filePath,
      line: UInt = #line,
      column: UInt = #column
    ) {
      self.fileID = fileID
      self.filePath = filePath
      self.line = line
      self.column = column
    }

    package var pendingRecordZoneChanges: [CKSyncEngine.PendingRecordZoneChange] {
      _pendingRecordZoneChanges.withValue { Array($0) }
    }

    package var pendingDatabaseChanges: [CKSyncEngine.PendingDatabaseChange] {
      _pendingDatabaseChanges.withValue { Array($0) }
    }

    package func removePendingChanges() {
      _pendingDatabaseChanges.withValue { $0.removeAll() }
      _pendingRecordZoneChanges.withValue { $0.removeAll() }
    }

    package func add(pendingRecordZoneChanges: [CKSyncEngine.PendingRecordZoneChange]) {
      self._pendingRecordZoneChanges.withValue {
        $0.append(contentsOf: pendingRecordZoneChanges)
      }
      // NB: Changes added while a cycle runs do not schedule another send (measured on device).
      if !pendingRecordZoneChanges.isEmpty, !isSendCycleRunning.value, !isSchedulerWaiting.value {
        isSendScheduled.setValue(true)
      }
    }

    package func remove(pendingRecordZoneChanges: [CKSyncEngine.PendingRecordZoneChange]) {
      self._pendingRecordZoneChanges.withValue {
        $0.subtract(pendingRecordZoneChanges)
      }
    }

    package func add(pendingDatabaseChanges: [CKSyncEngine.PendingDatabaseChange]) {
      self._pendingDatabaseChanges.withValue {
        $0.append(contentsOf: pendingDatabaseChanges)
      }
    }

    package func remove(pendingDatabaseChanges: [CKSyncEngine.PendingDatabaseChange]) {
      self._pendingDatabaseChanges.withValue {
        $0.subtract(pendingDatabaseChanges)
      }
    }
  }

  @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
  extension SyncEngine {
    package struct SendRecordsCallback {
      fileprivate let operation: @Sendable () async -> Void
      /// Whether a request was made (the batch was not `nil`).
      fileprivate var didSend = false
      fileprivate var hadFailures = false
      /// Set when the whole request was refused.
      fileprivate var wholeRequestError: CKError?
      package func receive() async {
        await operation()
      }
    }

    package func sendPendingRecordZoneChanges(
      options: CKSyncEngine.SendChangesOptions = CKSyncEngine.SendChangesOptions(),
      scope: CKDatabase.Scope,
      forceAtomicByZone: Bool? = nil,
      fileID: StaticString = #fileID,
      filePath: StaticString = #filePath,
      line: UInt = #line,
      column: UInt = #column
    ) async throws -> SendRecordsCallback {
      let syncEngine = syncEngine(for: scope)
      guard !syncEngine.state.pendingRecordZoneChanges.isEmpty
      else {
        reportIssue(
          "Processing empty set of record zone changes.",
          fileID: fileID,
          filePath: filePath,
          line: line,
          column: column
        )
        return SendRecordsCallback {}
      }
      guard try await container.accountStatus() == .available
      else {
        reportIssue(
          """
          User must be logged in to process pending changes.
          """,
          fileID: fileID,
          filePath: filePath,
          line: line,
          column: column
        )
        return SendRecordsCallback {}
      }

      var batch = await nextRecordZoneChangeBatch(
        reason: .scheduled,
        options: options,
        syncEngine: {
          switch scope {
          case .private:
            self.private
          case .shared:
            self.shared
          case .public:
            fatalError("Public database not supported in tests.")
          @unknown default:
            fatalError("Unknown database scope not supported in tests.")
          }
        }()
      )
      if let forceAtomicByZone {
        batch?.atomicByZone = forceAtomicByZone
      }
      guard let batch
      else {
        return SendRecordsCallback {}
      }

      var wholeRequestError: CKError?
      let saveResults: [CKRecord.ID: Result<CKRecord, any Error>]
      let deleteResults: [CKRecord.ID: Result<Void, any Error>]
      do {
        (saveResults, deleteResults) = try syncEngine.database.modifyRecords(
          saving: batch.recordsToSave,
          deleting: batch.recordIDsToDelete,
          savePolicy: .ifServerRecordUnchanged,
          atomically: batch.atomicByZone
        )
      } catch let error as CKError where error.isRequestRefusal {
        // NB: The engine reports a refused request as every record failing with that error.
        wholeRequestError = error
        saveResults = Dictionary(
          uniqueKeysWithValues: batch.recordsToSave.map { ($0.recordID, .failure(error)) }
        )
        deleteResults = Dictionary(
          uniqueKeysWithValues: batch.recordIDsToDelete.map { ($0, .failure(error)) }
        )
      }

      var savedRecords: [CKRecord] = []
      var failedRecordSaves: [(record: CKRecord, error: CKError)] = []
      var deletedRecordIDs: [CKRecord.ID] = []
      var failedRecordDeletes: [CKRecord.ID: CKError] = [:]
      for (recordID, result) in saveResults {
        switch result {
        case .success(let record):
          savedRecords.append(record)
        case .failure(let error as CKError):
          guard let record = batch.recordsToSave.first(where: { $0.recordID == recordID })
          else { fatalError("\(recordID.debugDescription) not found in pending changes") }
          failedRecordSaves.append((record: record, error: error))
        case .failure:
          fatalError("Mocks should only raise 'CKError' values.")
        }
      }
      for (recordID, result) in deleteResults {
        switch result {
        case .success:
          deletedRecordIDs.append(recordID)
        case .failure(let error as CKError):
          failedRecordDeletes[recordID] = error
        case .failure:
          fatalError("Mocks should only raise 'CKError' values.")
        }
      }
      syncEngine.state.remove(
        pendingRecordZoneChanges: savedRecords.map { .saveRecord($0.recordID) }
      )
      syncEngine.state.finishInFlight(
        batch.recordsToSave.map { .saveRecord($0.recordID) }
          + batch.recordIDsToDelete.map { .deleteRecord($0) }
      )
      // NB: The real engine retries a refused request itself, so those changes go back to pending.
      syncEngine.state.add(
        pendingRecordZoneChanges: failedRecordSaves.filter { $0.error.isRequestRefusal }
          .map { .saveRecord($0.record.recordID) }
          + failedRecordDeletes.filter { $0.value.isRequestRefusal }.keys.map { .deleteRecord($0) }
      )
      syncEngine.state.remove(
        pendingRecordZoneChanges: failedRecordSaves.filter { !$0.error.isRequestRefusal }
          .map { .saveRecord($0.record.recordID) }
      )
      syncEngine.state.remove(
        pendingRecordZoneChanges: deletedRecordIDs.map { .deleteRecord($0) }
      )
      syncEngine.state.remove(
        pendingRecordZoneChanges: failedRecordDeletes.filter { !$0.value.isRequestRefusal }.keys
          .map { .deleteRecord($0) }
      )

      var callback = SendRecordsCallback { [savedRecords, failedRecordSaves, deletedRecordIDs, failedRecordDeletes] in
        await syncEngine.parentSyncEngine
          .handleEvent(
            .sentRecordZoneChanges(
              savedRecords: savedRecords,
              failedRecordSaves: failedRecordSaves,
              deletedRecordIDs: deletedRecordIDs,
              failedRecordDeletes: failedRecordDeletes
            ),
            syncEngine: syncEngine
          )
      }
      callback.didSend = true
      callback.hadFailures = !failedRecordSaves.isEmpty || !failedRecordDeletes.isEmpty
      callback.wholeRequestError = wholeRequestError
      return callback
    }

    /// Runs one send cycle the way `CKSyncEngine` does: `willSendChanges`, batches of at most
    /// 250 records until nothing is left, then `didSendChanges`.
    ///
    /// A cycle ends at the first batch with a failure. After a refusal (`serviceUnavailable`,
    /// `requestRateLimited`, ...) the engine also waits for the scheduler: nothing is sent until
    /// ``MockSyncEngine/advanceScheduler()`` or a manual `sendChanges()`.
    // devmode: ending the cycle on any failed batch is the measured case (everything failed);
    // whether the real engine continues after a partial failure is unverified.
    package func runSendCycle(scope: CKDatabase.Scope) async throws {
      let engine = syncEngine(for: scope)
      let state = engine.state
      let alreadyRunning = state.isSendCycleRunning.withValue { isRunning in
        defer { isRunning = true }
        return isRunning
      }
      guard !alreadyRunning else { return }
      defer { state.isSendCycleRunning.setValue(false) }
      state.isSendScheduled.setValue(false)

      engine.database.fuzzAccountStatus()
      guard try await container.accountStatus() == .available else {
        // NB: Without an account the engine sends nothing and waits for it to come back.
        state.isSchedulerWaiting.setValue(true)
        return
      }

      await handleEvent(.willSendChanges, syncEngine: engine)
      await postStateUpdate(engine)
      while !state.pendingRecordZoneChanges.isEmpty {
        let callback = try await sendPendingRecordZoneChanges(scope: scope)
        guard callback.didSend else { break }
        await callback.receive()
        await postStateUpdate(engine)
        if callback.hadFailures {
          if callback.wholeRequestError != nil { state.isSchedulerWaiting.setValue(true) }
          break
        }
      }
      await handleEvent(.didSendChanges, syncEngine: engine)
      await postStateUpdate(engine)
    }

    private func postStateUpdate(_ engine: MockSyncEngine) async {
      guard let bytes = engine.state.stateBytesPerPendingChange.value else { return }
      let changes = engine.state.pendingRecordZoneChanges.map { change -> [String: Any] in
        switch change {
        case .saveRecord(let id): ["type": 0, "recordName": id.recordName]
        case .deleteRecord(let id): ["type": 1, "recordName": id.recordName]
        @unknown default: [:]
        }
      }
      // NB: Base64 in the stored JSON adds a third; pad so the stored size is about `bytes` each.
      let padding = Data(count: max(bytes * 3 / 4 - 40, 0))
      let plist = ["pendingRecordModifications": changes.map { $0.merging(["pad": padding]) { $1 }}]
      guard
        let data = try? PropertyListSerialization.data(
          fromPropertyList: plist, format: .binary, options: 0),
        let json = try? JSONSerialization.data(
          withJSONObject: ["data": data.base64EncodedString()]),
        let serialization = try? JSONDecoder().decode(
          CKSyncEngine.State.Serialization.self, from: json)
      else {
        reportIssue("Could not build a state serialization.")
        return
      }
      await handleEvent(.stateUpdate(stateSerialization: serialization), syncEngine: engine)
    }

    /// Runs the send the real engine would have scheduled, if there is one. Returns whether it ran.
    @discardableResult
    package func runScheduledSend(scope: CKDatabase.Scope) async throws -> Bool {
      let state = syncEngine(for: scope).state
      guard state.isSendScheduled.value, !state.isSchedulerWaiting.value else { return false }
      try await runSendCycle(scope: scope)
      return true
    }

    package func processPendingRecordZoneChanges(
      options: CKSyncEngine.SendChangesOptions = CKSyncEngine.SendChangesOptions(),
      scope: CKDatabase.Scope,
      forceAtomicByZone: Bool? = nil,
      fileID: StaticString = #fileID,
      filePath: StaticString = #filePath,
      line: UInt = #line,
      column: UInt = #column
    ) async throws {
      try await sendPendingRecordZoneChanges(
        options: options,
        scope: scope,
        forceAtomicByZone: forceAtomicByZone,
        fileID: fileID,
        filePath: filePath,
        line: line,
        column: column
      )
      .receive()
    }

    package func processPendingDatabaseChanges(
      scope: CKDatabase.Scope,
      fileID: StaticString = #fileID,
      filePath: StaticString = #filePath,
      line: UInt = #line,
      column: UInt = #column
    ) async throws {
      let syncEngine = syncEngine(for: scope)
      guard !syncEngine.state.pendingDatabaseChanges.isEmpty
      else {
        reportIssue(
          "Processing empty set of database changes.",
          fileID: fileID,
          filePath: filePath,
          line: line,
          column: column
        )
        return
      }
      guard try await container.accountStatus() == .available
      else {
        reportIssue(
          "User must be logged in to process pending changes.",
          fileID: fileID,
          filePath: filePath,
          line: line,
          column: column
        )
        return
      }

      var zonesToSave: [CKRecordZone] = []
      var zoneIDsToDelete: [CKRecordZone.ID] = []
      for pendingDatabaseChange in syncEngine.state.pendingDatabaseChanges {
        switch pendingDatabaseChange {
        case .saveZone(let zone):
          zonesToSave.append(zone)
        case .deleteZone(let zoneID):
          zoneIDsToDelete.append(zoneID)
        @unknown default:
          fatalError("Unsupported pendingDatabaseChange: \(pendingDatabaseChange)")
        }
      }
      let results:
        (
          saveResults: [CKRecordZone.ID: Result<CKRecordZone, any Error>],
          deleteResults: [CKRecordZone.ID: Result<Void, any Error>]
        ) = try syncEngine.database.modifyRecordZones(
          saving: zonesToSave,
          deleting: zoneIDsToDelete
        )
      var savedZones: [CKRecordZone] = []
      var failedZoneSaves: [(zone: CKRecordZone, error: CKError)] = []
      var deletedZoneIDs: [CKRecordZone.ID] = []
      var failedZoneDeletes: [CKRecordZone.ID: CKError] = [:]
      for (zoneID, saveResult) in results.saveResults {
        switch saveResult {
        case .success(let zone):
          savedZones.append(zone)
        case .failure(let error as CKError):
          failedZoneSaves.append((zonesToSave.first(where: { $0.zoneID == zoneID })!, error))
        case .failure(let error):
          reportIssue("Error thrown not CKError: \(error)")
        }
      }
      for (zoneID, deleteResult) in results.deleteResults {
        switch deleteResult {
        case .success:
          deletedZoneIDs.append(zoneID)
        case .failure(let error as CKError):
          failedZoneDeletes[zoneID] = error
        case .failure(let error):
          reportIssue("Error thrown not CKError: \(error)")
        }
      }

      syncEngine.state.remove(pendingDatabaseChanges: savedZones.map { .saveZone($0) })
      syncEngine.state.remove(pendingDatabaseChanges: deletedZoneIDs.map { .deleteZone($0) })

      await syncEngine.parentSyncEngine
        .handleEvent(
          .sentDatabaseChanges(
            savedZones: savedZones,
            failedZoneSaves: failedZoneSaves,
            deletedZoneIDs: deletedZoneIDs,
            failedZoneDeletes: failedZoneDeletes
          ),
          syncEngine: syncEngine
        )
    }

    package var `private`: MockSyncEngine {
      syncEngines.private as! MockSyncEngine
    }
    package var shared: MockSyncEngine {
      syncEngines.shared as! MockSyncEngine
    }

    package func syncEngine(for scope: CKDatabase.Scope) -> MockSyncEngine {
      switch scope {
      case .public:
        fatalError("Public database not supported in sync engines.")
      case .private:
        `private`
      case .shared:
        shared
      @unknown default:
        fatalError("Unknown database scope not supported in sync engines.")
      }
    }
  }
#endif
