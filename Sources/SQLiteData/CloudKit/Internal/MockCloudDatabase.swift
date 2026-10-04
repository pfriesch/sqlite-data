#if canImport(CloudKit)
  package import ConcurrencyExtras
  package import CloudKit
  import Dependencies
  import IssueReporting

  @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
  package final class MockCloudDatabase: CloudDatabase {
    package let state = LockIsolated(State())
    package let databaseScope: CKDatabase.Scope
    let _container = IsolatedWeakVar<MockCloudContainer>()
    let dataManager = Dependency(\.dataManager)

    package struct State {
      private var lastRecordChangeTag = 0
      package var storage: [CKRecordZone.ID: Zone] = [:]
      var assets: [AssetID: Data] = [:]
      var deletedRecords: [(CKRecord.ID, CKRecord.RecordType)] = []
      mutating func nextRecordChangeTag() -> Int {
        lastRecordChangeTag += 1
        return lastRecordChangeTag
      }
    }

    struct AssetID: Hashable {
      let recordID: CKRecord.ID
      let key: String
    }

    package struct Zone {
      package var zone: CKRecordZone
      package var records: [CKRecord.ID: CKRecord] = [:]
    }

    package init(databaseScope: CKDatabase.Scope) {
      self.databaseScope = databaseScope
    }

    package func set(container: MockCloudContainer) {
      _container.set(container)
    }

    package var container: MockCloudContainer {
      _container.value!
    }

    /// What the server accepts in one request; larger requests fail with `limitExceeded`.
    package static let maxItemsPerRequest = 400
    package static let maxBytesPerRequest = 2_000_000

    private let injectedErrors = LockIsolated<[CKError]>([])

    /// Makes the next `count` `modifyRecords` requests fail as a whole with `error`.
    package func failNextRequests(_ count: Int = 1, with error: CKError) {
      injectedErrors.withValue { $0 += Array(repeating: error, count: count) }
    }

    private func nextInjectedError() -> CKError? {
      injectedErrors.withValue { $0.isEmpty ? nil : $0.removeFirst() }
    }

    package func record(for recordID: CKRecord.ID) throws -> CKRecord {
      let accountStatus = container.accountStatus()
      guard accountStatus == .available
      else { throw ckError(forAccountStatus: accountStatus) }
      let record = try state.withValue { state in
        guard let zone = state.storage[recordID.zoneID]
        else { throw CKError(.zoneNotFound) }
        guard let record = zone.records[recordID]
        else { throw CKError(.unknownItem) }
        guard let record = record.copy() as? CKRecord
        else { fatalError("Could not copy CKRecord.") }
        return record
      }

      try state.withValue { state in
        for key in record.allKeys() {
          guard let assetData = state.assets[AssetID(recordID: record.recordID, key: key)]
          else { continue }
          let url = dataManager.wrappedValue.temporaryDirectory.appending(path: UUID().uuidString)
          try dataManager.wrappedValue.save(assetData, to: url)
          record[key] = CKAsset(fileURL: url)
        }
      }

      return record
    }

    package func records(
      for ids: [CKRecord.ID],
      desiredKeys: [CKRecord.FieldKey]?
    ) throws -> [CKRecord.ID: Result<CKRecord, any Error>] {
      let accountStatus = container.accountStatus()
      guard accountStatus == .available
      else { throw ckError(forAccountStatus: accountStatus) }

      guard ids.count <= Self.maxItemsPerRequest
      else { throw CKError(.limitExceeded) }

      var results: [CKRecord.ID: Result<CKRecord, any Error>] = [:]
      for id in ids {
        results[id] = Result { try record(for: id) }
      }
      return results
    }

    /// What a request costs and when the server refuses it, so a workload run through the mock
    /// reports the time and throttling the real service would show for the same configuration.
    ///
    /// Time is *simulated*: it only advances through ``simulatedSeconds``, never by sleeping, so
    /// runs stay fast and deterministic. Numbers are from the iPhone measurements in
    /// `Docs/CloudKitSyncInternals.md` (Development environment). Only what was measured is
    /// modeled; everything else is a knob left at 1.
    package struct Profile: Sendable {
      /// Raw `modifyRecords` time per record: 2.10 s per 250 records without `parent`.
      package var secondsPerRecord = 2.10 / 250
      /// A record with `record.parent` (tables registered with `tables:`, not `privateTables:`)
      /// takes this many times longer: 4.22 s vs 2.10 s per 250 records.
      package var parentMultiplier = 4.22 / 2.10
      /// Production vs Development speed is not measured; Development is the slower one.
      package var environmentMultiplier = 1.0
      /// A refused request returns in 0.3-0.5 s.
      package var refusalSeconds = 0.4
      /// How long the system scheduler stays quiet after a refusal: 32 s to 12+ min observed.
      package var schedulerWaitSeconds = 60.0
      /// `nil` never throttles.
      package var throttle: Throttle? = Throttle()

      /// Token bucket over records. The server tripped at about 750-1,750 records within 20-40 s
      /// and passed about 1,200 records/min; retry-after was 11-76 s.
      package struct Throttle: Sendable {
        package var burstRecords = 1_000.0
        package var recordsPerSecond = 20.0
        package var minimumRetryAfterSeconds = 11.0
        package init() {}
      }

      package static let measuredDevelopment = Profile()
      package init() {}
    }

    /// `nil` (the default) makes requests instant and never throttles.
    package let profile = LockIsolated<Profile?>(nil)
    /// Time the requests made so far would have taken, plus time advanced by tests.
    package let simulatedSeconds = LockIsolated(0.0)
    private let bucket = LockIsolated<(tokens: Double, at: Double)?>(nil)

    package func advanceSimulatedTime(by seconds: Double) {
      simulatedSeconds.withValue { $0 += seconds }
    }

    private func throttleError(forRecords count: Int, profile: Profile) -> CKError? {
      guard let throttle = profile.throttle else { return nil }
      let now = simulatedSeconds.value
      return bucket.withValue { bucket in
        let previous = bucket ?? (throttle.burstRecords, now)
        let tokens = min(
          throttle.burstRecords,
          previous.tokens + (now - previous.at) * throttle.recordsPerSecond
        )
        if tokens >= Double(count) {
          bucket = (tokens - Double(count), now)
          return nil
        }
        bucket = (tokens, now)
        let wait = (Double(count) - tokens) / throttle.recordsPerSecond
        return .throttled(retryAfter: max(wait, throttle.minimumRetryAfterSeconds).rounded(.up))
      }
    }

    package func modifyRecords(
      saving recordsToSave: [CKRecord] = [],
      deleting recordIDsToDelete: [CKRecord.ID] = [],
      savePolicy: CKModifyRecordsOperation.RecordSavePolicy = .ifServerRecordUnchanged,
      atomically: Bool = true
    ) throws -> (
      saveResults: [CKRecord.ID: Result<CKRecord, any Error>],
      deleteResults: [CKRecord.ID: Result<Void, any Error>]
    ) {
      guard let profile = profile.value
      else {
        return try applyModifyRecords(
          saving: recordsToSave, deleting: recordIDsToDelete, savePolicy: savePolicy,
          atomically: atomically
        )
      }
      if let error = throttleError(
        forRecords: recordsToSave.count + recordIDsToDelete.count, profile: profile
      ) {
        advanceSimulatedTime(by: profile.refusalSeconds)
        throw error
      }
      let results = try applyModifyRecords(
        saving: recordsToSave, deleting: recordIDsToDelete, savePolicy: savePolicy,
        atomically: atomically
      )
      let weight = recordsToSave.reduce(0.0) { $0 + ($1.parent == nil ? 1 : profile.parentMultiplier) }
        + Double(recordIDsToDelete.count)
      advanceSimulatedTime(by: weight * profile.secondsPerRecord * profile.environmentMultiplier)
      return results
    }

    private func applyModifyRecords(
      saving recordsToSave: [CKRecord] = [],
      deleting recordIDsToDelete: [CKRecord.ID] = [],
      savePolicy: CKModifyRecordsOperation.RecordSavePolicy = .ifServerRecordUnchanged,
      atomically: Bool = true
    ) throws -> (
      saveResults: [CKRecord.ID: Result<CKRecord, any Error>],
      deleteResults: [CKRecord.ID: Result<Void, any Error>]
    ) {
      let accountStatus = container.accountStatus()
      guard accountStatus == .available
      else { throw ckError(forAccountStatus: accountStatus) }

      if let error = nextInjectedError() { throw error }

      guard
        (recordsToSave.count + recordIDsToDelete.count) <= Self.maxItemsPerRequest,
        recordsToSave.reduce(0, { $0 + $1.estimatedByteCount }) <= Self.maxBytesPerRequest
      else {
        throw CKError(.limitExceeded)
      }

      return state.withValue { state in
        let previousStorage = state.storage
        var saveResults: [CKRecord.ID: Result<CKRecord, any Error>] = [:]
        var deleteResults: [CKRecord.ID: Result<Void, any Error>] = [:]

        switch savePolicy {
        case .ifServerRecordUnchanged:
          for recordToSave in recordsToSave {
            if let share = recordToSave as? CKShare {
              let isSavingRootRecord = recordsToSave.contains(where: {
                $0.share?.recordID == share.recordID
              })
              let shareWasPreviouslySaved =
                state.storage[share.recordID.zoneID]?.records[share.recordID] != nil
              guard shareWasPreviouslySaved || isSavingRootRecord
              else {
                saveResults[recordToSave.recordID] = .failure(CKError(.invalidArguments))
                continue
              }
            } else if databaseScope == .shared,
              recordToSave.parent == nil,
              recordToSave.share == nil
            {
              // NB: Emit 'permissionFailure' if saving to shared database with no parent reference
              //     or share reference.
              saveResults[recordToSave.recordID] = .failure(CKError(.permissionFailure))
              continue
            }

            // NB: Emit 'zoneNotFound' error if saving record with a zone not found in database.
            guard state.storage[recordToSave.recordID.zoneID] != nil
            else {
              saveResults[recordToSave.recordID] = .failure(CKError(.zoneNotFound))
              continue
            }

            let existingRecord = state.storage[recordToSave.recordID.zoneID]?.records[
              recordToSave.recordID
            ]

            func saveRecordToDatabase() {
              let hasReferenceViolation =
                recordToSave.parent.map { parent in
                  state.storage[parent.recordID.zoneID]?.records[parent.recordID] == nil
                    && !recordsToSave.contains { $0.recordID == parent.recordID }
                }
                ?? false
              guard !hasReferenceViolation
              else {
                saveResults[recordToSave.recordID] = .failure(CKError(.referenceViolation))
                return
              }

              func root(of record: CKRecord) -> CKRecord {
                guard let parent = record.parent
                else { return record }
                return (state.storage[parent.recordID.zoneID]?.records[parent.recordID]).map(
                  root
                ) ?? record
              }
              func share(for rootRecord: CKRecord) -> CKShare? {
                for (_, record) in state.storage[rootRecord.recordID.zoneID]?.records ?? [:] {
                  guard record.recordID == rootRecord.share?.recordID
                  else { continue }
                  return record as? CKShare
                }
                return nil
              }
              let rootRecord = root(of: recordToSave)
              let share = share(for: rootRecord)
              let isSavingShare = recordsToSave.contains { $0.recordID == share?.recordID }
              if !isSavingShare,
                !(recordToSave is CKShare),
                let share,
                !(share.publicPermission == .readWrite
                  || share.currentUserParticipant?.permission == .readWrite)
              {
                saveResults[recordToSave.recordID] = .failure(CKError(.permissionFailure))
                return
              }

              guard let databaseCopy = recordToSave.copy() as? CKRecord
              else { fatalError("Could not copy CKRecord.") }
              databaseCopy._recordChangeTag = state.nextRecordChangeTag()

              for key in databaseCopy.allKeys() {
                guard let assetURL = (databaseCopy[key] as? CKAsset)?.fileURL
                else { continue }
                state.assets[AssetID(recordID: databaseCopy.recordID, key: key)] =
                  try? dataManager.wrappedValue
                  .load(assetURL)
              }

              // TODO: This should merge copy's values to more accurately reflect reality
              state.storage[recordToSave.recordID.zoneID]?.records[recordToSave.recordID] =
                databaseCopy
              saveResults[recordToSave.recordID] = .success(databaseCopy.copy() as! CKRecord)

              // NB: "Touch" parent records when saving a child:
              if let parent = recordToSave.parent,
                // If the parent isn't also being saved in this batch.
                !recordsToSave.contains(where: { $0.recordID == parent.recordID }),
                // And if the parent is in the database.
                let parentRecord = state.storage[parent.recordID.zoneID]?.records[parent.recordID]?
                  .copy()
                  as? CKRecord
              {
                parentRecord._recordChangeTag = state.nextRecordChangeTag()
                state.storage[parent.recordID.zoneID]?.records[parent.recordID] = parentRecord
              }
            }

            switch (existingRecord, recordToSave._recordChangeTag) {
            case (.some(let existingRecord), .some(let recordToSaveChangeTag)):
              // We are trying to save a record with a change tag that also already exists in the
              // DB. If the tags match, we can save the record. Otherwise, we notify the sync engine
              // that the server record has changed since it was last synced.
              if existingRecord._recordChangeTag == recordToSaveChangeTag {
                precondition(existingRecord._recordChangeTag != nil)
                saveRecordToDatabase()
              } else {
                saveResults[recordToSave.recordID] = .failure(
                  CKError(
                    .serverRecordChanged,
                    userInfo: [
                      CKRecordChangedErrorServerRecordKey: existingRecord.copy() as Any,
                      CKRecordChangedErrorClientRecordKey: recordToSave.copy(),
                    ]
                  )
                )
              }
              break
            case (.some(let existingRecord), .none):
              // We are trying to save a record that does not have a change tag yet also already
              // exists in the DB. This means the user has created a new CKRecord from scratch,
              // giving it a new identity, rather than leveraging an existing CKRecord.
              saveResults[recordToSave.recordID] = .failure(
                CKError(
                  .serverRejectedRequest,
                  userInfo: [
                    CKRecordChangedErrorServerRecordKey: existingRecord.copy() as Any,
                    CKRecordChangedErrorClientRecordKey: recordToSave.copy(),
                  ]
                )
              )
            case (.none, .some):
              // We are trying to save a record with a change tag but it does not exist in the DB.
              // This means the record was deleted by another device.
              saveResults[recordToSave.recordID] = .failure(CKError(.unknownItem))
            case (.none, .none):
              // We are trying to save a record with no change tag and no existing record in the DB.
              // This means it's a brand new record.
              saveRecordToDatabase()
            }
          }
        case .allKeys, .changedKeys:
          fatalError()
        @unknown default:
          fatalError()
        }
        for recordIDToDelete in recordIDsToDelete {
          guard state.storage[recordIDToDelete.zoneID] != nil
          else {
            deleteResults[recordIDToDelete] = .failure(CKError(.zoneNotFound))
            continue
          }
          let hasReferenceViolation = !Set(
            state.storage[recordIDToDelete.zoneID]?.records.values
              .compactMap { $0.parent?.recordID == recordIDToDelete ? $0.recordID : nil }
              ?? []
          )
          .subtracting(recordIDsToDelete)
          .isEmpty

          guard !hasReferenceViolation
          else {
            deleteResults[recordIDToDelete] = .failure(CKError(.referenceViolation))
            continue
          }
          let recordToDelete = state.storage[recordIDToDelete.zoneID]?.records[recordIDToDelete]
          state.storage[recordIDToDelete.zoneID]?.records[recordIDToDelete] = nil
          deleteResults[recordIDToDelete] = .success(())
          if let recordType = recordToDelete?.recordType {
            state.deletedRecords.append((recordIDToDelete, recordType))
          }

          // NB: If deleting a share that the current user owns, delete the shared records and all
          //     associated records.
          if databaseScope == .shared,
            let shareToDelete = recordToDelete as? CKShare,
            shareToDelete.recordID.zoneID.ownerName == CKCurrentUserDefaultName
          {
            func deleteRecords(referencing recordID: CKRecord.ID) {
              for recordToDelete in (state.storage[recordIDToDelete.zoneID]?.records ?? [:]).values
              {
                guard
                  recordToDelete.share?.recordID == recordID
                    || recordToDelete.parent?.recordID == recordID
                else {
                  continue
                }
                state.storage[recordIDToDelete.zoneID]?.records[recordToDelete.recordID] = nil
                deleteResults[recordToDelete.recordID] = .success(())
                state.deletedRecords.append((recordIDToDelete, recordToDelete.recordType))
                deleteRecords(referencing: recordToDelete.recordID)
              }
            }
            deleteRecords(referencing: shareToDelete.recordID)
          }
        }

        guard atomically
        else {
          return (saveResults: saveResults, deleteResults: deleteResults)
        }

        let affectedZones = Set(
          recordsToSave.map(\.recordID.zoneID) + recordIDsToDelete.map(\.zoneID)
        )
        for zoneID in affectedZones {
          let saveResultsInZone = saveResults.filter { recordID, _ in recordID.zoneID == zoneID }
          let deleteResultsInZone = deleteResults.filter { recordID, _ in
            recordID.zoneID == zoneID
          }
          let saveSuccessRecordIDs = saveResultsInZone.compactMap { recordID, result in
            (try? result.get()) == nil ? nil : recordID
          }
          let deleteSuccessRecordIDs = deleteResultsInZone.compactMap { recordID, result in
            (try? result.get()) == nil ? nil : recordID
          }
          guard
            saveSuccessRecordIDs.count != saveResultsInZone.count
              || deleteSuccessRecordIDs.count != deleteResultsInZone.count
          else {
            continue
          }
          // Every successful save and deletion becomes a '.batchRequestFailed'.
          for saveSuccessRecordID in saveSuccessRecordIDs {
            saveResults[saveSuccessRecordID] = .failure(CKError(.batchRequestFailed))
          }
          for deleteSuccessRecordID in deleteSuccessRecordIDs {
            deleteResults[deleteSuccessRecordID] = .failure(CKError(.batchRequestFailed))
          }
          // All storage changes are reverted in zone.
          state.storage[zoneID]?.records = previousStorage[zoneID]?.records ?? [:]
        }
        return (saveResults: saveResults, deleteResults: deleteResults)
      }
    }

    package func modifyRecordZones(
      saving recordZonesToSave: [CKRecordZone] = [],
      deleting recordZoneIDsToDelete: [CKRecordZone.ID] = []
    ) throws -> (
      saveResults: [CKRecordZone.ID: Result<CKRecordZone, any Error>],
      deleteResults: [CKRecordZone.ID: Result<Void, any Error>]
    ) {
      let accountStatus = container.accountStatus()
      guard accountStatus == .available
      else { throw ckError(forAccountStatus: accountStatus) }

      return state.withValue { state in
        var saveResults: [CKRecordZone.ID: Result<CKRecordZone, any Error>] = [:]
        var deleteResults: [CKRecordZone.ID: Result<Void, any Error>] = [:]

        for recordZoneToSave in recordZonesToSave {
          state.storage[recordZoneToSave.zoneID] =
            state.storage[recordZoneToSave.zoneID] ?? Zone(zone: recordZoneToSave)
          saveResults[recordZoneToSave.zoneID] = .success(recordZoneToSave)
        }

        for recordZoneIDsToDelete in recordZoneIDsToDelete {
          guard state.storage[recordZoneIDsToDelete] != nil
          else {
            deleteResults[recordZoneIDsToDelete] = .failure(CKError(.zoneNotFound))
            continue
          }
          state.storage[recordZoneIDsToDelete] = nil
          deleteResults[recordZoneIDsToDelete] = .success(())
        }

        return (saveResults: saveResults, deleteResults: deleteResults)
      }
    }

    package nonisolated static func == (lhs: MockCloudDatabase, rhs: MockCloudDatabase) -> Bool {
      lhs === rhs
    }

    package nonisolated func hash(into hasher: inout Hasher) {
      hasher.combine(ObjectIdentifier(self))
    }
  }

  @available(macOS 13, iOS 16, tvOS 16, watchOS 9, *)
  private func ckError(forAccountStatus accountStatus: CKAccountStatus) -> CKError {
    switch accountStatus {
    case .couldNotDetermine, .restricted, .noAccount:
      return CKError(.notAuthenticated)
    case .temporarilyUnavailable:
      return CKError(.accountTemporarilyUnavailable)
    case .available:
      fatalError()
    @unknown default:
      fatalError()
    }
  }

  @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
  extension CKError {
    /// A throttle refusal as CloudKit sends it: `serviceUnavailable` with a retry-after.
    package static func throttled(retryAfter seconds: Double = 30) -> CKError {
      CKError(.serviceUnavailable, userInfo: [CKErrorRetryAfterKey: seconds])
    }

    /// Errors that fail a whole request (every record in it) rather than one record.
    var isRequestRefusal: Bool {
      switch code {
      case .serviceUnavailable, .requestRateLimited, .zoneBusy, .networkFailure,
        .networkUnavailable, .limitExceeded:
        true
      default:
        false
      }
    }
  }

  extension CKRecord {
    /// Rough size of the record data a request carries; enough to trip the 2 MB limit.
    fileprivate var estimatedByteCount: Int {
      allKeys().reduce(recordID.recordName.utf8.count) { total, key in
        switch self[key] {
        case let value as String: total + value.utf8.count
        case let value as Data: total + value.count
        default: total + 8
        }
      }
    }
  }
#endif
