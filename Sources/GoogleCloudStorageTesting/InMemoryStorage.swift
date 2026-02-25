import Foundation
import GoogleCloudStorage
import Synchronization

public final class InMemoryStorage: StorageProtocol {

  private let objects = Mutex<[String: Data]>([:])

  public init() {}

  private func key(object: Object, in bucket: Bucket) -> String {
    bucket.name + "/" + object.path
  }

  public func insert(data: Data, contentType: String, object: Object, in bucket: Bucket) {
    objects.withLock {
      $0[key(object: object, in: bucket)] = data
    }
  }

  public func delete(object: Object, in bucket: Bucket) {
    _ = objects.withLock {
      $0.removeValue(forKey: key(object: object, in: bucket))
    }
  }

  public func download(object: Object, in bucket: Bucket) async throws -> Data {
    guard let data = objects.withLock({ $0[key(object: object, in: bucket)] }) else {
      throw StorageError.objectNotFound("Object \(object.path) not found in bucket \(bucket.name)")
    }
    return data
  }

  public func list(in bucket: Bucket) async throws -> [Object] {
    let prefix = bucket.name + "/"
    return objects.withLock { storage in
      storage.keys
        .filter { $0.hasPrefix(prefix) }
        .map { Object(path: String($0.dropFirst(prefix.count))) }
    }
  }

  public func generateSignedURL(
    for action: SignedAction,
    expiration: TimeInterval,
    object: Object,
    in bucket: Bucket
  ) async throws -> String {
    // For in-memory storage, we can't generate actual signed URLs
    throw StorageError.unsupportedOperation("InMemoryStorage does not support signed URLs")
  }
}
