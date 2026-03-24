import Foundation
import GoogleCloudStorage
import Synchronization

public final class InMemoryStorage: StorageProtocol, Sendable {

  private let objects = Mutex<[String: Data]>([:])
  private let signedURLServerHolder = Mutex<SignedURLLocalHTTPServer?>(nil)

  public init() {}

  deinit {
    let server = signedURLServerHolder.withLock { holder in
      let out = holder
      holder = nil
      return out
    }
    server?.shutdown()
  }

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
    let server = try await signedURLServerIfNeeded()
    let baseURL = try await server.ensureStarted()
    let token = UUID().uuidString

    switch action {
    case .reading:
      server.registerToken(
        token,
        kind: .read { [weak self] in
          guard let self else { return nil }
          return self.objects.withLock { $0[self.key(object: object, in: bucket)] }
        },
        expiration: expiration
      )
    case .writing:
      server.registerToken(
        token,
        kind: .write { [weak self] data, contentType in
          guard let self else { return }
          self.insert(data: data, contentType: contentType, object: object, in: bucket)
        },
        expiration: expiration
      )
    }

    return baseURL + "/" + token
  }

  private func signedURLServerIfNeeded() async throws -> SignedURLLocalHTTPServer {
    if let existing = signedURLServerHolder.withLock({ $0 }) {
      return existing
    }

    let server = SignedURLLocalHTTPServer()
    _ = try await server.ensureStarted()

    return signedURLServerHolder.withLock { holder in
      if let existing = holder {
        server.shutdown()
        return existing
      }
      holder = server
      return server
    }
  }
}
