import Foundation
import GoogleCloudStorage
import Synchronization

public final class LocalFileSystemStorage: StorageProtocol, Sendable {

  private let baseURL: URL
  private let signedURLServerHolder = Mutex<SignedURLLocalHTTPServer?>(nil)

  public init() throws {
    guard
      let homeDirectory = FileManager.default.homeDirectoryForCurrentUser.path(
        percentEncoded: false) as String?
    else {
      throw StorageError.initializationFailed("Could not determine home directory")
    }

    self.baseURL = URL(fileURLWithPath: homeDirectory)
      .appendingPathComponent(".google-cloud-storage")

    // Create the base directory if it doesn't exist
    try FileManager.default.createDirectory(at: baseURL, withIntermediateDirectories: true)
  }

  deinit {
    let server = signedURLServerHolder.withLock { holder in
      let out = holder
      holder = nil
      return out
    }
    server?.shutdown()
  }

  private func fileURL(for object: Object, in bucket: Bucket) -> URL {
    baseURL
      .appendingPathComponent(bucket.name)
      .appendingPathComponent(object.path)
  }

  private func bucketURL(for bucket: Bucket) -> URL {
    baseURL.appendingPathComponent(bucket.name)
  }

  public func insert(data: Data, contentType: String, object: Object, in bucket: Bucket)
    async throws
  {
    try writeDataToFileSync(data: data, object: object, in: bucket)
  }

  public func delete(object: Object, in bucket: Bucket) async throws {
    let fileURL = fileURL(for: object, in: bucket)

    guard FileManager.default.fileExists(atPath: fileURL.path) else {
      throw StorageError.objectNotFound(
        "Object \(object.path) not found in bucket \(bucket.name)")
    }

    try FileManager.default.removeItem(at: fileURL)
  }

  public func download(object: Object, in bucket: Bucket) async throws -> Data {
    let fileURL = fileURL(for: object, in: bucket)

    guard FileManager.default.fileExists(atPath: fileURL.path) else {
      throw StorageError.objectNotFound(
        "Object \(object.path) not found in bucket \(bucket.name)")
    }

    return try Data(contentsOf: fileURL)
  }

  public func list(in bucket: Bucket) async throws -> [Object] {
    let bucketURL = bucketURL(for: bucket)

    guard FileManager.default.fileExists(atPath: bucketURL.path) else {
      return []
    }

    guard
      let enumerator = FileManager.default.enumerator(
        at: bucketURL,
        includingPropertiesForKeys: [.isRegularFileKey],
        options: .skipsHiddenFiles
      )
    else {
      return []
    }

    return enumerator.allObjects.compactMap { item -> Object? in
      guard let fileURL = item as? URL,
        let resourceValues = try? fileURL.resourceValues(forKeys: [.isRegularFileKey]),
        resourceValues.isRegularFile == true
      else { return nil }
      let relativePath = fileURL.path.dropFirst(bucketURL.path.count + 1)
      return Object(path: String(relativePath))
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
          let url = self.fileURL(for: object, in: bucket)
          guard FileManager.default.fileExists(atPath: url.path) else { return nil }
          return try? Data(contentsOf: url)
        },
        expiration: expiration
      )
    case .writing:
      server.registerToken(
        token,
        kind: .write { [weak self] data, _ in
          guard let self else { return }
          try? self.writeDataToFileSync(data: data, object: object, in: bucket)
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

  private func writeDataToFileSync(data: Data, object: Object, in bucket: Bucket) throws {
    let fileURL = fileURL(for: object, in: bucket)
    let bucketURL = bucketURL(for: bucket)

    try FileManager.default.createDirectory(at: bucketURL, withIntermediateDirectories: true)

    let objectDirectory = fileURL.deletingLastPathComponent()
    if objectDirectory != bucketURL {
      try FileManager.default.createDirectory(
        at: objectDirectory, withIntermediateDirectories: true)
    }

    try data.write(to: fileURL)
  }
}
