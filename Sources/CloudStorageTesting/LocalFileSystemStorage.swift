import CloudStorage
import Foundation

public final class LocalFileSystemStorage: StorageProtocol {

    private let baseURL: URL

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
        let fileURL = fileURL(for: object, in: bucket)
        let bucketURL = bucketURL(for: bucket)

        // Create bucket directory if it doesn't exist
        try FileManager.default.createDirectory(at: bucketURL, withIntermediateDirectories: true)

        // Create object directory structure if needed
        let objectDirectory = fileURL.deletingLastPathComponent()
        if objectDirectory != bucketURL {
            try FileManager.default.createDirectory(
                at: objectDirectory, withIntermediateDirectories: true)
        }

        // Write the data to the file
        try data.write(to: fileURL)
    }

    public func delete(object: Object, in bucket: Bucket) async throws {
        let fileURL = fileURL(for: object, in: bucket)

        guard FileManager.default.fileExists(atPath: fileURL.path) else {
            throw StorageError.objectNotFound(
                "Object \(object.path) not found in bucket \(bucket.name)")
        }

        try FileManager.default.removeItem(at: fileURL)
    }

    public func generateSignedURL(
        for action: SignedAction,
        expiration: TimeInterval,
        object: Object,
        in bucket: Bucket
    ) async throws -> String {
        fileURL(for: object, in: bucket).absoluteString
    }
}
