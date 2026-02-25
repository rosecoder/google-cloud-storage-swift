import Foundation
import GoogleCloudStorage
import GoogleCloudStorageTesting
import Testing

@Suite
struct LocalFileSystemStorageTests {

  private let bucket = Bucket(name: "test-bucket")

  @Test func initializationCreatesBaseDirectory() async throws {
    let storage = try LocalFileSystemStorage()

    // Storage should initialize without error
    #expect(storage != nil)
  }

  @Test func insertCreatesFileAndDirectories() async throws {
    let storage = try LocalFileSystemStorage()
    let object = Object(path: "deep/nested/path/file.txt")
    let testData = "Hello, World!".data(using: .utf8)!

    try await storage.insert(
      data: testData, contentType: "text/plain", object: object, in: bucket)

    // Verify file was created by checking if we can generate a URL
    let signedURL = try await storage.generateSignedURL(
      for: .reading,
      expiration: 3600,
      object: object,
      in: bucket
    )

    let fileURL = URL(string: signedURL)!
    #expect(FileManager.default.fileExists(atPath: fileURL.path))

    // Verify content
    let retrievedData = try Data(contentsOf: fileURL)
    #expect(retrievedData == testData)

    // Cleanup
    try await storage.delete(object: object, in: bucket)
  }

  @Test func insertMultipleObjectsInSameBucket() async throws {
    let storage = try LocalFileSystemStorage()
    let object1 = Object(path: "folder1/file1.txt")
    let object2 = Object(path: "folder2/file2.txt")
    let testData1 = "Content 1".data(using: .utf8)!
    let testData2 = "Content 2".data(using: .utf8)!

    try await storage.insert(
      data: testData1, contentType: "text/plain", object: object1, in: bucket)
    try await storage.insert(
      data: testData2, contentType: "text/plain", object: object2, in: bucket)

    // Verify both files exist
    let url1 = try await storage.generateSignedURL(
      for: .reading, expiration: 3600, object: object1, in: bucket)
    let url2 = try await storage.generateSignedURL(
      for: .reading, expiration: 3600, object: object2, in: bucket)

    #expect(FileManager.default.fileExists(atPath: URL(string: url1)!.path))
    #expect(FileManager.default.fileExists(atPath: URL(string: url2)!.path))

    // Cleanup
    try await storage.delete(object: object1, in: bucket)
    try await storage.delete(object: object2, in: bucket)
  }

  @Test func insertOverwritesExistingFile() async throws {
    let storage = try LocalFileSystemStorage()
    let object = Object(path: "overwrite/file.txt")
    let originalData = "Original content".data(using: .utf8)!
    let newData = "New content".data(using: .utf8)!

    // Insert original file
    try await storage.insert(
      data: originalData, contentType: "text/plain", object: object, in: bucket)

    // Overwrite with new content
    try await storage.insert(
      data: newData, contentType: "text/plain", object: object, in: bucket)

    // Verify new content
    let signedURL = try await storage.generateSignedURL(
      for: .reading, expiration: 3600, object: object, in: bucket)
    let fileURL = URL(string: signedURL)!
    let retrievedData = try Data(contentsOf: fileURL)
    #expect(retrievedData == newData)

    // Cleanup
    try await storage.delete(object: object, in: bucket)
  }

  @Test func downloadReturnsInsertedData() async throws {
    let storage = try LocalFileSystemStorage()
    let object = Object(path: "download/file.txt")
    let testData = "Hello, World!".data(using: .utf8)!

    try await storage.insert(data: testData, contentType: "text/plain", object: object, in: bucket)

    let downloadedData = try await storage.download(object: object, in: bucket)
    #expect(downloadedData == testData)

    // Cleanup
    try await storage.delete(object: object, in: bucket)
  }

  @Test func downloadNonexistentObjectThrowsError() async throws {
    let storage = try LocalFileSystemStorage()
    let object = Object(path: "nonexistent/file.txt")

    await #expect(throws: StorageError.self) {
      try await storage.download(object: object, in: bucket)
    }
  }

  @Test func downloadAfterDeleteThrowsError() async throws {
    let storage = try LocalFileSystemStorage()
    let object = Object(path: "download/ephemeral.txt")
    let testData = "temporary".data(using: .utf8)!

    try await storage.insert(data: testData, contentType: "text/plain", object: object, in: bucket)
    try await storage.delete(object: object, in: bucket)

    await #expect(throws: StorageError.self) {
      try await storage.download(object: object, in: bucket)
    }
  }

  @Test func deleteExistingObject() async throws {
    let storage = try LocalFileSystemStorage()
    let object = Object(path: "delete/me.txt")
    let testData = "To be deleted".data(using: .utf8)!

    // First insert the object
    try await storage.insert(
      data: testData, contentType: "text/plain", object: object, in: bucket)

    // Verify it exists
    let signedURL = try await storage.generateSignedURL(
      for: .reading, expiration: 3600, object: object, in: bucket)
    let fileURL = URL(string: signedURL)!
    #expect(FileManager.default.fileExists(atPath: fileURL.path))

    // Delete it
    try await storage.delete(object: object, in: bucket)

    // Verify it's gone
    #expect(!FileManager.default.fileExists(atPath: fileURL.path))
  }

  @Test func deleteNonexistentObjectThrowsError() async throws {
    let storage = try LocalFileSystemStorage()
    let object = Object(path: "nonexistent/file.txt")

    await #expect(throws: StorageError.self) {
      try await storage.delete(object: object, in: bucket)
    }
  }

  @Test func generateSignedURLForReading() async throws {
    let storage = try LocalFileSystemStorage()
    let object = Object(path: "signed/url/test.txt")
    let testData = "URL test content".data(using: .utf8)!

    // Insert file first
    try await storage.insert(
      data: testData, contentType: "text/plain", object: object, in: bucket)

    // Generate signed URL
    let signedURL = try await storage.generateSignedURL(
      for: .reading,
      expiration: 3600,
      object: object,
      in: bucket
    )

    // URL should be a valid file URL
    #expect(signedURL.hasPrefix("file://"))

    let fileURL = URL(string: signedURL)!
    #expect(FileManager.default.fileExists(atPath: fileURL.path))

    // Cleanup
    try await storage.delete(object: object, in: bucket)
  }

  @Test func generateSignedURLForWriting() async throws {
    let storage = try LocalFileSystemStorage()
    let object = Object(path: "write/test.txt")

    // Generate signed URL for writing (should work even if file doesn't exist)
    let signedURL = try await storage.generateSignedURL(
      for: .writing,
      expiration: 3600,
      object: object,
      in: bucket
    )

    // URL should be a valid file URL
    #expect(signedURL.hasPrefix("file://"))

    // The URL should point to where the file would be stored
    let fileURL = URL(string: signedURL)!
    let expectedPath = fileURL.path
    #expect(expectedPath.contains(bucket.name))
    #expect(expectedPath.contains(object.path))
  }

  @Test func insertDifferentContentTypes() async throws {
    let storage = try LocalFileSystemStorage()

    let textObject = Object(path: "content-types/text.txt")
    let jsonObject = Object(path: "content-types/data.json")
    let binaryObject = Object(path: "content-types/image.jpg")

    let textData = "Plain text content".data(using: .utf8)!
    let jsonData = "{\"message\": \"hello world\"}".data(using: .utf8)!
    let binaryData = Data([0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10, 0x4A, 0x46])  // JPEG header

    try await storage.insert(
      data: textData, contentType: "text/plain", object: textObject, in: bucket)
    try await storage.insert(
      data: jsonData, contentType: "application/json", object: jsonObject, in: bucket)
    try await storage.insert(
      data: binaryData, contentType: "image/jpeg", object: binaryObject, in: bucket)

    // Verify all files were created
    let textURL = URL(
      string: try await storage.generateSignedURL(
        for: .reading, expiration: 3600, object: textObject, in: bucket))!
    let jsonURL = URL(
      string: try await storage.generateSignedURL(
        for: .reading, expiration: 3600, object: jsonObject, in: bucket))!
    let binaryURL = URL(
      string: try await storage.generateSignedURL(
        for: .reading, expiration: 3600, object: binaryObject, in: bucket))!

    #expect(FileManager.default.fileExists(atPath: textURL.path))
    #expect(FileManager.default.fileExists(atPath: jsonURL.path))
    #expect(FileManager.default.fileExists(atPath: binaryURL.path))

    // Verify content integrity
    #expect(try Data(contentsOf: textURL) == textData)
    #expect(try Data(contentsOf: jsonURL) == jsonData)
    #expect(try Data(contentsOf: binaryURL) == binaryData)

    // Cleanup
    try await storage.delete(object: textObject, in: bucket)
    try await storage.delete(object: jsonObject, in: bucket)
    try await storage.delete(object: binaryObject, in: bucket)
  }

  @Test func listReturnsEmptyForEmptyBucket() async throws {
    let storage = try LocalFileSystemStorage()
    let emptyBucket = Bucket(name: "empty-list-bucket")

    let objects = try await storage.list(in: emptyBucket)
    #expect(objects.isEmpty)
  }

  @Test func listReturnsInsertedObjects() async throws {
    let storage = try LocalFileSystemStorage()
    let listBucket = Bucket(name: "list-test-bucket")
    let object1 = Object(path: "a/file1.txt")
    let object2 = Object(path: "b/file2.txt")

    try await storage.insert(
      data: Data(), contentType: "text/plain", object: object1, in: listBucket)
    try await storage.insert(
      data: Data(), contentType: "text/plain", object: object2, in: listBucket)

    let listed = try await storage.list(in: listBucket)
    let paths = listed.map(\.path).sorted()

    #expect(paths == ["a/file1.txt", "b/file2.txt"])

    // Cleanup
    try await storage.delete(object: object1, in: listBucket)
    try await storage.delete(object: object2, in: listBucket)
  }

  @Test func listIsIsolatedToBucket() async throws {
    let storage = try LocalFileSystemStorage()
    let bucketA = Bucket(name: "isolated-list-bucket-a")
    let bucketB = Bucket(name: "isolated-list-bucket-b")
    let object = Object(path: "shared/path.txt")

    try await storage.insert(
      data: Data(), contentType: "text/plain", object: object, in: bucketA)

    let listedA = try await storage.list(in: bucketA)
    let listedB = try await storage.list(in: bucketB)

    #expect(listedA.map(\.path) == ["shared/path.txt"])
    #expect(listedB.isEmpty)

    // Cleanup
    try await storage.delete(object: object, in: bucketA)
  }

  @Test func listReflectsDeletions() async throws {
    let storage = try LocalFileSystemStorage()
    let deletionBucket = Bucket(name: "deletion-list-bucket")
    let object = Object(path: "to-delete.txt")

    try await storage.insert(
      data: Data(), contentType: "text/plain", object: object, in: deletionBucket)
    #expect(try await storage.list(in: deletionBucket).count == 1)

    try await storage.delete(object: object, in: deletionBucket)
    #expect(try await storage.list(in: deletionBucket).isEmpty)
  }

  @Test func bucketSeparation() async throws {
    let storage = try LocalFileSystemStorage()
    let bucket1 = Bucket(name: "bucket-1")
    let bucket2 = Bucket(name: "bucket-2")
    let object = Object(path: "same/path.txt")

    let data1 = "Data in bucket 1".data(using: .utf8)!
    let data2 = "Data in bucket 2".data(using: .utf8)!

    // Insert same object path in different buckets
    try await storage.insert(
      data: data1, contentType: "text/plain", object: object, in: bucket1)
    try await storage.insert(
      data: data2, contentType: "text/plain", object: object, in: bucket2)

    // Verify they're stored separately
    let url1 = URL(
      string: try await storage.generateSignedURL(
        for: .reading, expiration: 3600, object: object, in: bucket1))!
    let url2 = URL(
      string: try await storage.generateSignedURL(
        for: .reading, expiration: 3600, object: object, in: bucket2))!

    #expect(url1.path != url2.path)  // Different paths
    #expect(try Data(contentsOf: url1) == data1)
    #expect(try Data(contentsOf: url2) == data2)

    // Cleanup
    try await storage.delete(object: object, in: bucket1)
    try await storage.delete(object: object, in: bucket2)
  }

  @Test func emptyFileHandling() async throws {
    let storage = try LocalFileSystemStorage()
    let object = Object(path: "empty/file.txt")
    let emptyData = Data()

    try await storage.insert(
      data: emptyData, contentType: "text/plain", object: object, in: bucket)

    let signedURL = try await storage.generateSignedURL(
      for: .reading, expiration: 3600, object: object, in: bucket)
    let fileURL = URL(string: signedURL)!

    #expect(FileManager.default.fileExists(atPath: fileURL.path))

    let retrievedData = try Data(contentsOf: fileURL)
    #expect(retrievedData.isEmpty)

    // Cleanup
    try await storage.delete(object: object, in: bucket)
  }
}
