import Foundation
import GoogleCloudStorage
import GoogleCloudStorageTesting
import Testing

@Suite
struct LocalFileSystemStorageTests {

  private let bucket = Bucket(name: "test-bucket")

  @Test func initializationCreatesBaseDirectory() async throws {
    _ = try LocalFileSystemStorage()

    let base = FileManager.default.homeDirectoryForCurrentUser.appendingPathComponent(
      ".google-cloud-storage")
    #expect(FileManager.default.fileExists(atPath: base.path))
  }

  @Test func insertCreatesFileAndDirectories() async throws {
    let storage = try LocalFileSystemStorage()
    let object = Object(path: "deep/nested/path/file.txt")
    let testData = "Hello, World!".data(using: .utf8)!

    try await storage.insert(
      data: testData, contentType: "text/plain", object: object, in: bucket)

    let signedURL = try await storage.generateSignedURL(
      for: .reading,
      expiration: 3600,
      object: object,
      in: bucket
    )
    #expect(signedURL.hasPrefix("http://127.0.0.1:"))

    let retrievedData = try await storage.download(object: object, in: bucket)
    #expect(retrievedData == testData)

    let (data, response) = try await URLSession.shared.data(
      from: try #require(URL(string: signedURL)))
    let status = try #require((response as? HTTPURLResponse)?.statusCode)
    #expect(status == 200)
    #expect(data == testData)

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

    #expect(try await storage.download(object: object1, in: bucket) == testData1)
    #expect(try await storage.download(object: object2, in: bucket) == testData2)

    try await storage.delete(object: object1, in: bucket)
    try await storage.delete(object: object2, in: bucket)
  }

  @Test func insertOverwritesExistingFile() async throws {
    let storage = try LocalFileSystemStorage()
    let object = Object(path: "overwrite/file.txt")
    let originalData = "Original content".data(using: .utf8)!
    let newData = "New content".data(using: .utf8)!

    try await storage.insert(
      data: originalData, contentType: "text/plain", object: object, in: bucket)

    try await storage.insert(
      data: newData, contentType: "text/plain", object: object, in: bucket)

    let retrievedData = try await storage.download(object: object, in: bucket)
    #expect(retrievedData == newData)

    try await storage.delete(object: object, in: bucket)
  }

  @Test func downloadReturnsInsertedData() async throws {
    let storage = try LocalFileSystemStorage()
    let object = Object(path: "download/file.txt")
    let testData = "Hello, World!".data(using: .utf8)!

    try await storage.insert(data: testData, contentType: "text/plain", object: object, in: bucket)

    let downloadedData = try await storage.download(object: object, in: bucket)
    #expect(downloadedData == testData)

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

    try await storage.insert(
      data: testData, contentType: "text/plain", object: object, in: bucket)

    #expect(try await storage.download(object: object, in: bucket) == testData)

    try await storage.delete(object: object, in: bucket)

    await #expect(throws: StorageError.self) {
      try await storage.download(object: object, in: bucket)
    }
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

    try await storage.insert(
      data: testData, contentType: "text/plain", object: object, in: bucket)

    let signedURL = try await storage.generateSignedURL(
      for: .reading,
      expiration: 3600,
      object: object,
      in: bucket
    )

    #expect(signedURL.hasPrefix("http://127.0.0.1:"))

    let (data, response) = try await URLSession.shared.data(
      from: try #require(URL(string: signedURL)))
    let status = try #require((response as? HTTPURLResponse)?.statusCode)
    #expect(status == 200)
    #expect(data == testData)

    try await storage.delete(object: object, in: bucket)
  }

  @Test func generateSignedURLForWriting() async throws {
    let storage = try LocalFileSystemStorage()
    let object = Object(path: "write/test.txt")
    let payload = "written-via-signed-put".data(using: .utf8)!

    let signedURL = try await storage.generateSignedURL(
      for: .writing,
      expiration: 3600,
      object: object,
      in: bucket
    )

    #expect(signedURL.hasPrefix("http://127.0.0.1:"))

    var request = URLRequest(url: try #require(URL(string: signedURL)))
    request.httpMethod = "PUT"
    request.setValue("text/plain", forHTTPHeaderField: "Content-Type")
    request.httpBody = payload

    let (_, response) = try await URLSession.shared.data(for: request)
    let status = try #require((response as? HTTPURLResponse)?.statusCode)
    #expect((200..<300).contains(status))

    #expect(try await storage.download(object: object, in: bucket) == payload)

    try await storage.delete(object: object, in: bucket)
  }

  @Test func generateSignedURLForWritingUploadsViaHTTP() async throws {
    let storage = try LocalFileSystemStorage()
    let object = Object(path: "signed/write/via-http.txt")
    let payload = "uploaded-by-put".data(using: .utf8)!

    let signedURL = try await storage.generateSignedURL(
      for: .writing,
      expiration: 30,
      object: object,
      in: bucket
    )

    #expect(signedURL.hasPrefix("http://127.0.0.1:"))

    var request = URLRequest(url: try #require(URL(string: signedURL)))
    request.httpMethod = "PUT"
    request.setValue("text/plain", forHTTPHeaderField: "Content-Type")
    request.httpBody = payload

    let (_, response) = try await URLSession.shared.data(for: request)
    let status = try #require((response as? HTTPURLResponse)?.statusCode)
    #expect((200..<300).contains(status))

    let downloaded = try await storage.download(object: object, in: bucket)
    #expect(downloaded == payload)

    try await storage.delete(object: object, in: bucket)
  }

  @Test func generateSignedURLForReadingServesObjectViaHTTP() async throws {
    let storage = try LocalFileSystemStorage()
    let object = Object(path: "signed/read/via-http.txt")
    let payload = "already-on-disk".data(using: .utf8)!

    try await storage.insert(
      data: payload, contentType: "text/plain", object: object, in: bucket)

    let signedURL = try await storage.generateSignedURL(
      for: .reading,
      expiration: 30,
      object: object,
      in: bucket
    )

    #expect(signedURL.hasPrefix("http://127.0.0.1:"))

    let (data, response) = try await URLSession.shared.data(
      from: try #require(URL(string: signedURL)))
    let status = try #require((response as? HTTPURLResponse)?.statusCode)
    #expect(status == 200)
    #expect(data == payload)

    try await storage.delete(object: object, in: bucket)
  }

  @Test func concurrentSignedURLWritesDoNotConflict() async throws {
    let storage = try LocalFileSystemStorage()
    let count = 32
    var urls: [(Int, String)] = []
    for index in 0..<count {
      let object = Object(path: "concurrent/signed/\(index).txt")
      let url = try await storage.generateSignedURL(
        for: .writing,
        expiration: 60,
        object: object,
        in: bucket
      )
      urls.append((index, url))
    }

    let statuses = try await withThrowingTaskGroup(of: Int.self) { group in
      for (index, urlString) in urls {
        group.addTask {
          let body = "payload-\(index)".data(using: .utf8)!
          var request = URLRequest(url: try #require(URL(string: urlString)))
          request.httpMethod = "PUT"
          request.setValue("text/plain", forHTTPHeaderField: "Content-Type")
          request.httpBody = body
          let (_, response) = try await URLSession.shared.data(for: request)
          return try #require((response as? HTTPURLResponse)?.statusCode)
        }
      }
      var codes: [Int] = []
      for try await status in group {
        codes.append(status)
      }
      return codes
    }

    for status in statuses {
      #expect((200..<300).contains(status))
    }

    for index in 0..<count {
      let object = Object(path: "concurrent/signed/\(index).txt")
      let expected = "payload-\(index)".data(using: .utf8)!
      let got = try await storage.download(object: object, in: bucket)
      #expect(got == expected)
      try await storage.delete(object: object, in: bucket)
    }
  }

  @Test func separateLocalFileSystemStorageInstancesUseDistinctPorts() async throws {
    let storageA = try LocalFileSystemStorage()
    let storageB = try LocalFileSystemStorage()
    let object = Object(path: "port-check.txt")

    let urlA = try await storageA.generateSignedURL(
      for: .writing, expiration: 30, object: object, in: bucket)
    let urlB = try await storageB.generateSignedURL(
      for: .writing, expiration: 30, object: object, in: bucket)

    #expect(urlA != urlB)
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

    #expect(try await storage.download(object: textObject, in: bucket) == textData)
    #expect(try await storage.download(object: jsonObject, in: bucket) == jsonData)
    #expect(try await storage.download(object: binaryObject, in: bucket) == binaryData)

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

    try await storage.insert(
      data: data1, contentType: "text/plain", object: object, in: bucket1)
    try await storage.insert(
      data: data2, contentType: "text/plain", object: object, in: bucket2)

    #expect(try await storage.download(object: object, in: bucket1) == data1)
    #expect(try await storage.download(object: object, in: bucket2) == data2)

    try await storage.delete(object: object, in: bucket1)
    try await storage.delete(object: object, in: bucket2)
  }

  @Test func emptyFileHandling() async throws {
    let storage = try LocalFileSystemStorage()
    let object = Object(path: "empty/file.txt")
    let emptyData = Data()

    try await storage.insert(
      data: emptyData, contentType: "text/plain", object: object, in: bucket)

    let retrievedData = try await storage.download(object: object, in: bucket)
    #expect(retrievedData.isEmpty)

    try await storage.delete(object: object, in: bucket)
  }
}
