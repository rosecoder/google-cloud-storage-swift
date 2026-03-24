import Foundation
import GoogleCloudStorage
import GoogleCloudStorageTesting
import Testing

@Suite
struct InMemoryStorageTests {

  private let storage = InMemoryStorage()
  private let bucket = Bucket(name: "test-bucket")

  @Test func insertAndRetrieveObject() async throws {
    let object = Object(path: "test/file.txt")
    let testData = "Hello, World!".data(using: .utf8)!

    // Insert data
    storage.insert(data: testData, contentType: "text/plain", object: object, in: bucket)

    // Verify the data was stored by checking internal state
    // Since we can't directly retrieve, we test through the key mechanism
  }

  @Test func insertMultipleObjects() async throws {
    let object1 = Object(path: "folder1/file1.txt")
    let object2 = Object(path: "folder2/file2.txt")
    let testData1 = "Content 1".data(using: .utf8)!
    let testData2 = "Content 2".data(using: .utf8)!

    // Insert multiple objects
    storage.insert(data: testData1, contentType: "text/plain", object: object1, in: bucket)
    storage.insert(data: testData2, contentType: "text/plain", object: object2, in: bucket)

    // Both operations should complete without error
  }

  @Test func downloadReturnsInsertedData() async throws {
    let object = Object(path: "test/file.txt")
    let testData = "Hello, World!".data(using: .utf8)!

    storage.insert(data: testData, contentType: "text/plain", object: object, in: bucket)

    let downloadedData = try await storage.download(object: object, in: bucket)
    #expect(downloadedData == testData)
  }

  @Test func downloadNonexistentObjectThrowsError() async throws {
    let object = Object(path: "nonexistent/file.txt")

    await #expect(throws: StorageError.self) {
      try await storage.download(object: object, in: bucket)
    }
  }

  @Test func downloadAfterDeleteThrowsError() async throws {
    let object = Object(path: "test/ephemeral.txt")
    let testData = "temporary".data(using: .utf8)!

    storage.insert(data: testData, contentType: "text/plain", object: object, in: bucket)
    storage.delete(object: object, in: bucket)

    await #expect(throws: StorageError.self) {
      try await storage.download(object: object, in: bucket)
    }
  }

  @Test func deleteObject() async throws {
    let object = Object(path: "test/delete-me.txt")
    let testData = "To be deleted".data(using: .utf8)!

    // First insert the object
    storage.insert(data: testData, contentType: "text/plain", object: object, in: bucket)

    // Then delete it
    storage.delete(object: object, in: bucket)

    // Delete should complete without error
  }

  @Test func deleteNonexistentObject() async throws {
    let object = Object(path: "nonexistent/file.txt")

    // Deleting a non-existent object should not throw
    storage.delete(object: object, in: bucket)
  }

  @Test func generateSignedURLForWritingUploadsViaHTTP() async throws {
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

    storage.delete(object: object, in: bucket)
  }

  @Test func generateSignedURLForReadingServesObjectViaHTTP() async throws {
    let object = Object(path: "signed/read/via-http.txt")
    let payload = "already-in-memory".data(using: .utf8)!
    storage.insert(data: payload, contentType: "text/plain", object: object, in: bucket)

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

    storage.delete(object: object, in: bucket)
  }

  @Test func concurrentSignedURLWritesDoNotConflict() async throws {
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
      storage.delete(object: object, in: bucket)
    }
  }

  @Test func separateInMemoryStorageInstancesUseDistinctPorts() async throws {
    let storageA = InMemoryStorage()
    let storageB = InMemoryStorage()
    let object = Object(path: "port-check.txt")

    let urlA = try await storageA.generateSignedURL(
      for: .writing, expiration: 30, object: object, in: bucket)
    let urlB = try await storageB.generateSignedURL(
      for: .writing, expiration: 30, object: object, in: bucket)

    #expect(urlA != urlB)
  }

  @Test func threadSafetyTest() async throws {
    let numberOfOperations = 100
    let objects = (0..<numberOfOperations).map { Object(path: "concurrent/file\($0).txt") }

    // Perform concurrent insert operations
    await withTaskGroup(of: Void.self) { group in
      for (index, object) in objects.enumerated() {
        group.addTask {
          let data = "Data \(index)".data(using: .utf8)!
          self.storage.insert(
            data: data, contentType: "text/plain", object: object, in: self.bucket)
        }
      }
    }

    // Perform concurrent delete operations
    await withTaskGroup(of: Void.self) { group in
      for object in objects {
        group.addTask {
          self.storage.delete(object: object, in: self.bucket)
        }
      }
    }

    // All operations should complete without data races or crashes
  }

  @Test func listReturnsEmptyForEmptyBucket() async throws {
    let emptyBucket = Bucket(name: "empty-bucket")
    let objects = try await storage.list(in: emptyBucket)
    #expect(objects.isEmpty)
  }

  @Test func listReturnsInsertedObjects() async throws {
    let listBucket = Bucket(name: "list-bucket")
    let object1 = Object(path: "a/file1.txt")
    let object2 = Object(path: "b/file2.txt")

    storage.insert(data: Data(), contentType: "text/plain", object: object1, in: listBucket)
    storage.insert(data: Data(), contentType: "text/plain", object: object2, in: listBucket)

    let listed = try await storage.list(in: listBucket)
    let paths = listed.map(\.path).sorted()

    #expect(paths == ["a/file1.txt", "b/file2.txt"])
  }

  @Test func listIsIsolatedToBucket() async throws {
    let bucketA = Bucket(name: "isolated-bucket-a")
    let bucketB = Bucket(name: "isolated-bucket-b")
    let object = Object(path: "shared/path.txt")

    storage.insert(data: Data(), contentType: "text/plain", object: object, in: bucketA)

    let listedA = try await storage.list(in: bucketA)
    let listedB = try await storage.list(in: bucketB)

    #expect(listedA.map(\.path) == ["shared/path.txt"])
    #expect(listedB.isEmpty)
  }

  @Test func listReflectsDeletions() async throws {
    let deletionBucket = Bucket(name: "deletion-bucket")
    let object = Object(path: "to-delete.txt")

    storage.insert(data: Data(), contentType: "text/plain", object: object, in: deletionBucket)
    #expect(try await storage.list(in: deletionBucket).count == 1)

    storage.delete(object: object, in: deletionBucket)
    #expect(try await storage.list(in: deletionBucket).isEmpty)
  }

  @Test func keyGenerationConsistency() async throws {
    let bucket1 = Bucket(name: "bucket1")
    let bucket2 = Bucket(name: "bucket2")
    let object = Object(path: "same/path.txt")
    let testData = "Test data".data(using: .utf8)!

    // Same object path in different buckets should be stored separately
    storage.insert(data: testData, contentType: "text/plain", object: object, in: bucket1)
    storage.insert(data: testData, contentType: "text/plain", object: object, in: bucket2)

    // Both should be stored without interference
  }

  @Test func insertWithDifferentContentTypes() async throws {
    let textObject = Object(path: "files/text.txt")
    let jsonObject = Object(path: "files/data.json")
    let binaryObject = Object(path: "files/image.jpg")

    let textData = "Plain text".data(using: .utf8)!
    let jsonData = "{\"key\": \"value\"}".data(using: .utf8)!
    let binaryData = Data([0xFF, 0xD8, 0xFF, 0xE0])  // JPEG header

    storage.insert(data: textData, contentType: "text/plain", object: textObject, in: bucket)
    storage.insert(
      data: jsonData, contentType: "application/json", object: jsonObject, in: bucket)
    storage.insert(
      data: binaryData, contentType: "image/jpeg", object: binaryObject, in: bucket)

    // All insertions should succeed regardless of content type
  }
}
