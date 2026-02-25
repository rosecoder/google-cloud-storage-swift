import GoogleCloudStorage
import Testing

@Suite(.enabledIfAuthenticatedWithGoogleCloud)
struct DownloadTests {

  @Test func downloadSimpleTextObject() async throws {
    let bucket = Bucket.test
    let object = Object.test()
    let originalData = "Hello world!".data(using: .utf8)!

    let storage = Storage()
    let run = Task { try await storage.run() }
    do {
      // Arrange
      try await storage.insert(
        data: originalData,
        contentType: "text/plain",
        object: object,
        in: bucket
      )

      // Act
      let downloadedData = try await storage.download(object: object, in: bucket)

      // Assert
      #expect(downloadedData == originalData)

      // Cleanup
      try await storage.delete(object: object, in: bucket)
    } catch {
      run.cancel()
      try await run.value
      throw error
    }
    run.cancel()
    try await run.value
  }

  @Test func downloadNonexistentObjectThrowsNotFound() async throws {
    let bucket = Bucket.test
    let object = Object.test()

    let storage = Storage()
    let run = Task { try await storage.run() }
    await #expect(throws: Storage.NotFoundError.self) {
      try await storage.download(object: object, in: bucket)
    }
    run.cancel()
    try await run.value
  }
}
