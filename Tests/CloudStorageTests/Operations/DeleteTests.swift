import CloudStorage
import Testing

@Suite(.enabledIfAuthenticatedWithGoogleCloud)
struct DeleteTests {

  @Test func deleteObject() async throws {
    let bucket = Bucket.test
    let object = Object.test()

    let storage = Storage()
    let run = Task { try await storage.run() }
    do {
      // Arrange
      try await storage.insert(
        data: "Hello world!".data(using: .utf8)!,
        contentType: "text/plain",
        object: object,
        in: bucket
      )

      // Act
      try await storage.delete(object: object, in: bucket)

      // Assert
      // TODO: Check if object is deleted

      // Cleanup
    } catch {
      run.cancel()
      try await run.value
      throw error
    }
    run.cancel()
    try await run.value
  }
}
