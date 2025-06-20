import Foundation
import GoogleCloudAuthTesting
import NIO
import Testing

@testable import CloudStorage

@Suite(.enabledIfAuthenticatedWithGoogleCloud)
struct SigningTests {

  private let object = Object.test()
  private let bucket = Bucket.test

  @Test func signForWrite() async throws {
    let storage = Storage()
    let run = Task { try await storage.run() }

    do {
      // Generate URL for writing
      let urlForWrite = try await storage.generateSignedURL(
        for: .writing, object: object, in: bucket)

      // Upload a plain text file
      var request = URLRequest(url: try #require(URL(string: urlForWrite)))
      request.httpMethod = "PUT"
      request.setValue("text/plain", forHTTPHeaderField: "Content-Type")

      request.httpBody = "Hello world!".data(using: .utf8)!

      let (_, response) = try await URLSession.shared.data(for: request)
      let statusCode = try #require((response as? HTTPURLResponse)?.statusCode)
      #expect((200..<300).contains(statusCode))

      // Generate URL for reading
      let urlForRead = try await storage.generateSignedURL(
        for: .reading,
        object: object,
        in: bucket
      )

      // Assert that uploaded is same as read
      let (data, _) = try await URLSession.shared.data(from: try #require(URL(string: urlForRead)))
      let string = try #require(String(data: data, encoding: .utf8))
      #expect(string == "Hello world!")

      // Cleanup
      try await storage.delete(object: object, in: bucket)
    } catch {
      run.cancel()
      try await run.value
      throw error
    }
    run.cancel()
    try await run.value
    _ = storage
  }
}
