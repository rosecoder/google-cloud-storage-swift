import AsyncHTTPClient
import Foundation
import GoogleCloudAuth
import NIO
import NIOHTTP1
import ServiceLifecycle
import Synchronization

public final class Storage: Service, StorageProtocol {

  let authorization: Authorization
  let client: HTTPClient

  private let _signingMethod = Mutex<SigningMethod>(.iam)

  public var signingMethod: SigningMethod {
    get { _signingMethod.withLock { $0 } }
    set { _signingMethod.withLock { $0 = newValue } }
  }

  public init() {
    self.authorization = Authorization(
      scopes: [
        "https://www.googleapis.com/auth/cloud-platform",
        "https://www.googleapis.com/auth/iam",
        "https://www.googleapis.com/auth/devstorage.read_write",
      ], eventLoopGroup: .singletonMultiThreadedEventLoopGroup)

    self.client = HTTPClient(eventLoopGroupProvider: .shared(.singletonMultiThreadedEventLoopGroup))
  }

  deinit {
    let client = self.client
    let authorization = self.authorization
    Task.detached {
      try? await client.shutdown()
      try? await authorization.shutdown()
    }
  }

  public func run() async throws {
    await cancelWhenGracefulShutdown {
      while !Task.isCancelled {
        try? await Task.sleep(nanoseconds: .max / 2)
      }
    }

    try await client.shutdown()
    try await authorization.shutdown()
  }

  // MARK: - Requests

  public struct UnparsableRemoteError: Error {

    public let statusCode: UInt
    public let description: String
  }

  public struct NotFoundError: Error {}

  public struct RemoteError: Error, Decodable {

    public let code: Int
    public let message: String

    // MARK: - Decodable

    private enum RootCodingKeys: String, CodingKey {
      case error = "error"
    }

    private enum CodingKeys: String, CodingKey {
      case code = "code"
      case message = "message"
    }

    public init(from decoder: Decoder) throws {
      let rootContainer = try decoder.container(keyedBy: RootCodingKeys.self)
      let container = try rootContainer.nestedContainer(keyedBy: CodingKeys.self, forKey: .error)

      self.code = try container.decode(Int.self, forKey: .code)
      self.message = try container.decode(String.self, forKey: .message)
    }
  }

  func executeData(
    method: HTTPMethod,
    path: String,
    queryItems: [URLQueryItem]? = nil,
    headers: HTTPHeaders? = nil,
    body: HTTPClientRequest.Body? = nil
  ) async throws -> Data {
    var urlComponents = URLComponents(string: "https://storage.googleapis.com" + path)!
    urlComponents.queryItems = queryItems

    var request = HTTPClientRequest(url: urlComponents.string!)
    request.method = method
    if let headers {
      request.headers = headers
    }
    if let body {
      request.body = body
    }

    let accessToken = try await authorization.accessToken()
    request.headers.add(name: "Authorization", value: "Bearer " + accessToken)

    let response = try await client.execute(request, timeout: .seconds(30))

    switch response.status {
    case .ok, .created, .accepted:
      let responseBody = try await response.body.collect(upTo: 100 * 1024 * 1024)  // 100 MB
      return Data(buffer: responseBody)
    case .notFound:
      _ = try? await response.body.collect(upTo: 1024 * 10)  // drain so the connection can be reused
      throw NotFoundError()
    default:
      let responseBody = try await response.body.collect(upTo: 1024 * 10)  // 10 KB

      let remoteError: RemoteError
      do {
        remoteError = try JSONDecoder().decode(RemoteError.self, from: responseBody)
      } catch {
        throw UnparsableRemoteError(
          statusCode: response.status.code, description: String(buffer: responseBody))
      }
      throw remoteError
    }
  }

  func execute<T: Decodable>(
    method: HTTPMethod,
    path: String,
    queryItems: [URLQueryItem]? = nil,
    headers: HTTPHeaders? = nil,
    body: HTTPClientRequest.Body? = nil
  ) async throws -> T {
    var urlComponents = URLComponents(string: "https://storage.googleapis.com" + path)!
    urlComponents.queryItems = queryItems

    var request = HTTPClientRequest(url: urlComponents.string!)
    request.method = method
    if let headers {
      request.headers = headers
    }
    if let body {
      request.body = body
    }

    let accessToken = try await authorization.accessToken()
    request.headers.add(name: "Authorization", value: "Bearer " + accessToken)

    let response = try await client.execute(request, timeout: .seconds(30))

    switch response.status {
    case .ok, .created, .accepted:
      let responseBody = try await response.body.collect(upTo: 1024 * 1024)  // 1 MB
      return try JSONDecoder().decode(T.self, from: responseBody)
    case .notFound:
      _ = try? await response.body.collect(upTo: 1024 * 10)  // drain so the connection can be reused
      throw NotFoundError()
    default:
      let responseBody = try await response.body.collect(upTo: 1024 * 10)  // 10 KB

      let remoteError: RemoteError
      do {
        remoteError = try JSONDecoder().decode(RemoteError.self, from: responseBody)
      } catch {
        throw UnparsableRemoteError(
          statusCode: response.status.code, description: String(buffer: responseBody))
      }
      throw remoteError
    }
  }

  func execute(
    method: HTTPMethod,
    path: String,
    queryItems: [URLQueryItem]? = nil,
    headers: HTTPHeaders? = nil,
    body: HTTPClientRequest.Body? = nil
  ) async throws {
    var urlComponents = URLComponents(string: "https://storage.googleapis.com" + path)!
    urlComponents.queryItems = queryItems

    var request = HTTPClientRequest(url: urlComponents.string!)
    request.method = method
    if let headers {
      request.headers = headers
    }
    if let body {
      request.body = body
    }

    // Authorization
    let accessToken = try await authorization.accessToken()
    request.headers.add(name: "Authorization", value: "Bearer " + accessToken)

    // Perform
    let response = try await client.execute(request, timeout: .seconds(30))

    switch response.status {
    case .ok, .created, .accepted, .noContent:
      return
    case .notFound:
      _ = try? await response.body.collect(upTo: 1024 * 10)  // drain so the connection can be reused
      throw NotFoundError()
    default:
      let body = try await response.body.collect(upTo: 1024 * 10)  // 10 KB

      let remoteError: RemoteError
      do {
        remoteError = try JSONDecoder().decode(RemoteError.self, from: body)
      } catch {
        throw UnparsableRemoteError(
          statusCode: response.status.code, description: String(buffer: body))
      }
      throw remoteError
    }
  }
}
