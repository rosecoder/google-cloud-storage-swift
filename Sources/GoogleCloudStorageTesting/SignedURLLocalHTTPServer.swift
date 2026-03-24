import Foundation
import GoogleCloudStorage
import NIOCore
import NIOHTTP1
import NIOPosix
import Synchronization

/// Local HTTP server for one-shot signed-URL style requests in tests. `internal` for reuse within this module.
internal final class SignedURLLocalHTTPServer: Sendable {

  enum TokenKind: Sendable {
    /// `GET` — returns optional body (`nil` → 404).
    case read(@Sendable () -> Data?)
    /// `PUT` — receives body and `Content-Type` header value (or `application/octet-stream`).
    case write(@Sendable (Data, String) -> Void)
  }

  private struct TokenEntry {
    let expiresAt: Date
    let kind: TokenKind
  }

  /// Reference type so the NIO pipeline can share the table without copying a noncopyable `Mutex`.
  private final class TokenRegistry: Sendable {
    private let lock = NSLock()
    private let map = Mutex<[String: TokenEntry]>([:])

    func get(_ token: String) -> TokenEntry? {
      lock.lock()
      defer { lock.unlock() }
      return map.withLock { $0[token] }
    }

    func set(_ token: String, entry: TokenEntry) {
      lock.lock()
      defer { lock.unlock() }
      map.withLock { $0[token] = entry }
    }

    func remove(_ token: String) {
      lock.lock()
      defer { lock.unlock() }
      map.withLock { _ = $0.removeValue(forKey: token) }
    }
  }

  private let tokens = TokenRegistry()
  private let group: MultiThreadedEventLoopGroup
  private let bindQueue = DispatchQueue(label: "SignedURLLocalHTTPServer.bind")
  private let bindState = Mutex<BindState?>(nil)
  private enum BindState {
    case bound(baseURL: String)
  }

  private let shutdownState = Mutex<Bool>(false)

  init() {
    self.group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
  }

  deinit {
    shutdown()
  }

  /// Starts the listener if needed. Serialized on `bindQueue` so concurrent callers share one bind.
  func ensureStarted() async throws -> String {
    try await withCheckedThrowingContinuation { continuation in
      bindQueue.async {
        let alreadyShutDown = self.shutdownState.withLock { $0 }
        if alreadyShutDown {
          continuation.resume(
            throwing: StorageError.initializationFailed("Signed URL server has been shut down"))
          return
        }

        if let state = self.bindState.withLock({ $0 }), case .bound(let url) = state {
          continuation.resume(returning: url)
          return
        }

        let bootstrap = ServerBootstrap(group: self.group)
          .serverChannelOption(.backlog, value: 256)
          .serverChannelOption(.socketOption(.so_reuseaddr), value: 1)
          .childChannelOption(.socketOption(.so_reuseaddr), value: 1)
          .childChannelInitializer { channel in
            channel.pipeline.configureHTTPServerPipeline(withErrorHandling: true).flatMap { _ in
              channel.pipeline.addHandler(HTTPHandler(tokens: self.tokens))
            }
          }

        do {
          let channel = try bootstrap.bind(host: "127.0.0.1", port: 0).wait()
          guard let port = channel.localAddress?.port else {
            continuation.resume(
              throwing: StorageError.initializationFailed("Could not read bound port"))
            _ = channel.close(mode: .all)
            return
          }
          let baseURL = "http://127.0.0.1:\(port)"
          self.bindState.withLock { $0 = .bound(baseURL: baseURL) }
          continuation.resume(returning: baseURL)
        } catch {
          continuation.resume(throwing: error)
        }
      }
    }
  }

  func registerToken(
    _ token: String,
    kind: TokenKind,
    expiration: TimeInterval
  ) {
    let expiresAt = Date().addingTimeInterval(expiration)
    tokens.set(token, entry: TokenEntry(expiresAt: expiresAt, kind: kind))

    let sleepNs = UInt64(max(expiration, 0) * 1_000_000_000)
    Task { [tokens] in
      try? await Task.sleep(nanoseconds: sleepNs)
      tokens.remove(token)
    }
  }

  /// Synchronous teardown for `InMemoryStorage.deinit` (NIO has no async `deinit`). Idempotent.
  func shutdown() {
    let shouldShutdown = shutdownState.withLock { done in
      if done { return false }
      done = true
      return true
    }
    guard shouldShutdown else { return }
    try? group.syncShutdownGracefully()
  }

  // MARK: - HTTP handler

  private final class HTTPHandler: ChannelInboundHandler, @unchecked Sendable {
    typealias InboundIn = HTTPServerRequestPart
    typealias OutboundOut = HTTPServerResponsePart

    private var head: HTTPRequestHead?
    private var body: ByteBuffer?
    private let tokens: TokenRegistry

    init(tokens: TokenRegistry) {
      self.tokens = tokens
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
      let part = unwrapInboundIn(data)
      switch part {
      case .head(let h):
        head = h
        body = context.channel.allocator.buffer(capacity: 0)
      case .body(var buffer):
        if body == nil {
          body = context.channel.allocator.buffer(capacity: buffer.readableBytes)
        }
        body?.writeBuffer(&buffer)
      case .end:
        respond(context: context)
      }
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
      send(
        context: context, status: .internalServerError, body: nil,
        extraHeaders: [("Connection", "close")])
      context.close(promise: nil)
    }

    private func respond(context: ChannelHandlerContext) {
      defer {
        head = nil
        body = nil
      }

      guard let head else {
        send(
          context: context, status: .badRequest, body: nil, extraHeaders: [("Connection", "close")])
        context.close(promise: nil)
        return
      }

      let token = pathToken(from: head.uri)
      guard let token else {
        send(
          context: context, status: .notFound, body: nil, extraHeaders: [("Connection", "close")])
        context.close(promise: nil)
        return
      }

      guard let entry = tokens.get(token) else {
        send(
          context: context, status: .notFound, body: nil, extraHeaders: [("Connection", "close")])
        context.close(promise: nil)
        return
      }

      if Date() > entry.expiresAt {
        tokens.remove(token)
        send(
          context: context, status: .gone, body: nil, extraHeaders: [("Connection", "close")])
        context.close(promise: nil)
        return
      }

      switch entry.kind {
      case .read(let read):
        guard head.method == .GET else {
          send(
            context: context, status: .methodNotAllowed, body: nil,
            extraHeaders: [("Connection", "close"), ("Allow", "GET")])
          context.close(promise: nil)
          return
        }
        guard let data = read() else {
          tokens.remove(token)
          send(
            context: context, status: .notFound, body: nil, extraHeaders: [("Connection", "close")])
          context.close(promise: nil)
          return
        }
        tokens.remove(token)
        send(
          context: context, status: .ok, body: data, extraHeaders: [("Connection", "close")])
        context.close(promise: nil)

      case .write(let write):
        guard head.method == .PUT else {
          send(
            context: context, status: .methodNotAllowed, body: nil,
            extraHeaders: [("Connection", "close"), ("Allow", "PUT")])
          context.close(promise: nil)
          return
        }
        guard let contentLengthString = head.headers.first(name: "content-length"),
          let contentLength = Int(contentLengthString)
        else {
          send(
            context: context, status: .lengthRequired, body: nil,
            extraHeaders: [("Connection", "close")])
          context.close(promise: nil)
          return
        }
        guard var buf = body, buf.readableBytes == contentLength else {
          send(
            context: context, status: .badRequest, body: nil,
            extraHeaders: [("Connection", "close")])
          context.close(promise: nil)
          return
        }
        guard let bytes = buf.readBytes(length: contentLength) else {
          send(
            context: context, status: .badRequest, body: nil,
            extraHeaders: [("Connection", "close")])
          context.close(promise: nil)
          return
        }
        let putData = Data(bytes)
        let contentType =
          head.headers.first(name: "Content-Type") ?? "application/octet-stream"
        write(putData, contentType)
        tokens.remove(token)
        send(
          context: context, status: .noContent, body: nil, extraHeaders: [("Connection", "close")])
        context.close(promise: nil)
      }
    }

    private func pathToken(from uri: String) -> String? {
      let path =
        uri.split(separator: "?", maxSplits: 1, omittingEmptySubsequences: false).first.map(
          String.init)
        ?? uri
      guard path.hasPrefix("/") else { return nil }
      let trimmed = path.dropFirst()
      guard !trimmed.isEmpty else { return nil }
      let segment = trimmed.split(separator: "/", maxSplits: 1, omittingEmptySubsequences: false)
        .first.map(String.init)
      return segment
    }

    private func send(
      context: ChannelHandlerContext,
      status: HTTPResponseStatus,
      body: Data?,
      extraHeaders: [(String, String)]
    ) {
      var headers = HTTPHeaders()
      for (n, v) in extraHeaders {
        headers.add(name: n, value: v)
      }
      if let body {
        headers.add(name: "Content-Length", value: String(body.count))
        context.write(
          wrapOutboundOut(
            .head(HTTPResponseHead(version: .http1_1, status: status, headers: headers))),
          promise: nil)
        var buffer = context.channel.allocator.buffer(capacity: body.count)
        buffer.writeBytes(body)
        context.write(wrapOutboundOut(.body(.byteBuffer(buffer))), promise: nil)
        context.writeAndFlush(wrapOutboundOut(.end(nil)), promise: nil)
      } else {
        headers.add(name: "Content-Length", value: "0")
        context.write(
          wrapOutboundOut(
            .head(HTTPResponseHead(version: .http1_1, status: status, headers: headers))),
          promise: nil)
        context.writeAndFlush(wrapOutboundOut(.end(nil)), promise: nil)
      }
    }
  }
}
