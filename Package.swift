// swift-tools-version:6.0
import PackageDescription

let package = Package(
  name: "google-cloud-storage-swift",
  platforms: [
    .macOS(.v15)
  ],
  products: [
    .library(name: "CloudStorage", targets: ["CloudStorage"]),
    .library(name: "CloudStorageTesting", targets: ["CloudStorageTesting"]),
  ],
  dependencies: [
    .package(url: "https://github.com/apple/swift-crypto.git", from: "3.3.0"),
    .package(url: "https://github.com/apple/swift-distributed-tracing.git", from: "1.1.0"),
    .package(url: "https://github.com/apple/swift-log.git", from: "1.4.2"),
    .package(url: "https://github.com/rosecoder/google-cloud-auth-swift.git", from: "1.2.0"),
    .package(url: "https://github.com/rosecoder/google-cloud-service-context.git", from: "0.0.2"),
    .package(url: "https://github.com/swift-server/async-http-client.git", from: "1.10.0"),
  ],
  targets: [
    .target(
      name: "CloudStorage",
      dependencies: [
        .product(name: "AsyncHTTPClient", package: "async-http-client"),
        .product(name: "Crypto", package: "swift-crypto"),
        .product(name: "Logging", package: "swift-log"),
        .product(name: "Tracing", package: "swift-distributed-tracing"),
        .product(name: "GoogleCloudServiceContext", package: "google-cloud-service-context"),
        .product(name: "GoogleCloudAuth", package: "google-cloud-auth-swift"),
      ]
    ),
    .testTarget(name: "CloudStorageTests", dependencies: ["CloudStorage"]),

    .target(
      name: "CloudStorageTesting",
      dependencies: [
        "CloudStorage"
      ]
    ),
  ]
)
