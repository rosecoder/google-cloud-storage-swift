import Foundation
import GoogleCloudStorage

extension Bucket {

  static let test = Bucket(
    name: ProcessInfo.processInfo.environment["GOOGLE_CLOUD_STORAGE_TEST_BUCKET"]
      ?? "google-cloud-storage-swift-test-bucket"
  )
}
