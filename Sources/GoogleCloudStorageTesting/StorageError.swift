public enum StorageError: Error {
  case unsupportedOperation(String)
  case initializationFailed(String)
  case objectNotFound(String)
}
