import CloudStorage
import Foundation

extension Object {

    static func test(prefix: StaticString = #fileID) -> Object {
        Object(path: "\(prefix)/\(UUID().uuidString)")
    }
}
