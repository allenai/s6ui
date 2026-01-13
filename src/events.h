#pragma once

#include <string>
#include <vector>
#include <variant>

// Forward declarations
struct S3Bucket;
struct S3Object;

// Event types that backends emit
enum class EventType {
    BucketsLoaded,
    BucketsLoadError,
    ObjectsLoaded,
    ObjectsLoadError,
    ObjectContentLoaded,
    ObjectContentLoadError,
    ObjectRangeLoaded,
    ObjectRangeLoadError,
};

// Event payload types
struct BucketsLoadedPayload {
    std::vector<S3Bucket> buckets;
};

struct ObjectsLoadedPayload {
    std::string bucket;
    std::string prefix;
    std::string continuation_token;  // For pagination
    std::vector<S3Object> objects;
    std::string next_continuation_token;
    bool is_truncated;
};

struct ErrorPayload {
    std::string bucket;  // Empty for bucket list errors
    std::string prefix;
    std::string error_message;
};

struct ObjectContentLoadedPayload {
    std::string bucket;
    std::string key;
    std::string content;
};

struct ObjectContentErrorPayload {
    std::string bucket;
    std::string key;
    std::string error_message;
};

struct ObjectRangeLoadedPayload {
    std::string bucket;
    std::string key;
    size_t startByte;
    size_t totalSize;  // Total size of the object
    std::string data;
};

struct ObjectRangeErrorPayload {
    std::string bucket;
    std::string key;
    size_t startByte;
    std::string error_message;
};

// A state change event from a backend
struct StateEvent {
    EventType type;
    std::variant<
        BucketsLoadedPayload,
        ObjectsLoadedPayload,
        ErrorPayload,
        ObjectContentLoadedPayload,
        ObjectContentErrorPayload,
        ObjectRangeLoadedPayload,
        ObjectRangeErrorPayload
    > payload;

    // Helper constructors
    static StateEvent bucketsLoaded(std::vector<S3Bucket> buckets) {
        StateEvent e;
        e.type = EventType::BucketsLoaded;
        e.payload = BucketsLoadedPayload{std::move(buckets)};
        return e;
    }

    static StateEvent bucketsError(const std::string& error) {
        StateEvent e;
        e.type = EventType::BucketsLoadError;
        e.payload = ErrorPayload{"", "", error};
        return e;
    }

    static StateEvent objectsLoaded(
        const std::string& bucket,
        const std::string& prefix,
        const std::string& continuation_token,
        std::vector<S3Object> objects,
        const std::string& next_continuation_token,
        bool is_truncated
    ) {
        StateEvent e;
        e.type = EventType::ObjectsLoaded;
        e.payload = ObjectsLoadedPayload{
            bucket, prefix, continuation_token,
            std::move(objects), next_continuation_token, is_truncated
        };
        return e;
    }

    static StateEvent objectsError(
        const std::string& bucket,
        const std::string& prefix,
        const std::string& error
    ) {
        StateEvent e;
        e.type = EventType::ObjectsLoadError;
        e.payload = ErrorPayload{bucket, prefix, error};
        return e;
    }

    static StateEvent objectContentLoaded(
        const std::string& bucket,
        const std::string& key,
        std::string content
    ) {
        StateEvent e;
        e.type = EventType::ObjectContentLoaded;
        e.payload = ObjectContentLoadedPayload{bucket, key, std::move(content)};
        return e;
    }

    static StateEvent objectContentError(
        const std::string& bucket,
        const std::string& key,
        const std::string& error
    ) {
        StateEvent e;
        e.type = EventType::ObjectContentLoadError;
        e.payload = ObjectContentErrorPayload{bucket, key, error};
        return e;
    }

    static StateEvent objectRangeLoaded(
        const std::string& bucket,
        const std::string& key,
        size_t startByte,
        size_t totalSize,
        std::string data
    ) {
        StateEvent e;
        e.type = EventType::ObjectRangeLoaded;
        e.payload = ObjectRangeLoadedPayload{bucket, key, startByte, totalSize, std::move(data)};
        return e;
    }

    static StateEvent objectRangeError(
        const std::string& bucket,
        const std::string& key,
        size_t startByte,
        const std::string& error
    ) {
        StateEvent e;
        e.type = EventType::ObjectRangeLoadError;
        e.payload = ObjectRangeErrorPayload{bucket, key, startByte, error};
        return e;
    }
};
