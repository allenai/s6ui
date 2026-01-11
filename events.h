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

// A state change event from a backend
struct StateEvent {
    EventType type;
    std::variant<
        BucketsLoadedPayload,
        ObjectsLoadedPayload,
        ErrorPayload
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
};
