#pragma once

#include <string>
#include <vector>
#include <variant>
#include <cstdint>

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
    // Streaming events
    ObjectStreamStarted,
    ObjectStreamChunk,
    ObjectStreamComplete,
    ObjectStreamError,
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
    int64_t total_size = -1;  // Total file size from Content-Range, -1 if unknown
};

struct ObjectContentErrorPayload {
    std::string bucket;
    std::string key;
    std::string error_message;
};

// Streaming event payloads
struct ObjectStreamStartedPayload {
    std::string bucket;
    std::string key;
    int64_t total_size;      // Total file size, -1 if unknown
};

struct ObjectStreamChunkPayload {
    std::string bucket;
    std::string key;
    size_t bytes_received;   // Total bytes received so far
};

struct ObjectStreamCompletePayload {
    std::string bucket;
    std::string key;
    size_t total_bytes;      // Final total size
};

struct ObjectStreamErrorPayload {
    std::string bucket;
    std::string key;
    std::string error_message;
    size_t bytes_received;   // How much was downloaded before error
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
        ObjectStreamStartedPayload,
        ObjectStreamChunkPayload,
        ObjectStreamCompletePayload,
        ObjectStreamErrorPayload
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
        std::string content,
        int64_t total_size = -1
    ) {
        StateEvent e;
        e.type = EventType::ObjectContentLoaded;
        e.payload = ObjectContentLoadedPayload{bucket, key, std::move(content), total_size};
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

    // Streaming event helpers
    static StateEvent objectStreamStarted(
        const std::string& bucket,
        const std::string& key,
        int64_t total_size
    ) {
        StateEvent e;
        e.type = EventType::ObjectStreamStarted;
        e.payload = ObjectStreamStartedPayload{bucket, key, total_size};
        return e;
    }

    static StateEvent objectStreamChunk(
        const std::string& bucket,
        const std::string& key,
        size_t bytes_received
    ) {
        StateEvent e;
        e.type = EventType::ObjectStreamChunk;
        e.payload = ObjectStreamChunkPayload{bucket, key, bytes_received};
        return e;
    }

    static StateEvent objectStreamComplete(
        const std::string& bucket,
        const std::string& key,
        size_t total_bytes
    ) {
        StateEvent e;
        e.type = EventType::ObjectStreamComplete;
        e.payload = ObjectStreamCompletePayload{bucket, key, total_bytes};
        return e;
    }

    static StateEvent objectStreamError(
        const std::string& bucket,
        const std::string& key,
        const std::string& error,
        size_t bytes_received
    ) {
        StateEvent e;
        e.type = EventType::ObjectStreamError;
        e.payload = ObjectStreamErrorPayload{bucket, key, error, bytes_received};
        return e;
    }
};
