#pragma once

#include "events.h"
#include <string>
#include <vector>
#include <memory>

// Abstract backend interface
// Implementations handle async execution and queue events for the model to poll
class IBackend {
public:
    virtual ~IBackend() = default;

    // Take all pending events (called by model each frame)
    // Returns events and clears the internal queue
    virtual std::vector<StateEvent> takeEvents() = 0;

    // Request bucket list
    virtual void listBuckets() = 0;

    // Request objects in a bucket/prefix
    // continuation_token is empty for first request, or the token from previous response
    virtual void listObjects(
        const std::string& bucket,
        const std::string& prefix,
        const std::string& continuation_token = ""
    ) = 0;

    // Request object content (for preview)
    // max_bytes limits download size (0 = no limit)
    virtual void getObject(
        const std::string& bucket,
        const std::string& key,
        size_t max_bytes = 0
    ) = 0;

    // Cancel all pending requests (optional, for cleanup)
    virtual void cancelAll() {}
};
