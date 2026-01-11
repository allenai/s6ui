#pragma once

#include "events.h"
#include <string>
#include <functional>
#include <memory>

// Callback type for pushing events to the model
using EventCallback = std::function<void(StateEvent)>;

// Abstract backend interface
// Implementations are responsible for async execution and pushing events via callback
class IBackend {
public:
    virtual ~IBackend() = default;

    // Set the callback for pushing events (called by BrowserModel)
    virtual void setEventCallback(EventCallback callback) = 0;

    // Request bucket list
    virtual void listBuckets() = 0;

    // Request objects in a bucket/prefix
    // continuation_token is empty for first request, or the token from previous response
    virtual void listObjects(
        const std::string& bucket,
        const std::string& prefix,
        const std::string& continuation_token = ""
    ) = 0;

    // Cancel all pending requests (optional, for cleanup)
    virtual void cancelAll() {}
};

// Factory function type for creating backends
using BackendFactory = std::function<std::unique_ptr<IBackend>()>;
