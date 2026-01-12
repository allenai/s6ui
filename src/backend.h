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
    // lowPriority = true for background prefetch, false for user-initiated requests
    // cancellable = true means this request can be cancelled by newer hover prefetches
    virtual void getObject(
        const std::string& bucket,
        const std::string& key,
        size_t max_bytes = 0,
        bool lowPriority = false,
        bool cancellable = false
    ) = 0;

    // Cancel all pending requests (optional, for cleanup)
    virtual void cancelAll() {}

    // Prefetch support - queue a low-priority background request
    // Used to preload subfolders for faster navigation
    // cancellable = true means this request can be cancelled by newer hover prefetches
    virtual void listObjectsPrefetch(
        const std::string& bucket,
        const std::string& prefix,
        bool cancellable = false
    ) = 0;

    // Boost a pending request to high priority (returns true if found and boosted)
    // Called when user navigates to a prefix that's already being prefetched
    virtual bool prioritizeRequest(
        const std::string& bucket,
        const std::string& prefix
    ) = 0;

    // Check if there's already a pending request for this bucket/prefix
    virtual bool hasPendingRequest(
        const std::string& bucket,
        const std::string& prefix
    ) const = 0;

    // Check if there's already a pending object fetch request
    virtual bool hasPendingObjectRequest(
        const std::string& bucket,
        const std::string& key
    ) const = 0;

    // Boost a pending object request to high priority (returns true if found)
    virtual bool prioritizeObjectRequest(
        const std::string& bucket,
        const std::string& key
    ) = 0;
};
