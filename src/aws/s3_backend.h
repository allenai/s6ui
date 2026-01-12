#pragma once

#include "../backend.h"
#include "aws_credentials.h"
#include <string>
#include <vector>
#include <map>
#include <mutex>
#include <thread>
#include <atomic>
#include <deque>
#include <condition_variable>
#include <chrono>

// S3 data types (moved from s3_client.h)
struct S3Bucket {
    std::string name;
    std::string creation_date;
};

struct S3Object {
    std::string key;
    std::string display_name;
    int64_t size = 0;
    std::string last_modified;
    bool is_folder = false;
};

// S3 backend implementation
class S3Backend : public IBackend {
public:
    explicit S3Backend(const AWSProfile& profile, size_t numWorkers = 5);
    ~S3Backend() override;

    std::vector<StateEvent> takeEvents() override;
    void listBuckets() override;
    void listObjects(
        const std::string& bucket,
        const std::string& prefix,
        const std::string& continuation_token = "",
        std::shared_ptr<std::atomic<bool>> cancel_flag = nullptr
    ) override;
    void getObject(
        const std::string& bucket,
        const std::string& key,
        size_t max_bytes = 0,
        bool lowPriority = false,
        bool cancellable = false
    ) override;
    void cancelAll() override;

    // Prefetch support - low priority background requests
    void listObjectsPrefetch(
        const std::string& bucket,
        const std::string& prefix,
        bool cancellable = false
    ) override;

    // Boost a pending request to high priority (returns true if found and boosted)
    bool prioritizeRequest(
        const std::string& bucket,
        const std::string& prefix
    ) override;

    // Check if a request is already pending for this bucket/prefix
    bool hasPendingRequest(
        const std::string& bucket,
        const std::string& prefix
    ) const override;

    // Check if there's already a pending object fetch request
    bool hasPendingObjectRequest(
        const std::string& bucket,
        const std::string& key
    ) const override;

    // Boost a pending object request to high priority
    bool prioritizeObjectRequest(
        const std::string& bucket,
        const std::string& key
    ) override;

    // Change the active profile
    void setProfile(const AWSProfile& profile);

private:
    // Work item for the background thread
    struct WorkItem {
        enum class Type { ListBuckets, ListObjects, GetObject, Shutdown };
        enum class Priority { High, Low };  // High = user action, Low = prefetch
        Type type;
        Priority priority = Priority::High;
        std::string bucket;
        std::string prefix;
        std::string continuation_token;
        std::string key;  // For GetObject
        size_t max_bytes = 0;  // For GetObject
        std::chrono::steady_clock::time_point queued_at;
        std::shared_ptr<std::atomic<bool>> cancel_flag;  // Shared flag to cancel this request
    };

    void workerThread(WorkItem::Priority priority, size_t workerIndex);
    void processWorkItem(WorkItem& item);
    void enqueue(WorkItem item);

    // Helper methods for queue operations with predicates
    template<typename Predicate>
    bool findInQueues(Predicate pred) const;

    template<typename Predicate>
    bool boostFromLowToHigh(Predicate pred);

    // HTTP and signing
    std::string httpGet(const std::string& url,
                        const std::map<std::string, std::string>& headers,
                        std::shared_ptr<std::atomic<bool>> cancel_flag = nullptr);
    std::string urlEncode(const std::string& value);

    // XML parsing
    std::vector<S3Bucket> parseListBucketsXml(const std::string& xml);
    struct ListObjectsResult {
        std::vector<S3Object> objects;
        std::string next_continuation_token;
        bool is_truncated = false;
        std::string error;
    };
    ListObjectsResult parseListObjectsXml(const std::string& xml);

    AWSProfile m_profile;
    size_t m_numWorkers;

    // High-priority worker threads and queue (user actions)
    std::vector<std::thread> m_highPriorityWorkers;
    mutable std::mutex m_highPriorityMutex;
    std::condition_variable m_highPriorityCv;
    std::deque<WorkItem> m_highPriorityQueue;

    // Low-priority worker threads and queue (prefetch)
    std::vector<std::thread> m_lowPriorityWorkers;
    mutable std::mutex m_lowPriorityMutex;
    std::condition_variable m_lowPriorityCv;
    std::deque<WorkItem> m_lowPriorityQueue;

    std::atomic<bool> m_shutdown{false};

    // Hover prefetch cancellation - when a new cancellable prefetch is queued,
    // the previous one is cancelled via this shared flag
    std::shared_ptr<std::atomic<bool>> m_currentHoverCancelFlag;
    std::mutex m_hoverCancelMutex;

    // Event queue (results from worker thread)
    std::mutex m_eventMutex;
    std::vector<StateEvent> m_events;

    void pushEvent(StateEvent event);
};
