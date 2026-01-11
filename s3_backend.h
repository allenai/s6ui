#pragma once

#include "backend.h"
#include "aws_credentials.h"
#include <string>
#include <vector>
#include <map>
#include <mutex>
#include <thread>
#include <atomic>
#include <queue>
#include <condition_variable>

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
    explicit S3Backend(const AWSProfile& profile);
    ~S3Backend() override;

    std::vector<StateEvent> takeEvents() override;
    void listBuckets() override;
    void listObjects(
        const std::string& bucket,
        const std::string& prefix,
        const std::string& continuation_token = ""
    ) override;
    void cancelAll() override;

    // Change the active profile
    void setProfile(const AWSProfile& profile);

private:
    // Work item for the background thread
    struct WorkItem {
        enum class Type { ListBuckets, ListObjects, Shutdown };
        Type type;
        std::string bucket;
        std::string prefix;
        std::string continuation_token;
    };

    void workerThread();
    void enqueue(WorkItem item);

    // HTTP and signing
    std::string httpGet(const std::string& url,
                        const std::map<std::string, std::string>& headers);
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

    // Worker thread and work queue
    std::thread m_worker;
    std::mutex m_workMutex;
    std::condition_variable m_workCv;
    std::queue<WorkItem> m_workQueue;
    std::atomic<bool> m_shutdown{false};

    // Event queue (results from worker thread)
    std::mutex m_eventMutex;
    std::vector<StateEvent> m_events;

    void pushEvent(StateEvent event);
};
