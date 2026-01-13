#include "s3_backend.h"
#include "aws_signer.h"
#include "loguru.hpp"
#include <curl/curl.h>
#include <sstream>
#include <cctype>

// Helper to parse endpoint URL and extract host (with port if present)
static std::string parseEndpointHost(const std::string& endpoint_url) {
    // Expected format: https://host:port or http://host:port or just host:port
    std::string url = endpoint_url;

    // Strip scheme if present
    if (url.find("https://") == 0) {
        url = url.substr(8);
    } else if (url.find("http://") == 0) {
        url = url.substr(7);
    }

    // Remove trailing slash if present
    if (!url.empty() && url.back() == '/') {
        url.pop_back();
    }

    // Remove path if present (keep only host:port)
    size_t pathPos = url.find('/');
    if (pathPos != std::string::npos) {
        url = url.substr(0, pathPos);
    }

    return url;
}

static size_t writeCallback(void* contents, size_t size, size_t nmemb, std::string* userp) {
    size_t total = size * nmemb;
    userp->append(static_cast<char*>(contents), total);
    return total;
}

// Header callback for capturing Content-Range header
struct HttpResponseContext {
    std::string body;
    size_t contentRangeTotal = 0;  // Total size from Content-Range header
};

static size_t headerCallback(char* buffer, size_t size, size_t nitems, void* userdata) {
    size_t total = size * nitems;
    auto* ctx = static_cast<HttpResponseContext*>(userdata);

    std::string header(buffer, total);
    // Look for Content-Range: bytes 0-1023/12345
    if (header.find("Content-Range:") == 0 || header.find("content-range:") == 0) {
        size_t slashPos = header.find('/');
        if (slashPos != std::string::npos) {
            std::string totalStr = header.substr(slashPos + 1);
            // Trim whitespace
            while (!totalStr.empty() && (totalStr.back() == '\r' || totalStr.back() == '\n' || totalStr.back() == ' ')) {
                totalStr.pop_back();
            }
            try {
                ctx->contentRangeTotal = std::stoull(totalStr);
            } catch (...) {
                // Ignore parse errors
            }
        }
    }
    return total;
}

static size_t writeCallbackCtx(void* contents, size_t size, size_t nmemb, void* userdata) {
    size_t total = size * nmemb;
    auto* ctx = static_cast<HttpResponseContext*>(userdata);
    ctx->body.append(static_cast<char*>(contents), total);
    return total;
}

static std::string extractTag(const std::string& xml, const std::string& tag) {
    std::string open = "<" + tag + ">";
    std::string close = "</" + tag + ">";
    size_t start = xml.find(open);
    if (start == std::string::npos) return "";
    start += open.size();
    size_t end = xml.find(close, start);
    if (end == std::string::npos) return "";
    return xml.substr(start, end - start);
}

static std::string extractError(const std::string& xml) {
    std::string code = extractTag(xml, "Code");
    std::string message = extractTag(xml, "Message");
    if (!code.empty()) {
        return code + ": " + message;
    }
    return "";
}

// Progress callback for cancellable requests
static int cancelCheckProgressCallback(void* clientp, curl_off_t /*dltotal*/, curl_off_t /*dlnow*/,
                                       curl_off_t /*ultotal*/, curl_off_t /*ulnow*/) {
    auto* cancel_flag = static_cast<std::atomic<bool>*>(clientp);
    // If cancelled, abort the transfer
    if (cancel_flag && cancel_flag->load()) {
        return 1;  // Non-zero aborts the transfer
    }
    return 0;
}

S3Backend::S3Backend(const AWSProfile& profile, size_t numWorkers)
    : m_profile(profile), m_numWorkers(numWorkers)
{
    LOG_F(INFO, "S3Backend: initializing with profile=%s region=%s numWorkers=%zu",
          profile.name.c_str(), profile.region.c_str(), numWorkers);
    curl_global_init(CURL_GLOBAL_DEFAULT);

    // Spawn high-priority workers
    for (size_t i = 0; i < m_numWorkers; ++i) {
        m_highPriorityWorkers.emplace_back(&S3Backend::workerThread, this, WorkItem::Priority::High, i);
    }

    // Spawn low-priority workers
    for (size_t i = 0; i < m_numWorkers; ++i) {
        m_lowPriorityWorkers.emplace_back(&S3Backend::workerThread, this, WorkItem::Priority::Low, i);
    }
}

S3Backend::~S3Backend() {
    LOG_F(INFO, "S3Backend: shutting down");
    cancelAll();

    // Signal shutdown
    m_shutdown = true;

    // Wake up all high-priority workers with shutdown items
    {
        std::lock_guard<std::mutex> lock(m_highPriorityMutex);
        for (size_t i = 0; i < m_numWorkers; ++i) {
            WorkItem item;
            item.type = WorkItem::Type::Shutdown;
            item.priority = WorkItem::Priority::High;
            m_highPriorityQueue.push_back(std::move(item));
        }
    }
    m_highPriorityCv.notify_all();

    // Wake up all low-priority workers with shutdown items
    {
        std::lock_guard<std::mutex> lock(m_lowPriorityMutex);
        for (size_t i = 0; i < m_numWorkers; ++i) {
            WorkItem item;
            item.type = WorkItem::Type::Shutdown;
            item.priority = WorkItem::Priority::Low;
            m_lowPriorityQueue.push_back(std::move(item));
        }
    }
    m_lowPriorityCv.notify_all();

    // Join all high-priority workers
    for (auto& worker : m_highPriorityWorkers) {
        if (worker.joinable()) {
            worker.join();
        }
    }

    // Join all low-priority workers
    for (auto& worker : m_lowPriorityWorkers) {
        if (worker.joinable()) {
            worker.join();
        }
    }

    curl_global_cleanup();
}

std::vector<StateEvent> S3Backend::takeEvents() {
    std::lock_guard<std::mutex> lock(m_eventMutex);
    std::vector<StateEvent> events = std::move(m_events);
    m_events.clear();
    return events;
}

void S3Backend::pushEvent(StateEvent event) {
    if (m_shutdown) return;  // Don't push events during shutdown
    std::lock_guard<std::mutex> lock(m_eventMutex);
    m_events.push_back(std::move(event));
}

void S3Backend::setProfile(const AWSProfile& profile) {
    LOG_F(INFO, "S3Backend: switching profile to %s region=%s",
          profile.name.c_str(), profile.region.c_str());
    m_profile = profile;
}

void S3Backend::listBuckets() {
    LOG_F(INFO, "S3Backend: queuing listBuckets request");
    WorkItem item;
    item.type = WorkItem::Type::ListBuckets;
    item.priority = WorkItem::Priority::High;
    item.queued_at = std::chrono::steady_clock::now();
    enqueue(std::move(item));
}

void S3Backend::listObjects(
    const std::string& bucket,
    const std::string& prefix,
    const std::string& continuation_token,
    std::shared_ptr<std::atomic<bool>> cancel_flag
) {
    LOG_F(INFO, "S3Backend: queuing listObjects bucket=%s prefix=%s token=%s cancellable=%d",
          bucket.c_str(), prefix.c_str(),
          continuation_token.empty() ? "(none)" : continuation_token.substr(0, 20).c_str(),
          cancel_flag != nullptr);
    WorkItem item;
    item.type = WorkItem::Type::ListObjects;
    item.priority = WorkItem::Priority::High;
    item.bucket = bucket;
    item.prefix = prefix;
    item.continuation_token = continuation_token;
    item.queued_at = std::chrono::steady_clock::now();
    item.cancel_flag = cancel_flag;
    enqueue(std::move(item));
}

void S3Backend::getObject(
    const std::string& bucket,
    const std::string& key,
    size_t max_bytes,
    bool lowPriority,
    bool cancellable
) {
    auto priority = lowPriority ? WorkItem::Priority::Low : WorkItem::Priority::High;
    LOG_F(INFO, "S3Backend: queuing getObject bucket=%s key=%s max_bytes=%zu priority=%s cancellable=%d",
          bucket.c_str(), key.c_str(), max_bytes, lowPriority ? "low" : "high", cancellable);

    WorkItem item;
    item.type = WorkItem::Type::GetObject;
    item.priority = priority;
    item.bucket = bucket;
    item.key = key;
    item.max_bytes = max_bytes;
    item.queued_at = std::chrono::steady_clock::now();

    if (cancellable) {
        // Cancel any previous hover prefetch and create a new cancel flag for this one
        std::lock_guard<std::mutex> lock(m_hoverCancelMutex);
        if (m_currentHoverCancelFlag) {
            m_currentHoverCancelFlag->store(true);
        }
        item.cancel_flag = std::make_shared<std::atomic<bool>>(false);
        m_currentHoverCancelFlag = item.cancel_flag;
    }

    enqueue(std::move(item));
}

void S3Backend::getObjectRange(
    const std::string& bucket,
    const std::string& key,
    size_t startByte,
    size_t endByte,
    std::shared_ptr<std::atomic<bool>> cancel_flag
) {
    LOG_F(INFO, "S3Backend: queuing getObjectRange bucket=%s key=%s range=%zu-%zu",
          bucket.c_str(), key.c_str(), startByte, endByte);

    WorkItem item;
    item.type = WorkItem::Type::GetObjectRange;
    item.priority = WorkItem::Priority::High;
    item.bucket = bucket;
    item.key = key;
    item.start_byte = startByte;
    item.end_byte = endByte;
    item.queued_at = std::chrono::steady_clock::now();
    item.cancel_flag = cancel_flag;

    enqueue(std::move(item));
}

void S3Backend::cancelAll() {
    {
        std::lock_guard<std::mutex> lock(m_highPriorityMutex);
        m_highPriorityQueue.clear();
    }
    {
        std::lock_guard<std::mutex> lock(m_lowPriorityMutex);
        m_lowPriorityQueue.clear();
    }
}

void S3Backend::listObjectsPrefetch(
    const std::string& bucket,
    const std::string& prefix,
    bool cancellable
) {
    LOG_F(INFO, "S3Backend: queuing prefetch bucket=%s prefix=%s cancellable=%d", bucket.c_str(), prefix.c_str(), cancellable);

    WorkItem item;
    item.type = WorkItem::Type::ListObjects;
    item.priority = WorkItem::Priority::Low;
    item.bucket = bucket;
    item.prefix = prefix;
    item.queued_at = std::chrono::steady_clock::now();

    if (cancellable) {
        // Cancel any previous hover prefetch and create a new cancel flag for this one
        std::lock_guard<std::mutex> lock(m_hoverCancelMutex);
        if (m_currentHoverCancelFlag) {
            m_currentHoverCancelFlag->store(true);
        }
        item.cancel_flag = std::make_shared<std::atomic<bool>>(false);
        m_currentHoverCancelFlag = item.cancel_flag;
    }

    enqueue(std::move(item));
}

// Template helpers for queue operations
template<typename Predicate>
bool S3Backend::findInQueues(Predicate pred) const {
    {
        std::lock_guard<std::mutex> lock(m_highPriorityMutex);
        for (const auto& item : m_highPriorityQueue) {
            if (pred(item)) return true;
        }
    }
    {
        std::lock_guard<std::mutex> lock(m_lowPriorityMutex);
        for (const auto& item : m_lowPriorityQueue) {
            if (pred(item)) return true;
        }
    }
    return false;
}

template<typename Predicate>
bool S3Backend::boostFromLowToHigh(Predicate pred) {
    WorkItem foundItem;
    bool found = false;

    // Extract from low priority queue if found
    {
        std::lock_guard<std::mutex> lock(m_lowPriorityMutex);
        for (auto it = m_lowPriorityQueue.begin(); it != m_lowPriorityQueue.end(); ++it) {
            if (pred(*it)) {
                foundItem = std::move(*it);
                m_lowPriorityQueue.erase(it);
                found = true;
                break;
            }
        }
    }

    if (found) {
        // Move to high priority queue
        foundItem.priority = WorkItem::Priority::High;
        {
            std::lock_guard<std::mutex> lock(m_highPriorityMutex);
            m_highPriorityQueue.push_front(std::move(foundItem));
        }
        m_highPriorityCv.notify_one();
        return true;
    }

    // Check if already in high priority queue
    {
        std::lock_guard<std::mutex> lock(m_highPriorityMutex);
        for (const auto& item : m_highPriorityQueue) {
            if (pred(item)) return true;
        }
    }

    return false;
}

bool S3Backend::prioritizeRequest(const std::string& bucket, const std::string& prefix) {
    bool result = boostFromLowToHigh([&](const WorkItem& item) {
        return item.type == WorkItem::Type::ListObjects &&
               item.bucket == bucket && item.prefix == prefix;
    });
    if (result) LOG_F(INFO, "S3Backend: prioritized request bucket=%s prefix=%s", bucket.c_str(), prefix.c_str());
    return result;
}

bool S3Backend::hasPendingRequest(const std::string& bucket, const std::string& prefix) const {
    return findInQueues([&](const WorkItem& item) {
        return item.type == WorkItem::Type::ListObjects &&
               item.bucket == bucket && item.prefix == prefix;
    });
}

bool S3Backend::hasPendingObjectRequest(const std::string& bucket, const std::string& key) const {
    return findInQueues([&](const WorkItem& item) {
        return item.type == WorkItem::Type::GetObject &&
               item.bucket == bucket && item.key == key;
    });
}

bool S3Backend::prioritizeObjectRequest(const std::string& bucket, const std::string& key) {
    bool result = boostFromLowToHigh([&](const WorkItem& item) {
        return item.type == WorkItem::Type::GetObject &&
               item.bucket == bucket && item.key == key;
    });
    if (result) LOG_F(INFO, "S3Backend: prioritized object request bucket=%s key=%s", bucket.c_str(), key.c_str());
    return result;
}

void S3Backend::enqueue(WorkItem item) {
    if (item.priority == WorkItem::Priority::High) {
        {
            std::lock_guard<std::mutex> lock(m_highPriorityMutex);
            m_highPriorityQueue.push_back(std::move(item));
        }
        m_highPriorityCv.notify_one();
    } else {
        {
            std::lock_guard<std::mutex> lock(m_lowPriorityMutex);
            // Push to front so most recent prefetch request is fetched first
            m_lowPriorityQueue.push_front(std::move(item));
        }
        m_lowPriorityCv.notify_one();
    }
}

void S3Backend::workerThread(WorkItem::Priority priority, size_t workerIndex) {
    const char* priorityStr = (priority == WorkItem::Priority::High) ? "High" : "Low";
    char threadName[32];
    snprintf(threadName, sizeof(threadName), "S3%s%zu", priorityStr, workerIndex);
    loguru::set_thread_name(threadName);
    LOG_F(INFO, "S3Backend: %s priority worker %zu started", priorityStr, workerIndex);

    // Select the appropriate queue, mutex, and cv based on priority
    auto& queue = (priority == WorkItem::Priority::High) ? m_highPriorityQueue : m_lowPriorityQueue;
    auto& mutex = (priority == WorkItem::Priority::High) ? m_highPriorityMutex : m_lowPriorityMutex;
    auto& cv = (priority == WorkItem::Priority::High) ? m_highPriorityCv : m_lowPriorityCv;

    while (true) {
        WorkItem item;
        {
            std::unique_lock<std::mutex> lock(mutex);
            cv.wait(lock, [this, &queue] {
                return !queue.empty() || m_shutdown;
            });

            if (m_shutdown && queue.empty()) {
                break;
            }

            item = std::move(queue.front());
            queue.pop_front();
        }

        if (item.type == WorkItem::Type::Shutdown) {
            break;
        }

        processWorkItem(item);
    }

    LOG_F(INFO, "S3Backend: %s priority worker %zu exiting", priorityStr, workerIndex);
}

void S3Backend::processWorkItem(WorkItem& item) {
    if (item.type == WorkItem::Type::ListBuckets) {
        std::string host;
        if (!m_profile.endpoint_url.empty()) {
            host = parseEndpointHost(m_profile.endpoint_url);
        } else {
            host = "s3." + m_profile.region + ".amazonaws.com";
        }
        LOG_F(1, "S3Backend: fetching bucket list from %s", host.c_str());

        auto signedReq = aws_sign_request(
            "GET", host, "/", "", m_profile.region, "s3",
            m_profile.access_key_id, m_profile.secret_access_key, "",
            m_profile.session_token
        );

        auto http_start = std::chrono::steady_clock::now();
        std::string response = httpGet(signedReq.url, signedReq.headers);
        auto http_end = std::chrono::steady_clock::now();

        if (response.find("ERROR:") == 0) {
            auto now = std::chrono::steady_clock::now();
            auto total_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - item.queued_at).count();
            auto http_ms = std::chrono::duration_cast<std::chrono::milliseconds>(http_end - http_start).count();
            LOG_F(WARNING, "S3Backend: listBuckets HTTP error: %s (total=%ldms http=%ldms)",
                  response.c_str(), total_ms, http_ms);
            pushEvent(StateEvent::bucketsError(response));
        } else {
            std::string error = extractError(response);
            if (!error.empty()) {
                auto now = std::chrono::steady_clock::now();
                auto total_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - item.queued_at).count();
                auto http_ms = std::chrono::duration_cast<std::chrono::milliseconds>(http_end - http_start).count();
                LOG_F(WARNING, "S3Backend: listBuckets S3 error: %s (total=%ldms http=%ldms)",
                      error.c_str(), total_ms, http_ms);
                pushEvent(StateEvent::bucketsError(error));
            } else {
                auto parse_start = std::chrono::steady_clock::now();
                auto buckets = parseListBucketsXml(response);
                auto now = std::chrono::steady_clock::now();
                auto total_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - item.queued_at).count();
                auto http_ms = std::chrono::duration_cast<std::chrono::milliseconds>(http_end - http_start).count();
                auto parse_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - parse_start).count();
                LOG_F(INFO, "S3Backend: listBuckets success, got %zu buckets (total=%ldms http=%ldms parse=%ldms)",
                      buckets.size(), total_ms, http_ms, parse_ms);
                pushEvent(StateEvent::bucketsLoaded(std::move(buckets)));
            }
        }
    }
    else if (item.type == WorkItem::Type::ListObjects) {
        std::string host;
        std::string path;
        if (!m_profile.endpoint_url.empty()) {
            // Path-style: endpoint/bucket
            host = parseEndpointHost(m_profile.endpoint_url);
            path = "/" + item.bucket;
        } else {
            // Virtual-host style: bucket.s3.region.amazonaws.com
            host = item.bucket + ".s3." + m_profile.region + ".amazonaws.com";
            path = "/";
        }
        LOG_F(1, "S3Backend: fetching objects bucket=%s prefix=%s host=%s path=%s",
              item.bucket.c_str(), item.prefix.c_str(), host.c_str(), path.c_str());

        // Build query string
        std::ostringstream query;
        query << "list-type=2";
        query << "&delimiter=" << urlEncode("/");
        query << "&max-keys=1000";
        if (!item.prefix.empty()) {
            query << "&prefix=" << urlEncode(item.prefix);
        }
        if (!item.continuation_token.empty()) {
            query << "&continuation-token=" << urlEncode(item.continuation_token);
        }

        auto signedReq = aws_sign_request(
            "GET", host, path, query.str(), m_profile.region, "s3",
            m_profile.access_key_id, m_profile.secret_access_key, "",
            m_profile.session_token
        );

        auto http_start = std::chrono::steady_clock::now();
        std::string response = httpGet(signedReq.url, signedReq.headers, item.cancel_flag);
        auto http_end = std::chrono::steady_clock::now();

        // If cancelled, just return without pushing any event
        if (response == "CANCELLED") {
            LOG_F(INFO, "S3Backend: listObjects cancelled bucket=%s prefix=%s (superseded by newer request)",
                  item.bucket.c_str(), item.prefix.c_str());
            return;
        }

        if (response.find("ERROR:") == 0) {
            auto now = std::chrono::steady_clock::now();
            auto total_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - item.queued_at).count();
            auto http_ms = std::chrono::duration_cast<std::chrono::milliseconds>(http_end - http_start).count();
            LOG_F(WARNING, "S3Backend: listObjects HTTP error: %s (total=%ldms http=%ldms)",
                  response.c_str(), total_ms, http_ms);
            pushEvent(StateEvent::objectsError(item.bucket, item.prefix, response));
        } else {
            auto parse_start = std::chrono::steady_clock::now();
            auto result = parseListObjectsXml(response);
            auto now = std::chrono::steady_clock::now();
            auto total_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - item.queued_at).count();
            auto http_ms = std::chrono::duration_cast<std::chrono::milliseconds>(http_end - http_start).count();
            auto parse_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - parse_start).count();

            if (!result.error.empty()) {
                LOG_F(WARNING, "S3Backend: listObjects S3 error: %s (total=%ldms http=%ldms parse=%ldms)",
                      result.error.c_str(), total_ms, http_ms, parse_ms);
                pushEvent(StateEvent::objectsError(item.bucket, item.prefix, result.error));
            } else {
                LOG_F(INFO, "S3Backend: listObjects success bucket=%s prefix=%s count=%zu truncated=%d (total=%ldms http=%ldms parse=%ldms)",
                      item.bucket.c_str(), item.prefix.c_str(),
                      result.objects.size(), result.is_truncated, total_ms, http_ms, parse_ms);
                pushEvent(StateEvent::objectsLoaded(
                    item.bucket,
                    item.prefix,
                    item.continuation_token,
                    std::move(result.objects),
                    result.next_continuation_token,
                    result.is_truncated
                ));
            }
        }
    }
    else if (item.type == WorkItem::Type::GetObject) {
        std::string host;
        std::string path;
        if (!m_profile.endpoint_url.empty()) {
            // Path-style: endpoint/bucket/key
            host = parseEndpointHost(m_profile.endpoint_url);
            path = "/" + item.bucket + "/" + item.key;
        } else {
            // Virtual-host style: bucket.s3.region.amazonaws.com/key
            host = item.bucket + ".s3." + m_profile.region + ".amazonaws.com";
            path = "/" + item.key;
        }
        LOG_F(1, "S3Backend: fetching object bucket=%s key=%s max_bytes=%zu host=%s path=%s",
              item.bucket.c_str(), item.key.c_str(), item.max_bytes, host.c_str(), path.c_str());

        auto signedReq = aws_sign_request(
            "GET", host, path, "", m_profile.region, "s3",
            m_profile.access_key_id, m_profile.secret_access_key, "",
            m_profile.session_token
        );

        // Add Range header if max_bytes is set (doesn't need to be signed)
        if (item.max_bytes > 0) {
            signedReq.headers["Range"] = "bytes=0-" + std::to_string(item.max_bytes - 1);
        }

        auto http_start = std::chrono::steady_clock::now();
        std::string response = httpGet(signedReq.url, signedReq.headers, item.cancel_flag);
        auto http_end = std::chrono::steady_clock::now();

        // If cancelled, just return without pushing any event
        if (response == "CANCELLED") {
            LOG_F(INFO, "S3Backend: getObject cancelled bucket=%s key=%s (superseded by newer request)",
                  item.bucket.c_str(), item.key.c_str());
            return;
        }

        auto now = std::chrono::steady_clock::now();
        auto total_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - item.queued_at).count();
        auto http_ms = std::chrono::duration_cast<std::chrono::milliseconds>(http_end - http_start).count();

        if (response.find("ERROR:") == 0) {
            LOG_F(WARNING, "S3Backend: getObject HTTP error: %s (total=%ldms http=%ldms)",
                  response.c_str(), total_ms, http_ms);
            pushEvent(StateEvent::objectContentError(item.bucket, item.key, response));
        } else {
            // Check for S3 error in XML response
            std::string error = extractError(response);
            if (!error.empty()) {
                LOG_F(WARNING, "S3Backend: getObject S3 error: %s (total=%ldms http=%ldms)",
                      error.c_str(), total_ms, http_ms);
                pushEvent(StateEvent::objectContentError(item.bucket, item.key, error));
            } else {
                LOG_F(INFO, "S3Backend: getObject success bucket=%s key=%s size=%zu (total=%ldms http=%ldms)",
                      item.bucket.c_str(), item.key.c_str(), response.size(), total_ms, http_ms);
                pushEvent(StateEvent::objectContentLoaded(item.bucket, item.key, std::move(response)));
            }
        }
    }
    else if (item.type == WorkItem::Type::GetObjectRange) {
        std::string host;
        std::string path;
        if (!m_profile.endpoint_url.empty()) {
            host = parseEndpointHost(m_profile.endpoint_url);
            path = "/" + item.bucket + "/" + item.key;
        } else {
            host = item.bucket + ".s3." + m_profile.region + ".amazonaws.com";
            path = "/" + item.key;
        }
        LOG_F(1, "S3Backend: fetching object range bucket=%s key=%s range=%zu-%zu host=%s path=%s",
              item.bucket.c_str(), item.key.c_str(), item.start_byte, item.end_byte, host.c_str(), path.c_str());

        auto signedReq = aws_sign_request(
            "GET", host, path, "", m_profile.region, "s3",
            m_profile.access_key_id, m_profile.secret_access_key, "",
            m_profile.session_token
        );

        // Add Range header
        signedReq.headers["Range"] = "bytes=" + std::to_string(item.start_byte) + "-" + std::to_string(item.end_byte);

        // Use httpGetWithContext to capture Content-Range header
        auto http_start = std::chrono::steady_clock::now();

        CURL* curl = curl_easy_init();
        if (!curl) {
            pushEvent(StateEvent::objectRangeError(item.bucket, item.key, item.start_byte, "ERROR: Failed to init curl"));
            return;
        }

        HttpResponseContext ctx;
        curl_easy_setopt(curl, CURLOPT_URL, signedReq.url.c_str());
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeCallbackCtx);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &ctx);
        curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, headerCallback);
        curl_easy_setopt(curl, CURLOPT_HEADERDATA, &ctx);
        curl_easy_setopt(curl, CURLOPT_TIMEOUT, 60L);  // Longer timeout for larger chunks

        if (item.cancel_flag) {
            curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 0L);
            curl_easy_setopt(curl, CURLOPT_XFERINFOFUNCTION, cancelCheckProgressCallback);
            curl_easy_setopt(curl, CURLOPT_XFERINFODATA, item.cancel_flag.get());
        }

        struct curl_slist* headerList = nullptr;
        for (const auto& [key, value] : signedReq.headers) {
            std::string header = key + ": " + value;
            headerList = curl_slist_append(headerList, header.c_str());
        }
        if (headerList) {
            curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headerList);
        }

        CURLcode res = curl_easy_perform(curl);

        if (headerList) curl_slist_free_all(headerList);
        curl_easy_cleanup(curl);

        auto http_end = std::chrono::steady_clock::now();

        if (res == CURLE_ABORTED_BY_CALLBACK) {
            LOG_F(INFO, "S3Backend: getObjectRange cancelled bucket=%s key=%s",
                  item.bucket.c_str(), item.key.c_str());
            return;
        }

        auto now = std::chrono::steady_clock::now();
        auto total_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - item.queued_at).count();
        auto http_ms = std::chrono::duration_cast<std::chrono::milliseconds>(http_end - http_start).count();

        if (res != CURLE_OK) {
            LOG_F(WARNING, "S3Backend: getObjectRange HTTP error: %s (total=%ldms http=%ldms)",
                  curl_easy_strerror(res), total_ms, http_ms);
            pushEvent(StateEvent::objectRangeError(item.bucket, item.key, item.start_byte,
                "ERROR: " + std::string(curl_easy_strerror(res))));
        } else {
            // Check for S3 error in XML response
            std::string error = extractError(ctx.body);
            if (!error.empty()) {
                LOG_F(WARNING, "S3Backend: getObjectRange S3 error: %s (total=%ldms http=%ldms)",
                      error.c_str(), total_ms, http_ms);
                pushEvent(StateEvent::objectRangeError(item.bucket, item.key, item.start_byte, error));
            } else {
                LOG_F(INFO, "S3Backend: getObjectRange success bucket=%s key=%s range=%zu-%zu got=%zu total=%zu (total=%ldms http=%ldms)",
                      item.bucket.c_str(), item.key.c_str(), item.start_byte, item.end_byte,
                      ctx.body.size(), ctx.contentRangeTotal, total_ms, http_ms);
                pushEvent(StateEvent::objectRangeLoaded(item.bucket, item.key, item.start_byte,
                    ctx.contentRangeTotal, std::move(ctx.body)));
            }
        }
    }
}

std::string S3Backend::httpGet(const std::string& url,
                               const std::map<std::string, std::string>& headers,
                               std::shared_ptr<std::atomic<bool>> cancel_flag) {
    CURL* curl = curl_easy_init();
    if (!curl) return "ERROR: Failed to init curl";

    std::string response;
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 30L);

    // For cancellable requests, enable progress callback to check cancellation
    if (cancel_flag) {
        curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 0L);
        curl_easy_setopt(curl, CURLOPT_XFERINFOFUNCTION, cancelCheckProgressCallback);
        curl_easy_setopt(curl, CURLOPT_XFERINFODATA, cancel_flag.get());
    }

    struct curl_slist* headerList = nullptr;
    for (const auto& [key, value] : headers) {
        std::string header = key + ": " + value;
        headerList = curl_slist_append(headerList, header.c_str());
    }
    if (headerList) {
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headerList);
    }

    CURLcode res = curl_easy_perform(curl);

    if (headerList) curl_slist_free_all(headerList);
    curl_easy_cleanup(curl);

    if (res == CURLE_ABORTED_BY_CALLBACK) {
        return "CANCELLED";  // Special marker for cancelled request
    }
    if (res != CURLE_OK) {
        return "ERROR: " + std::string(curl_easy_strerror(res));
    }

    return response;
}

std::string S3Backend::urlEncode(const std::string& value) {
    std::ostringstream encoded;
    for (unsigned char c : value) {
        if (std::isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') {
            encoded << c;
        } else {
            encoded << '%' << std::hex << std::uppercase
                   << static_cast<int>(c);
        }
    }
    return encoded.str();
}

std::vector<S3Bucket> S3Backend::parseListBucketsXml(const std::string& xml) {
    std::vector<S3Bucket> buckets;

    std::string search = "<Bucket>";
    std::string endSearch = "</Bucket>";
    size_t pos = 0;

    while (true) {
        size_t start = xml.find(search, pos);
        if (start == std::string::npos) break;
        size_t end = xml.find(endSearch, start);
        if (end == std::string::npos) break;

        std::string bucketXml = xml.substr(start, end - start + endSearch.size());

        S3Bucket bucket;
        bucket.name = extractTag(bucketXml, "Name");
        bucket.creation_date = extractTag(bucketXml, "CreationDate");

        if (!bucket.name.empty()) {
            buckets.push_back(std::move(bucket));
        }

        pos = end + endSearch.size();
    }

    return buckets;
}

S3Backend::ListObjectsResult S3Backend::parseListObjectsXml(const std::string& xml) {
    ListObjectsResult result;

    // Check for error
    result.error = extractError(xml);
    if (!result.error.empty()) {
        return result;
    }

    // Check truncation
    std::string truncated = extractTag(xml, "IsTruncated");
    result.is_truncated = (truncated == "true");

    // Get continuation token
    result.next_continuation_token = extractTag(xml, "NextContinuationToken");

    // Get common prefixes (folders)
    std::string prefixSearch = "<CommonPrefixes>";
    std::string prefixEnd = "</CommonPrefixes>";
    size_t pos = 0;
    while (true) {
        size_t start = xml.find(prefixSearch, pos);
        if (start == std::string::npos) break;
        size_t end = xml.find(prefixEnd, start);
        if (end == std::string::npos) break;

        std::string prefixXml = xml.substr(start, end - start);
        std::string prefix = extractTag(prefixXml, "Prefix");

        if (!prefix.empty()) {
            S3Object obj;
            obj.key = prefix;
            obj.is_folder = true;
            obj.size = 0;

            // Get display name (last component)
            std::string displayPrefix = prefix;
            if (!displayPrefix.empty() && displayPrefix.back() == '/') {
                displayPrefix.pop_back();
            }
            size_t lastSlash = displayPrefix.rfind('/');
            obj.display_name = (lastSlash != std::string::npos) ?
                displayPrefix.substr(lastSlash + 1) : displayPrefix;

            result.objects.push_back(std::move(obj));
        }
        pos = end + prefixEnd.size();
    }

    // Get objects (files)
    std::string contentsSearch = "<Contents>";
    std::string contentsEnd = "</Contents>";
    pos = 0;
    while (true) {
        size_t start = xml.find(contentsSearch, pos);
        if (start == std::string::npos) break;
        size_t end = xml.find(contentsEnd, start);
        if (end == std::string::npos) break;

        std::string contentsXml = xml.substr(start, end - start);

        S3Object obj;
        obj.key = extractTag(contentsXml, "Key");
        obj.is_folder = false;

        std::string sizeStr = extractTag(contentsXml, "Size");
        obj.size = sizeStr.empty() ? 0 : std::stoll(sizeStr);

        obj.last_modified = extractTag(contentsXml, "LastModified");

        // Get display name
        size_t lastSlash = obj.key.rfind('/');
        obj.display_name = (lastSlash != std::string::npos) ?
            obj.key.substr(lastSlash + 1) : obj.key;

        // Skip folder markers
        if (!obj.key.empty() && obj.key.back() != '/') {
            result.objects.push_back(std::move(obj));
        }

        pos = end + contentsEnd.size();
    }

    return result;
}
