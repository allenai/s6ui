#include "s3_backend.h"
#include "aws_signer.h"
#include "loguru.hpp"
#include <curl/curl.h>
#include <sstream>
#include <cctype>
#include <GLFW/glfw3.h>

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

// Context for streaming downloads - emits events as chunks arrive
struct StreamingDownloadContext {
    S3Backend* backend = nullptr;
    std::string bucket;
    std::string key;
    size_t bytesReceived = 0;  // Bytes received in this request (offset from startByte)
    size_t startByte = 0;      // Starting byte offset in file
    size_t totalSize = 0;      // Total file size
    std::shared_ptr<std::atomic<bool>> cancel_flag;
    std::string buffer;        // Buffer for chunking
    std::function<void(StateEvent)> pushEvent;  // Callback to push events

    static constexpr size_t CHUNK_SIZE = 256 * 1024;  // Emit events every 256KB
};

static size_t streamingWriteCallback(void* contents, size_t size, size_t nmemb, void* userdata) {
    size_t total = size * nmemb;
    auto* ctx = static_cast<StreamingDownloadContext*>(userdata);

    // Check cancellation
    if (ctx->cancel_flag && ctx->cancel_flag->load()) {
        return 0;  // Abort transfer
    }

    // Add to buffer
    ctx->buffer.append(static_cast<char*>(contents), total);

    // Emit events for complete chunks
    while (ctx->buffer.size() >= StreamingDownloadContext::CHUNK_SIZE) {
        std::string chunk(ctx->buffer.begin(), ctx->buffer.begin() + StreamingDownloadContext::CHUNK_SIZE);
        ctx->buffer.erase(0, StreamingDownloadContext::CHUNK_SIZE);

        size_t chunkOffset = ctx->startByte + ctx->bytesReceived;
        ctx->bytesReceived += chunk.size();

        ctx->pushEvent(StateEvent::objectRangeLoaded(
            ctx->bucket, ctx->key, chunkOffset, ctx->totalSize, std::move(chunk)));
    }

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

// Helper to extract region from an S3 endpoint like "bucket.s3.us-west-2.amazonaws.com"
static std::string extractRegionFromEndpoint(const std::string& endpoint) {
    // Expected formats:
    // - bucket.s3.region.amazonaws.com
    // - bucket.s3-region.amazonaws.com
    // - s3.region.amazonaws.com (without bucket prefix)
    // - s3-region.amazonaws.com (old format)
    // Special case: s3.amazonaws.com (global endpoint, no region)

    std::string search = endpoint;

    // Check for global endpoint first
    if (search == "s3.amazonaws.com" || search.find("s3.amazonaws.com") == 0) {
        // Check if there's actually a region between s3 and amazonaws
        // s3.amazonaws.com -> no region
        // s3.us-east-1.amazonaws.com -> has region
        size_t s3Pos = search.find("s3.");
        if (s3Pos != std::string::npos) {
            size_t regionStart = s3Pos + 3;  // After "s3."
            size_t amazonawsPos = search.find(".amazonaws.com", regionStart);
            if (amazonawsPos != std::string::npos) {
                // Check if there's anything between "s3." and ".amazonaws.com"
                if (amazonawsPos == regionStart) {
                    // It's "s3.amazonaws.com" with nothing in between
                    return "";
                }
            }
        }
    }

    // Look for "s3." or "s3-" patterns
    size_t s3DotPos = search.find("s3.");
    size_t s3DashPos = search.find("s3-");

    size_t s3Pos = std::string::npos;
    size_t regionStart = 0;

    if (s3DotPos != std::string::npos) {
        // Found "s3." - region starts after "s3."
        s3Pos = s3DotPos;
        regionStart = s3Pos + 3;  // Skip "s3."
    } else if (s3DashPos != std::string::npos) {
        // Found "s3-" - region starts after "s3-"
        s3Pos = s3DashPos;
        regionStart = s3Pos + 3;  // Skip "s3-"
    } else {
        return "";  // Can't parse
    }

    // Find the next dot (before amazonaws.com)
    size_t regionEnd = search.find('.', regionStart);
    if (regionEnd == std::string::npos || regionEnd <= regionStart) {
        return "";
    }

    std::string region = search.substr(regionStart, regionEnd - regionStart);

    // Validate it looks like a region (not "amazonaws" or other invalid values)
    // Valid regions typically contain at least one dash (e.g., us-east-1, eu-west-2)
    if (region.find('-') == std::string::npos) {
        return "";  // Not a valid region format
    }

    return region;
}

// Bucket region cache methods
std::string S3Backend::getCachedRegion(const std::string& bucket) const {
    std::lock_guard<std::mutex> lock(m_regionCacheMutex);
    auto it = m_bucketRegionCache.find(bucket);
    if (it != m_bucketRegionCache.end()) {
        return it->second;
    }
    return "";  // Not cached
}

void S3Backend::cacheRegion(const std::string& bucket, const std::string& region) {
    std::lock_guard<std::mutex> lock(m_regionCacheMutex);
    m_bucketRegionCache[bucket] = region;
    LOG_F(1, "S3Backend: cached region for bucket=%s region=%s", bucket.c_str(), region.c_str());
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
    {
        std::lock_guard<std::mutex> lock(m_eventMutex);
        m_events.push_back(std::move(event));
    }
    // Wake up the main event loop so it processes this event immediately
    glfwPostEmptyEvent();
}

void S3Backend::setProfile(const AWSProfile& profile) {
    LOG_F(INFO, "S3Backend: switching profile to %s region=%s",
          profile.name.c_str(), profile.region.c_str());

    // Cancel all pending requests to ensure clean state
    cancelAll();

    // Clear the bucket region cache when switching profiles
    {
        std::lock_guard<std::mutex> lock(m_regionCacheMutex);
        m_bucketRegionCache.clear();
        LOG_F(1, "S3Backend: cleared region cache on profile switch");
    }

    // Copy the profile and refresh credentials from disk
    m_profile = profile;
    if (!refresh_profile_credentials(m_profile)) {
        LOG_F(WARNING, "S3Backend: failed to refresh credentials for profile %s, using cached credentials",
              profile.name.c_str());
        // Fall back to using the provided profile as-is
        m_profile = profile;
    }

    LOG_F(INFO, "S3Backend: profile switched to %s region=%s",
          m_profile.name.c_str(), m_profile.region.c_str());
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

void S3Backend::getObjectStreaming(
    const std::string& bucket,
    const std::string& key,
    size_t startByte,
    size_t totalSize,
    std::shared_ptr<std::atomic<bool>> cancel_flag
) {
    LOG_F(INFO, "S3Backend: queuing getObjectStreaming bucket=%s key=%s startByte=%zu totalSize=%zu",
          bucket.c_str(), key.c_str(), startByte, totalSize);

    WorkItem item;
    item.type = WorkItem::Type::GetObjectStreaming;
    item.priority = WorkItem::Priority::High;
    item.bucket = bucket;
    item.key = key;
    item.start_byte = startByte;
    item.total_size = totalSize;
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
        // Clear cancel flag - once user explicitly navigates/selects, don't allow
        // cancellation by subsequent hover prefetches
        foundItem.cancel_flag.reset();
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
            LOG_F(WARNING, "S3Backend: listBuckets HTTP error: %s (total=%lldms http=%lldms)",
                  response.c_str(), total_ms, http_ms);
            pushEvent(StateEvent::bucketsError(response));
        } else {
            std::string error = extractError(response);
            if (!error.empty()) {
                auto now = std::chrono::steady_clock::now();
                auto total_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - item.queued_at).count();
                auto http_ms = std::chrono::duration_cast<std::chrono::milliseconds>(http_end - http_start).count();
                LOG_F(WARNING, "S3Backend: listBuckets S3 error: %s (total=%lldms http=%lldms)",
                      error.c_str(), total_ms, http_ms);
                pushEvent(StateEvent::bucketsError(error));
            } else {
                auto parse_start = std::chrono::steady_clock::now();
                auto buckets = parseListBucketsXml(response);
                auto now = std::chrono::steady_clock::now();
                auto total_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - item.queued_at).count();
                auto http_ms = std::chrono::duration_cast<std::chrono::milliseconds>(http_end - http_start).count();
                auto parse_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - parse_start).count();
                LOG_F(INFO, "S3Backend: listBuckets success, got %zu buckets (total=%lldms http=%lldms parse=%lldms)",
                      buckets.size(), total_ms, http_ms, parse_ms);
                pushEvent(StateEvent::bucketsLoaded(std::move(buckets)));
            }
        }
    }
    else if (item.type == WorkItem::Type::ListObjects) {
        // Check cache first, fall back to profile region
        std::string cachedRegion = getCachedRegion(item.bucket);
        std::string region = cachedRegion.empty() ? m_profile.region : cachedRegion;

        // Validate region is not empty
        if (region.empty()) {
            LOG_F(ERROR, "S3Backend: region is empty for bucket=%s, profile.region=%s, cached=%s",
                  item.bucket.c_str(), m_profile.region.c_str(), cachedRegion.c_str());
            pushEvent(StateEvent::objectsError(item.bucket, item.prefix,
                "ERROR: Region not configured. Please ensure your AWS profile has a valid region."));
            return;
        }

        bool retried = false;

        // Retry loop for handling PermanentRedirect
        for (int attempt = 0; attempt < 2; ++attempt) {
            std::string host;
            std::string path;
            if (!m_profile.endpoint_url.empty()) {
                // Path-style: endpoint/bucket
                host = parseEndpointHost(m_profile.endpoint_url);
                path = "/" + item.bucket;
            } else {
                // Virtual-host style: bucket.s3.region.amazonaws.com
                host = item.bucket + ".s3." + region + ".amazonaws.com";
                path = "/";
            }
            LOG_F(1, "S3Backend: fetching objects bucket=%s prefix=%s host=%s path=%s region=%s%s",
                  item.bucket.c_str(), item.prefix.c_str(), host.c_str(), path.c_str(), region.c_str(),
                  retried ? " (retry)" : "");

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
                "GET", host, path, query.str(), region, "s3",
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
                LOG_F(WARNING, "S3Backend: listObjects HTTP error: %s (total=%lldms http=%lldms)",
                      response.c_str(), total_ms, http_ms);
                pushEvent(StateEvent::objectsError(item.bucket, item.prefix, response));
                return;
            }

            auto parse_start = std::chrono::steady_clock::now();
            auto result = parseListObjectsXml(response);
            auto now = std::chrono::steady_clock::now();
            auto total_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - item.queued_at).count();
            auto http_ms = std::chrono::duration_cast<std::chrono::milliseconds>(http_end - http_start).count();
            auto parse_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - parse_start).count();

            if (!result.error.empty()) {
                // Check for PermanentRedirect error
                std::string errorCode = extractTag(response, "Code");
                if (errorCode == "PermanentRedirect" && attempt == 0) {
                    // Extract the correct endpoint and retry
                    std::string correctEndpoint = extractTag(response, "Endpoint");
                    LOG_F(INFO, "S3Backend: PermanentRedirect error, endpoint in response: '%s'",
                          correctEndpoint.c_str());

                    std::string correctRegion;

                    // Try to extract region from the endpoint
                    if (!correctEndpoint.empty()) {
                        correctRegion = extractRegionFromEndpoint(correctEndpoint);
                    }

                    // If that failed, try to extract region from bucket name
                    // Many buckets have region in their name, e.g., "my-bucket-us-east-1"
                    if (correctRegion.empty()) {
                        LOG_F(INFO, "S3Backend: trying to extract region from bucket name: '%s'",
                              item.bucket.c_str());
                        // Look for region pattern in bucket name (e.g., us-east-1, eu-west-2)
                        // Common patterns: us-east-1, us-west-2, eu-west-1, ap-southeast-1, etc.
                        std::string bucketLower = item.bucket;
                        for (auto& c : bucketLower) c = std::tolower(c);

                        // List of common AWS regions to search for
                        const char* regions[] = {
                            "us-east-1", "us-east-2", "us-west-1", "us-west-2",
                            "eu-west-1", "eu-west-2", "eu-west-3", "eu-central-1", "eu-north-1",
                            "ap-southeast-1", "ap-southeast-2", "ap-northeast-1", "ap-northeast-2", "ap-south-1",
                            "ca-central-1", "sa-east-1"
                        };

                        for (const char* regionName : regions) {
                            if (bucketLower.find(regionName) != std::string::npos) {
                                correctRegion = regionName;
                                LOG_F(INFO, "S3Backend: extracted region from bucket name: %s", correctRegion.c_str());
                                break;
                            }
                        }
                    }

                    // Last resort: use us-east-1 as default (most common region)
                    if (correctRegion.empty()) {
                        correctRegion = "us-east-1";
                        LOG_F(INFO, "S3Backend: falling back to default region: %s", correctRegion.c_str());
                    }

                    if (!correctRegion.empty() && correctRegion != region) {
                        LOG_F(INFO, "S3Backend: detected PermanentRedirect, retrying with region=%s (was %s)",
                              correctRegion.c_str(), region.c_str());
                        region = correctRegion;
                        cacheRegion(item.bucket, correctRegion);  // Cache for future requests
                        retried = true;
                        continue;  // Retry with corrected region
                    } else {
                        LOG_F(WARNING, "S3Backend: PermanentRedirect but could not determine correct region (endpoint: '%s', bucket: '%s')",
                              correctEndpoint.c_str(), item.bucket.c_str());
                    }
                }

                LOG_F(WARNING, "S3Backend: listObjects S3 error: %s (total=%lldms http=%lldms parse=%lldms)",
                      result.error.c_str(), total_ms, http_ms, parse_ms);
                pushEvent(StateEvent::objectsError(item.bucket, item.prefix, result.error));
                return;
            }

            // Cache the region on success (either profile region or corrected region)
            cacheRegion(item.bucket, region);

            LOG_F(INFO, "S3Backend: listObjects success bucket=%s prefix=%s count=%zu truncated=%d (total=%lldms http=%lldms parse=%lldms)",
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
            return;  // Success
        }
    }
    else if (item.type == WorkItem::Type::GetObject) {
        // Check cache first, fall back to profile region
        std::string cachedRegion = getCachedRegion(item.bucket);
        std::string region = cachedRegion.empty() ? m_profile.region : cachedRegion;

        // Validate region is not empty
        if (region.empty()) {
            LOG_F(ERROR, "S3Backend: region is empty for bucket=%s, profile.region=%s, cached=%s",
                  item.bucket.c_str(), m_profile.region.c_str(), cachedRegion.c_str());
            pushEvent(StateEvent::objectContentError(item.bucket, item.key,
                "ERROR: Region not configured. Please ensure your AWS profile has a valid region."));
            return;
        }

        bool retried = false;

        // Retry loop for handling PermanentRedirect
        for (int attempt = 0; attempt < 2; ++attempt) {
            std::string host;
            std::string path;
            if (!m_profile.endpoint_url.empty()) {
                // Path-style: endpoint/bucket/key
                host = parseEndpointHost(m_profile.endpoint_url);
                path = "/" + item.bucket + "/" + item.key;
            } else {
                // Virtual-host style: bucket.s3.region.amazonaws.com/key
                host = item.bucket + ".s3." + region + ".amazonaws.com";
                path = "/" + item.key;
            }
            LOG_F(1, "S3Backend: fetching object bucket=%s key=%s max_bytes=%zu host=%s path=%s region=%s%s",
                  item.bucket.c_str(), item.key.c_str(), item.max_bytes, host.c_str(), path.c_str(), region.c_str(),
                  retried ? " (retry)" : "");

            auto signedReq = aws_sign_request(
                "GET", host, path, "", region, "s3",
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
                LOG_F(WARNING, "S3Backend: getObject HTTP error: %s (total=%lldms http=%lldms)",
                      response.c_str(), total_ms, http_ms);
                pushEvent(StateEvent::objectContentError(item.bucket, item.key, response));
                return;
            }

            // Check for S3 error in XML response
            std::string error = extractError(response);
            if (!error.empty()) {
                // Check for PermanentRedirect error
                std::string errorCode = extractTag(response, "Code");
                if (errorCode == "PermanentRedirect" && attempt == 0) {
                    // Extract the correct endpoint and retry
                    std::string correctEndpoint = extractTag(response, "Endpoint");
                    LOG_F(INFO, "S3Backend: PermanentRedirect error, endpoint in response: '%s'",
                          correctEndpoint.c_str());

                    std::string correctRegion;

                    // Try to extract region from the endpoint
                    if (!correctEndpoint.empty()) {
                        correctRegion = extractRegionFromEndpoint(correctEndpoint);
                    }

                    // If that failed, try to extract region from bucket name
                    if (correctRegion.empty()) {
                        LOG_F(INFO, "S3Backend: trying to extract region from bucket name: '%s'",
                              item.bucket.c_str());
                        std::string bucketLower = item.bucket;
                        for (auto& c : bucketLower) c = std::tolower(c);

                        const char* regions[] = {
                            "us-east-1", "us-east-2", "us-west-1", "us-west-2",
                            "eu-west-1", "eu-west-2", "eu-west-3", "eu-central-1", "eu-north-1",
                            "ap-southeast-1", "ap-southeast-2", "ap-northeast-1", "ap-northeast-2", "ap-south-1",
                            "ca-central-1", "sa-east-1"
                        };

                        for (const char* regionName : regions) {
                            if (bucketLower.find(regionName) != std::string::npos) {
                                correctRegion = regionName;
                                LOG_F(INFO, "S3Backend: extracted region from bucket name: %s", correctRegion.c_str());
                                break;
                            }
                        }
                    }

                    // Last resort: use us-east-1 as default
                    if (correctRegion.empty()) {
                        correctRegion = "us-east-1";
                        LOG_F(INFO, "S3Backend: falling back to default region: %s", correctRegion.c_str());
                    }

                    if (!correctRegion.empty() && correctRegion != region) {
                        LOG_F(INFO, "S3Backend: detected PermanentRedirect, retrying with region=%s (was %s)",
                              correctRegion.c_str(), region.c_str());
                        region = correctRegion;
                        cacheRegion(item.bucket, correctRegion);  // Cache for future requests
                        retried = true;
                        continue;  // Retry with corrected region
                    } else {
                        LOG_F(WARNING, "S3Backend: PermanentRedirect but could not determine correct region (endpoint: '%s', bucket: '%s')",
                              correctEndpoint.c_str(), item.bucket.c_str());
                    }
                }

                // InvalidRange means the file is 0 bytes - return empty content
                if (errorCode == "InvalidRange") {
                    LOG_F(INFO, "S3Backend: getObject empty file (InvalidRange) bucket=%s key=%s (total=%lldms http=%lldms)",
                          item.bucket.c_str(), item.key.c_str(), total_ms, http_ms);
                    pushEvent(StateEvent::objectContentLoaded(item.bucket, item.key, ""));
                    return;
                }

                LOG_F(WARNING, "S3Backend: getObject S3 error: %s (total=%lldms http=%lldms)",
                      error.c_str(), total_ms, http_ms);
                pushEvent(StateEvent::objectContentError(item.bucket, item.key, error));
                return;
            }

            // Cache the region on success (either profile region or corrected region)
            cacheRegion(item.bucket, region);

            LOG_F(INFO, "S3Backend: getObject success bucket=%s key=%s size=%zu (total=%lldms http=%lldms)",
                  item.bucket.c_str(), item.key.c_str(), response.size(), total_ms, http_ms);
            pushEvent(StateEvent::objectContentLoaded(item.bucket, item.key, std::move(response)));
            return;  // Success
        }
    }
    else if (item.type == WorkItem::Type::GetObjectRange) {
        // Check cache first, fall back to profile region
        std::string cachedRegion = getCachedRegion(item.bucket);
        std::string region = cachedRegion.empty() ? m_profile.region : cachedRegion;

        // Validate region is not empty
        if (region.empty()) {
            LOG_F(ERROR, "S3Backend: region is empty for bucket=%s, profile.region=%s, cached=%s",
                  item.bucket.c_str(), m_profile.region.c_str(), cachedRegion.c_str());
            pushEvent(StateEvent::objectRangeError(item.bucket, item.key, item.start_byte,
                "ERROR: Region not configured. Please ensure your AWS profile has a valid region."));
            return;
        }

        bool retried = false;

        // Retry loop for handling PermanentRedirect
        for (int attempt = 0; attempt < 2; ++attempt) {
            std::string host;
            std::string path;
            if (!m_profile.endpoint_url.empty()) {
                host = parseEndpointHost(m_profile.endpoint_url);
                path = "/" + item.bucket + "/" + item.key;
            } else {
                host = item.bucket + ".s3." + region + ".amazonaws.com";
                path = "/" + item.key;
            }
            LOG_F(1, "S3Backend: fetching object range bucket=%s key=%s range=%zu-%zu host=%s path=%s region=%s%s",
                  item.bucket.c_str(), item.key.c_str(), item.start_byte, item.end_byte, host.c_str(), path.c_str(), region.c_str(),
                  retried ? " (retry)" : "");

            auto signedReq = aws_sign_request(
                "GET", host, path, "", region, "s3",
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
                LOG_F(WARNING, "S3Backend: getObjectRange HTTP error: %s (total=%lldms http=%lldms)",
                      curl_easy_strerror(res), total_ms, http_ms);
                pushEvent(StateEvent::objectRangeError(item.bucket, item.key, item.start_byte,
                    "ERROR: " + std::string(curl_easy_strerror(res))));
                return;
            }

            // Check for S3 error in XML response
            std::string error = extractError(ctx.body);
            if (!error.empty()) {
                // Check for PermanentRedirect error
                std::string errorCode = extractTag(ctx.body, "Code");
                if (errorCode == "PermanentRedirect" && attempt == 0) {
                    // Extract the correct endpoint and retry
                    std::string correctEndpoint = extractTag(ctx.body, "Endpoint");
                    LOG_F(INFO, "S3Backend: PermanentRedirect error, endpoint in response: '%s'",
                          correctEndpoint.c_str());

                    std::string correctRegion;

                    // Try to extract region from the endpoint
                    if (!correctEndpoint.empty()) {
                        correctRegion = extractRegionFromEndpoint(correctEndpoint);
                    }

                    // If that failed, try to extract region from bucket name
                    if (correctRegion.empty()) {
                        LOG_F(INFO, "S3Backend: trying to extract region from bucket name: '%s'",
                              item.bucket.c_str());
                        std::string bucketLower = item.bucket;
                        for (auto& c : bucketLower) c = std::tolower(c);

                        const char* regions[] = {
                            "us-east-1", "us-east-2", "us-west-1", "us-west-2",
                            "eu-west-1", "eu-west-2", "eu-west-3", "eu-central-1", "eu-north-1",
                            "ap-southeast-1", "ap-southeast-2", "ap-northeast-1", "ap-northeast-2", "ap-south-1",
                            "ca-central-1", "sa-east-1"
                        };

                        for (const char* regionName : regions) {
                            if (bucketLower.find(regionName) != std::string::npos) {
                                correctRegion = regionName;
                                LOG_F(INFO, "S3Backend: extracted region from bucket name: %s", correctRegion.c_str());
                                break;
                            }
                        }
                    }

                    // Last resort: use us-east-1 as default
                    if (correctRegion.empty()) {
                        correctRegion = "us-east-1";
                        LOG_F(INFO, "S3Backend: falling back to default region: %s", correctRegion.c_str());
                    }

                    if (!correctRegion.empty() && correctRegion != region) {
                        LOG_F(INFO, "S3Backend: detected PermanentRedirect, retrying with region=%s (was %s)",
                              correctRegion.c_str(), region.c_str());
                        region = correctRegion;
                        cacheRegion(item.bucket, correctRegion);  // Cache for future requests
                        retried = true;
                        continue;  // Retry with corrected region
                    } else {
                        LOG_F(WARNING, "S3Backend: PermanentRedirect but could not determine correct region (endpoint: '%s', bucket: '%s')",
                              correctEndpoint.c_str(), item.bucket.c_str());
                    }
                }

                LOG_F(WARNING, "S3Backend: getObjectRange S3 error: %s (total=%lldms http=%lldms)",
                      error.c_str(), total_ms, http_ms);
                pushEvent(StateEvent::objectRangeError(item.bucket, item.key, item.start_byte, error));
                return;
            }

            // Cache the region on success (either profile region or corrected region)
            cacheRegion(item.bucket, region);

            LOG_F(INFO, "S3Backend: getObjectRange success bucket=%s key=%s range=%zu-%zu got=%zu total=%zu (total=%lldms http=%lldms)",
                  item.bucket.c_str(), item.key.c_str(), item.start_byte, item.end_byte,
                  ctx.body.size(), ctx.contentRangeTotal, total_ms, http_ms);
            pushEvent(StateEvent::objectRangeLoaded(item.bucket, item.key, item.start_byte,
                ctx.contentRangeTotal, std::move(ctx.body)));
            return;  // Success
        }
    }
    else if (item.type == WorkItem::Type::GetObjectStreaming) {
        // Check cache first, fall back to profile region
        std::string cachedRegion = getCachedRegion(item.bucket);
        std::string region = cachedRegion.empty() ? m_profile.region : cachedRegion;

        // Validate region is not empty
        if (region.empty()) {
            LOG_F(ERROR, "S3Backend: region is empty for bucket=%s, profile.region=%s, cached=%s",
                  item.bucket.c_str(), m_profile.region.c_str(), cachedRegion.c_str());
            pushEvent(StateEvent::objectRangeError(item.bucket, item.key, item.start_byte,
                "ERROR: Region not configured. Please ensure your AWS profile has a valid region."));
            return;
        }

        bool retried = false;

        // Retry loop for handling PermanentRedirect
        for (int attempt = 0; attempt < 2; ++attempt) {
            std::string host;
            std::string path;
            if (!m_profile.endpoint_url.empty()) {
                host = parseEndpointHost(m_profile.endpoint_url);
                path = "/" + item.bucket + "/" + item.key;
            } else {
                host = item.bucket + ".s3." + region + ".amazonaws.com";
                path = "/" + item.key;
            }
            LOG_F(INFO, "S3Backend: streaming object bucket=%s key=%s startByte=%zu totalSize=%zu host=%s region=%s%s",
                  item.bucket.c_str(), item.key.c_str(), item.start_byte, item.total_size,
                  host.c_str(), region.c_str(), retried ? " (retry)" : "");

            auto signedReq = aws_sign_request(
                "GET", host, path, "", region, "s3",
                m_profile.access_key_id, m_profile.secret_access_key, "",
                m_profile.session_token
            );

            // Add Range header if starting from non-zero offset
            if (item.start_byte > 0) {
                signedReq.headers["Range"] = "bytes=" + std::to_string(item.start_byte) + "-";
            }

            auto http_start = std::chrono::steady_clock::now();

            CURL* curl = curl_easy_init();
            if (!curl) {
                pushEvent(StateEvent::objectRangeError(item.bucket, item.key, item.start_byte,
                    "ERROR: Failed to init curl"));
                return;
            }

            // Set up streaming context
            StreamingDownloadContext streamCtx;
            streamCtx.backend = this;
            streamCtx.bucket = item.bucket;
            streamCtx.key = item.key;
            streamCtx.startByte = item.start_byte;
            streamCtx.totalSize = item.total_size;
            streamCtx.cancel_flag = item.cancel_flag;
            streamCtx.pushEvent = [this](StateEvent event) { this->pushEvent(std::move(event)); };

            curl_easy_setopt(curl, CURLOPT_URL, signedReq.url.c_str());
            curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, streamingWriteCallback);
            curl_easy_setopt(curl, CURLOPT_WRITEDATA, &streamCtx);
            curl_easy_setopt(curl, CURLOPT_TIMEOUT, 300L);  // 5 minute timeout for large files

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

            if (res == CURLE_ABORTED_BY_CALLBACK || (item.cancel_flag && item.cancel_flag->load())) {
                LOG_F(INFO, "S3Backend: getObjectStreaming cancelled bucket=%s key=%s",
                      item.bucket.c_str(), item.key.c_str());
                return;
            }

            auto now = std::chrono::steady_clock::now();
            auto total_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - item.queued_at).count();
            auto http_ms = std::chrono::duration_cast<std::chrono::milliseconds>(http_end - http_start).count();

            if (res != CURLE_OK) {
                LOG_F(WARNING, "S3Backend: getObjectStreaming HTTP error: %s (total=%lldms http=%lldms)",
                      curl_easy_strerror(res), total_ms, http_ms);
                pushEvent(StateEvent::objectRangeError(item.bucket, item.key, item.start_byte,
                    "ERROR: " + std::string(curl_easy_strerror(res))));
                return;
            }

            // Check for S3 error in any buffered response (errors come as XML)
            std::string error = extractError(streamCtx.buffer);
            if (!error.empty()) {
                // Check for PermanentRedirect error
                std::string errorCode = extractTag(streamCtx.buffer, "Code");
                if (errorCode == "PermanentRedirect" && attempt == 0) {
                    std::string correctEndpoint = extractTag(streamCtx.buffer, "Endpoint");
                    LOG_F(INFO, "S3Backend: PermanentRedirect error, endpoint: '%s'", correctEndpoint.c_str());

                    std::string correctRegion;
                    if (!correctEndpoint.empty()) {
                        correctRegion = extractRegionFromEndpoint(correctEndpoint);
                    }
                    if (correctRegion.empty()) {
                        correctRegion = "us-east-1";
                    }

                    if (!correctRegion.empty() && correctRegion != region) {
                        LOG_F(INFO, "S3Backend: retrying with region=%s (was %s)",
                              correctRegion.c_str(), region.c_str());
                        region = correctRegion;
                        cacheRegion(item.bucket, correctRegion);
                        retried = true;
                        continue;
                    }
                }

                LOG_F(WARNING, "S3Backend: getObjectStreaming S3 error: %s", error.c_str());
                pushEvent(StateEvent::objectRangeError(item.bucket, item.key, item.start_byte, error));
                return;
            }

            // Cache region on success
            cacheRegion(item.bucket, region);

            // Emit any remaining buffered data as final chunk
            if (!streamCtx.buffer.empty()) {
                size_t chunkOffset = item.start_byte + streamCtx.bytesReceived;
                LOG_F(1, "S3Backend: emitting final chunk of %zu bytes at offset %zu",
                      streamCtx.buffer.size(), chunkOffset);
                pushEvent(StateEvent::objectRangeLoaded(item.bucket, item.key, chunkOffset,
                    item.total_size, std::move(streamCtx.buffer)));
            }

            LOG_F(INFO, "S3Backend: getObjectStreaming complete bucket=%s key=%s downloaded=%zu bytes (total=%lldms http=%lldms)",
                  item.bucket.c_str(), item.key.c_str(),
                  item.start_byte + streamCtx.bytesReceived + streamCtx.buffer.size(),
                  total_ms, http_ms);
            return;  // Success
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
