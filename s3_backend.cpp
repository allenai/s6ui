#include "s3_backend.h"
#include "aws_signer.h"
#include "loguru.hpp"
#include <curl/curl.h>
#include <sstream>
#include <cctype>

static size_t writeCallback(void* contents, size_t size, size_t nmemb, std::string* userp) {
    size_t total = size * nmemb;
    userp->append(static_cast<char*>(contents), total);
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

S3Backend::S3Backend(const AWSProfile& profile)
    : m_profile(profile)
{
    LOG_F(INFO, "S3Backend: initializing with profile=%s region=%s",
          profile.name.c_str(), profile.region.c_str());
    curl_global_init(CURL_GLOBAL_DEFAULT);
    m_worker = std::thread(&S3Backend::workerThread, this);
}

S3Backend::~S3Backend() {
    LOG_F(INFO, "S3Backend: shutting down");
    cancelAll();

    // Signal shutdown
    {
        std::lock_guard<std::mutex> lock(m_workMutex);
        m_shutdown = true;
        m_workQueue.push({WorkItem::Type::Shutdown, "", "", "", {}});
    }
    m_workCv.notify_one();

    if (m_worker.joinable()) {
        m_worker.join();
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
    enqueue({WorkItem::Type::ListBuckets, "", "", "", std::chrono::steady_clock::now()});
}

void S3Backend::listObjects(
    const std::string& bucket,
    const std::string& prefix,
    const std::string& continuation_token
) {
    LOG_F(INFO, "S3Backend: queuing listObjects bucket=%s prefix=%s token=%s",
          bucket.c_str(), prefix.c_str(),
          continuation_token.empty() ? "(none)" : continuation_token.substr(0, 20).c_str());
    enqueue({WorkItem::Type::ListObjects, bucket, prefix, continuation_token, std::chrono::steady_clock::now()});
}

void S3Backend::cancelAll() {
    std::lock_guard<std::mutex> lock(m_workMutex);
    std::queue<WorkItem> empty;
    std::swap(m_workQueue, empty);
}

void S3Backend::enqueue(WorkItem item) {
    {
        std::lock_guard<std::mutex> lock(m_workMutex);
        m_workQueue.push(std::move(item));
    }
    m_workCv.notify_one();
}

void S3Backend::workerThread() {
    loguru::set_thread_name("S3Worker");
    LOG_F(INFO, "S3Backend: worker thread started");

    while (true) {
        WorkItem item;
        {
            std::unique_lock<std::mutex> lock(m_workMutex);
            m_workCv.wait(lock, [this] {
                return !m_workQueue.empty() || m_shutdown;
            });

            if (m_shutdown && m_workQueue.empty()) {
                break;
            }

            item = std::move(m_workQueue.front());
            m_workQueue.pop();
        }

        if (item.type == WorkItem::Type::Shutdown) {
            break;
        }

        if (item.type == WorkItem::Type::ListBuckets) {
            std::string host = "s3." + m_profile.region + ".amazonaws.com";
            LOG_F(1, "S3Backend: fetching bucket list from %s", host.c_str());

            auto signedReq = aws_sign_request(
                "GET", host, "/", "", m_profile.region, "s3",
                m_profile.access_key_id, m_profile.secret_access_key
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
            std::string host = item.bucket + ".s3." + m_profile.region + ".amazonaws.com";
            LOG_F(1, "S3Backend: fetching objects bucket=%s prefix=%s",
                  item.bucket.c_str(), item.prefix.c_str());

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
                "GET", host, "/", query.str(), m_profile.region, "s3",
                m_profile.access_key_id, m_profile.secret_access_key
            );

            auto http_start = std::chrono::steady_clock::now();
            std::string response = httpGet(signedReq.url, signedReq.headers);
            auto http_end = std::chrono::steady_clock::now();

            if (response.find("ERROR:") == 0) {
                auto now = std::chrono::steady_clock::now();
                auto total_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - item.queued_at).count();
                auto http_ms = std::chrono::duration_cast<std::chrono::milliseconds>(http_end - http_start).count();
                LOG_F(WARNING, "S3Backend: listObjects HTTP error: %s (total=%lldms http=%lldms)",
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
                    LOG_F(WARNING, "S3Backend: listObjects S3 error: %s (total=%lldms http=%lldms parse=%lldms)",
                          result.error.c_str(), total_ms, http_ms, parse_ms);
                    pushEvent(StateEvent::objectsError(item.bucket, item.prefix, result.error));
                } else {
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
                }
            }
        }
    }

    LOG_F(INFO, "S3Backend: worker thread exiting");
}

std::string S3Backend::httpGet(const std::string& url,
                               const std::map<std::string, std::string>& headers) {
    CURL* curl = curl_easy_init();
    if (!curl) return "ERROR: Failed to init curl";

    std::string response;
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 30L);

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
