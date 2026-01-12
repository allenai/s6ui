#include "browser_model.h"
#include "loguru.hpp"
#include <algorithm>

BrowserModel::BrowserModel() = default;

BrowserModel::~BrowserModel() = default;

void BrowserModel::setBackend(std::unique_ptr<IBackend> backend) {
    LOG_F(INFO, "Setting backend");
    m_backend = std::move(backend);
}

void BrowserModel::loadProfiles() {
    LOG_F(INFO, "Loading AWS profiles");
    m_profiles = load_aws_profiles();
    m_selectedProfileIdx = 0;
    LOG_F(INFO, "Loaded %zu profiles", m_profiles.size());
}

void BrowserModel::selectProfile(int index) {
    if (index < 0 || index >= static_cast<int>(m_profiles.size())) return;
    if (index == m_selectedProfileIdx) return;

    LOG_F(INFO, "Selecting profile %d: %s", index, m_profiles[index].name.c_str());
    m_selectedProfileIdx = index;

    // Clear state
    m_buckets.clear();
    m_bucketsError.clear();
    m_nodes.clear();
    m_currentBucket.clear();
    m_currentPrefix.clear();

    // Update backend profile and refresh
    if (m_backend) {
        auto* s3Backend = dynamic_cast<S3Backend*>(m_backend.get());
        if (s3Backend && !m_profiles.empty()) {
            s3Backend->setProfile(m_profiles[m_selectedProfileIdx]);
        }
    }

    refresh();
}

void BrowserModel::refresh() {
    LOG_F(INFO, "Refreshing bucket list");
    m_buckets.clear();
    m_bucketsError.clear();
    m_bucketsLoading = true;
    m_nodes.clear();

    if (m_backend) {
        m_backend->listBuckets();
    }
}

void BrowserModel::expandNode(const std::string& bucket, const std::string& prefix) {
    auto& node = getOrCreateNode(bucket, prefix);

    if (node.expanded) return;

    LOG_F(INFO, "Expanding node: bucket=%s prefix=%s", bucket.c_str(), prefix.c_str());
    node.expanded = true;
    node.objects.clear();
    node.error.clear();
    node.loading = true;

    setCurrentPath(bucket, prefix);

    if (m_backend) {
        m_backend->listObjects(bucket, prefix);
    }
}

void BrowserModel::collapseNode(const std::string& bucket, const std::string& prefix) {
    auto* node = getNode(bucket, prefix);
    if (node) {
        node->expanded = false;
    }
}

void BrowserModel::loadMore(const std::string& bucket, const std::string& prefix) {
    auto* node = getNode(bucket, prefix);
    if (!node || !node->is_truncated || node->loading) return;

    LOG_F(INFO, "Loading more objects: bucket=%s prefix=%s", bucket.c_str(), prefix.c_str());
    node->loading = true;

    if (m_backend) {
        m_backend->listObjects(bucket, prefix, node->next_continuation_token);
    }
}

void BrowserModel::navigateTo(const std::string& s3_path) {
    LOG_F(INFO, "Navigating to: %s", s3_path.c_str());
    std::string bucket, prefix;
    if (!parseS3Path(s3_path, bucket, prefix)) return;
    if (bucket.empty()) return;

    // Add bucket if not in list
    addManualBucket(bucket);

    // Expand bucket root
    {
        auto& node = getOrCreateNode(bucket, "");
        node.pending_expand = true;
        node.expanded = true;
        node.objects.clear();
        node.loading = true;
    }

    // Expand each prefix component
    if (!prefix.empty()) {
        std::string currentPrefix;
        size_t pos = 0;
        while (pos < prefix.size()) {
            size_t nextSlash = prefix.find('/', pos);
            if (nextSlash == std::string::npos) {
                currentPrefix = prefix;
                pos = prefix.size();
            } else {
                currentPrefix = prefix.substr(0, nextSlash + 1);
                pos = nextSlash + 1;
            }

            auto& node = getOrCreateNode(bucket, currentPrefix);
            node.pending_expand = true;
            node.expanded = true;
            node.objects.clear();
        }
    }

    setCurrentPath(bucket, prefix);

    // Set scroll target
    m_scrollToTarget = true;
    m_scrollTargetBucket = bucket;
    m_scrollTargetPrefix = prefix;

    // Load bucket root
    if (m_backend) {
        m_backend->listObjects(bucket, "");
        if (!prefix.empty()) {
            m_backend->listObjects(bucket, prefix);
        }
    }
}

void BrowserModel::addManualBucket(const std::string& bucket_name) {
    for (const auto& b : m_buckets) {
        if (b.name == bucket_name) return;
    }
    S3Bucket newBucket;
    newBucket.name = bucket_name;
    newBucket.creation_date = "(manually added)";
    m_buckets.push_back(std::move(newBucket));
}

void BrowserModel::processEvents() {
    if (!m_backend) return;

    auto events = m_backend->takeEvents();

    for (auto& event : events) {
        switch (event.type) {
            case EventType::BucketsLoaded: {
                auto& payload = std::get<BucketsLoadedPayload>(event.payload);
                LOG_F(INFO, "Event: BucketsLoaded count=%zu", payload.buckets.size());
                m_buckets = std::move(payload.buckets);
                m_bucketsLoading = false;
                m_bucketsError.clear();
                break;
            }
            case EventType::BucketsLoadError: {
                auto& payload = std::get<ErrorPayload>(event.payload);
                LOG_F(WARNING, "Event: BucketsLoadError error=%s", payload.error_message.c_str());
                m_bucketsLoading = false;
                m_bucketsError = payload.error_message;
                break;
            }
            case EventType::ObjectsLoaded: {
                auto& payload = std::get<ObjectsLoadedPayload>(event.payload);
                LOG_F(INFO, "Event: ObjectsLoaded bucket=%s prefix=%s count=%zu truncated=%d",
                      payload.bucket.c_str(), payload.prefix.c_str(),
                      payload.objects.size(), payload.is_truncated);
                auto& node = getOrCreateNode(payload.bucket, payload.prefix);

                // If this is a continuation, append; otherwise replace
                if (payload.continuation_token.empty()) {
                    node.objects = std::move(payload.objects);
                } else {
                    for (auto& obj : payload.objects) {
                        node.objects.push_back(std::move(obj));
                    }
                }

                node.next_continuation_token = payload.next_continuation_token;
                node.is_truncated = payload.is_truncated;
                node.loading = false;
                node.error.clear();
                break;
            }
            case EventType::ObjectsLoadError: {
                auto& payload = std::get<ErrorPayload>(event.payload);
                LOG_F(WARNING, "Event: ObjectsLoadError bucket=%s prefix=%s error=%s",
                      payload.bucket.c_str(), payload.prefix.c_str(), payload.error_message.c_str());
                auto& node = getOrCreateNode(payload.bucket, payload.prefix);
                node.loading = false;
                node.error = payload.error_message;
                break;
            }
        }
    }
}

FolderNode* BrowserModel::getNode(const std::string& bucket, const std::string& prefix) {
    auto key = makeNodeKey(bucket, prefix);
    auto it = m_nodes.find(key);
    return (it != m_nodes.end()) ? &it->second : nullptr;
}

const FolderNode* BrowserModel::getNode(const std::string& bucket, const std::string& prefix) const {
    auto key = makeNodeKey(bucket, prefix);
    auto it = m_nodes.find(key);
    return (it != m_nodes.end()) ? &it->second : nullptr;
}

FolderNode& BrowserModel::getOrCreateNode(const std::string& bucket, const std::string& prefix) {
    auto key = makeNodeKey(bucket, prefix);
    auto& node = m_nodes[key];
    if (node.bucket.empty()) {
        node.bucket = bucket;
        node.prefix = prefix;
    }
    return node;
}

std::string BrowserModel::makeNodeKey(const std::string& bucket, const std::string& prefix) {
    return bucket + "/" + prefix;
}

void BrowserModel::setCurrentPath(const std::string& bucket, const std::string& prefix) {
    m_currentBucket = bucket;
    m_currentPrefix = prefix;
}

bool BrowserModel::parseS3Path(const std::string& path, std::string& bucket, std::string& prefix) {
    bucket.clear();
    prefix.clear();

    std::string p = path;

    // Remove s3:// prefix
    if (p.size() >= 5 && p.substr(0, 5) == "s3://") {
        p = p.substr(5);
    } else if (p.size() >= 3 && p.substr(0, 3) == "s3:") {
        p = p.substr(3);
    }

    // Remove leading slashes
    while (!p.empty() && p[0] == '/') {
        p = p.substr(1);
    }

    if (p.empty()) {
        return true;
    }

    // Split bucket/prefix
    size_t slash = p.find('/');
    if (slash == std::string::npos) {
        bucket = p;
    } else {
        bucket = p.substr(0, slash);
        prefix = p.substr(slash + 1);
    }

    return true;
}
