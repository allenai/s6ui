#include "browser_model.h"
#include "loguru.hpp"
#include <unordered_set>
#include <algorithm>
#include <cstdlib>
#include <ctime>

BrowserModel::BrowserModel() = default;

BrowserModel::~BrowserModel() {
    // Cancel any pending pagination requests
    if (m_paginationCancelFlag) {
        m_paginationCancelFlag->store(true);
    }
}

void BrowserModel::setSettings(AppSettings settings) {
    m_settings = std::move(settings);
}

void BrowserModel::recordRecentPath(const std::string& path) {
    if (path.empty() || path == "s3://") return;

    std::string profileName;
    if (m_selectedProfileIdx >= 0 && m_selectedProfileIdx < static_cast<int>(m_profiles.size())) {
        profileName = m_profiles[m_selectedProfileIdx].name;
    }
    if (profileName.empty()) return;

    auto& entries = m_settings.frecent_paths[profileName];
    int64_t now = static_cast<int64_t>(std::time(nullptr));

    // Find existing entry or create new one
    auto it = std::find_if(entries.begin(), entries.end(),
        [&path](const PathEntry& e) { return e.path == path; });

    if (it != entries.end()) {
        it->score += 1.0;
        it->last_accessed = now;
    } else {
        PathEntry entry;
        entry.path = path;
        entry.score = 1.0;
        entry.last_accessed = now;
        entries.push_back(std::move(entry));
    }

    // Cap at 500 stored entries - remove lowest-scoring entries
    if (entries.size() > 500) {
        std::sort(entries.begin(), entries.end(),
            [](const PathEntry& a, const PathEntry& b) { return a.score > b.score; });
        entries.resize(500);
    }
}

// Compute effective frecency score using time-decay buckets (z/zoxide style)
static double frecencyScore(const PathEntry& entry, int64_t now) {
    int64_t age = now - entry.last_accessed;
    double weight;
    if (age < 3600) {           // last hour
        weight = 4.0;
    } else if (age < 86400) {   // last day
        weight = 2.0;
    } else if (age < 604800) {  // last week
        weight = 1.0;
    } else {
        weight = 0.5;
    }
    return entry.score * weight;
}

std::vector<std::string> BrowserModel::topFrecentPaths(size_t count) const {
    std::string profileName;
    if (m_selectedProfileIdx >= 0 && m_selectedProfileIdx < static_cast<int>(m_profiles.size())) {
        profileName = m_profiles[m_selectedProfileIdx].name;
    }

    auto it = m_settings.frecent_paths.find(profileName);
    if (it == m_settings.frecent_paths.end() || it->second.empty()) {
        return {};
    }

    int64_t now = static_cast<int64_t>(std::time(nullptr));
    const auto& entries = it->second;

    // Build scored index pairs
    std::vector<std::pair<double, size_t>> scored;
    scored.reserve(entries.size());
    for (size_t i = 0; i < entries.size(); ++i) {
        scored.push_back({frecencyScore(entries[i], now), i});
    }

    // Partial sort for top N
    size_t n = std::min(count, scored.size());
    std::partial_sort(scored.begin(), scored.begin() + static_cast<ptrdiff_t>(n), scored.end(),
        [](const auto& a, const auto& b) { return a.first > b.first; });

    std::vector<std::string> result;
    result.reserve(n);
    for (size_t i = 0; i < n; ++i) {
        result.push_back(entries[scored[i].second].path);
    }
    return result;
}

void BrowserModel::setBackend(std::unique_ptr<IBackend> backend) {
    LOG_F(INFO, "Setting backend");
    m_backend = std::move(backend);
    m_preview.setBackend(m_backend.get());
}

void BrowserModel::loadProfiles() {
    LOG_F(INFO, "Loading AWS profiles");
    m_profiles = load_aws_profiles();
    m_selectedProfileIdx = 0;

    // Check for AWS_PROFILE environment variable
    const char* aws_profile_env = std::getenv("AWS_PROFILE");
    if (aws_profile_env) {
        std::string profile_name(aws_profile_env);
        for (size_t i = 0; i < m_profiles.size(); i++) {
            if (m_profiles[i].name == profile_name) {
                m_selectedProfileIdx = static_cast<int>(i);
                LOG_F(INFO, "Selected profile from AWS_PROFILE: %s", profile_name.c_str());
                break;
            }
        }
    } else {
        // No AWS_PROFILE set, look for a profile named "default"
        for (size_t i = 0; i < m_profiles.size(); i++) {
            if (m_profiles[i].name == "default") {
                m_selectedProfileIdx = static_cast<int>(i);
                LOG_F(INFO, "Selected profile 'default'");
                break;
            }
        }
    }

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
    m_preview.clearAll();

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
    m_preview.clearAll();
    m_lastHoveredFolder.clear();

    // Cancel any pending pagination requests
    if (m_paginationCancelFlag) {
        m_paginationCancelFlag->store(true);
    }
    m_paginationCancelFlag.reset();

    if (m_backend) {
        m_backend->listBuckets();
    }
}

void BrowserModel::loadFolder(const std::string& bucket, const std::string& prefix) {
    auto& node = getOrCreateNode(bucket, prefix);

    // If already loaded, don't reload
    if (node.loaded) return;

    // Try to boost any pending prefetch request to high priority.
    if (m_backend && m_backend->prioritizeRequest(bucket, prefix)) {
        node.loading = true;
        LOG_F(INFO, "Boosted pending prefetch for folder: bucket=%s prefix=%s",
              bucket.c_str(), prefix.c_str());
        return;
    }

    LOG_F(INFO, "Loading folder: bucket=%s prefix=%s", bucket.c_str(), prefix.c_str());
    node.objects.clear();
    node.error.clear();
    node.loading = true;

    if (m_backend) {
        m_backend->listObjects(bucket, prefix);
    }
}

void BrowserModel::loadMore(const std::string& bucket, const std::string& prefix) {
    auto* node = getNode(bucket, prefix);
    if (!node || !node->is_truncated || node->loading) return;

    LOG_F(INFO, "Loading more objects: bucket=%s prefix=%s token=%s",
          bucket.c_str(), prefix.c_str(),
          node->next_continuation_token.empty() ? "(none)" : node->next_continuation_token.substr(0, 20).c_str());
    node->loading = true;

    if (m_backend) {
        m_backend->listObjects(bucket, prefix, node->next_continuation_token, m_paginationCancelFlag);
    }
}

void BrowserModel::navigateTo(const std::string& s3_path) {
    LOG_F(INFO, "Navigating to: %s", s3_path.c_str());
    std::string bucket, prefix;
    if (!parseS3Path(s3_path, bucket, prefix)) return;

    if (bucket.empty()) {
        // Navigate to root (bucket list)
        clearSelection();
        setCurrentPath("", "");
        return;
    }

    // Add bucket if not in list
    addManualBucket(bucket);

    // Navigate to the specified location
    navigateInto(bucket, prefix);
}

void BrowserModel::navigateUp() {
    if (m_currentBucket.empty()) {
        return;
    }

    if (m_currentPrefix.empty()) {
        LOG_F(INFO, "Navigating up to bucket list");
        clearSelection();
        setCurrentPath("", "");
        return;
    }

    // Go up one level in the prefix
    std::string newPrefix = m_currentPrefix;

    if (!newPrefix.empty() && newPrefix.back() == '/') {
        newPrefix.pop_back();
    }

    size_t lastSlash = newPrefix.rfind('/');
    if (lastSlash == std::string::npos) {
        newPrefix = "";
    } else {
        newPrefix = newPrefix.substr(0, lastSlash + 1);
    }

    LOG_F(INFO, "Navigating up from %s to %s", m_currentPrefix.c_str(), newPrefix.c_str());
    navigateInto(m_currentBucket, newPrefix);
}

void BrowserModel::navigateInto(const std::string& bucket, const std::string& prefix) {
    LOG_F(INFO, "Navigating into: bucket=%s prefix=%s", bucket.c_str(), prefix.c_str());
    clearSelection();
    setCurrentPath(bucket, prefix);
    loadFolder(bucket, prefix);

    // Record in recent paths
    if (!bucket.empty()) {
        std::string path = "s3://" + bucket + "/" + prefix;
        recordRecentPath(path);
    }

    // If folder is already loaded (e.g. from prefetch or returning to a previous folder)
    const auto* node = getNode(bucket, prefix);
    if (node && node->loaded) {
        triggerPrefetch(bucket, node->objects);

        if (node->is_truncated && !node->loading) {
            LOG_F(INFO, "Resuming pagination for folder: bucket=%s prefix=%s",
                  bucket.c_str(), prefix.c_str());
            loadMore(bucket, prefix);
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

void BrowserModel::selectFile(const std::string& bucket, const std::string& key) {
    // Look up fileSize from the current folder node
    int64_t fileSize = 0;
    const auto* node = getNode(m_currentBucket, m_currentPrefix);
    if (node) {
        for (const auto& obj : node->objects) {
            if (!obj.is_folder && obj.key == key) {
                fileSize = obj.size;
                break;
            }
        }
    }
    m_preview.selectFile(bucket, key, fileSize);
}

void BrowserModel::clearSelection() {
    m_preview.clearSelection();
}

void BrowserModel::prefetchFilePreview(const std::string& bucket, const std::string& key) {
    m_preview.prefetchFilePreview(bucket, key);
}

void BrowserModel::prefetchFolder(const std::string& bucket, const std::string& prefix) {
    if (!m_backend) return;

    // Skip if already loaded or loading
    auto* node = getNode(bucket, prefix);
    if (node && (node->loaded || node->loading)) return;

    // Skip if this is the same folder we're already fetching (avoid re-queueing every frame)
    std::string folderKey = bucket + "/" + prefix;
    if (m_lastHoveredFolder == folderKey) return;

    // Reset loading state for the previous hover-prefetch folder since we're cancelling it.
    if (!m_lastHoveredFolder.empty()) {
        size_t slashPos = m_lastHoveredFolder.find('/');
        if (slashPos != std::string::npos) {
            std::string oldBucket = m_lastHoveredFolder.substr(0, slashPos);
            std::string oldPrefix = m_lastHoveredFolder.substr(slashPos + 1);
            auto* oldNode = getNode(oldBucket, oldPrefix);
            if (oldNode && oldNode->loading && !oldNode->loaded) {
                oldNode->loading = false;
            }
        }
    }

    auto& newNode = getOrCreateNode(bucket, prefix);
    newNode.loading = true;

    m_lastHoveredFolder = folderKey;
    LOG_F(INFO, "Prefetching folder on hover: bucket=%s prefix=%s", bucket.c_str(), prefix.c_str());
    m_backend->listObjectsPrefetch(bucket, prefix, true /* cancellable */);
}

bool BrowserModel::processEvents() {
    if (!m_backend) return false;

    auto events = m_backend->takeEvents();
    if (events.empty()) return false;

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
                LOG_F(INFO, "Event: ObjectsLoaded bucket=%s prefix=%s count=%zu truncated=%d total=%zu",
                      payload.bucket.c_str(), payload.prefix.c_str(),
                      payload.objects.size(), payload.is_truncated,
                      getNode(payload.bucket, payload.prefix) ?
                          getNode(payload.bucket, payload.prefix)->objects.size() + payload.objects.size() : payload.objects.size());
                auto& node = getOrCreateNode(payload.bucket, payload.prefix);

                // If this is a continuation, append; otherwise replace
                if (payload.continuation_token.empty()) {
                    node.objects = std::move(payload.objects);
                } else {
                    std::unordered_set<std::string> existingKeys;
                    existingKeys.reserve(node.objects.size());
                    for (const auto& obj : node.objects) {
                        existingKeys.insert(obj.key);
                    }

                    for (auto& obj : payload.objects) {
                        if (existingKeys.find(obj.key) == existingKeys.end()) {
                            node.objects.push_back(std::move(obj));
                        }
                    }
                }

                node.next_continuation_token = payload.next_continuation_token;
                node.is_truncated = payload.is_truncated;
                node.loading = false;
                node.loaded = true;
                node.error.clear();

                if (payload.bucket == m_currentBucket && payload.prefix == m_currentPrefix) {
                    if (node.is_truncated) {
                        LOG_F(INFO, "Auto-continuing pagination for current folder: %s/%s",
                              payload.bucket.c_str(), payload.prefix.c_str());
                        loadMore(payload.bucket, payload.prefix);
                    }

                    if (payload.continuation_token.empty()) {
                        triggerPrefetch(payload.bucket, node.objects);
                    }
                }
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
            case EventType::ObjectContentLoaded: {
                auto& payload = std::get<ObjectContentLoadedPayload>(event.payload);
                m_preview.onObjectContentLoaded(payload);
                break;
            }
            case EventType::ObjectContentLoadError: {
                auto& payload = std::get<ObjectContentErrorPayload>(event.payload);
                m_preview.onObjectContentLoadError(payload);
                break;
            }
            case EventType::ObjectRangeLoaded: {
                auto& payload = std::get<ObjectRangeLoadedPayload>(event.payload);
                m_preview.onObjectRangeLoaded(payload);
                break;
            }
            case EventType::ObjectRangeLoadError: {
                auto& payload = std::get<ObjectRangeErrorPayload>(event.payload);
                m_preview.onObjectRangeLoadError(payload);
                break;
            }
        }
    }
    return true;
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
    if (bucket != m_currentBucket || prefix != m_currentPrefix) {
        if (m_paginationCancelFlag) {
            LOG_F(INFO, "Cancelling pagination for old folder: %s/%s",
                  m_currentBucket.c_str(), m_currentPrefix.c_str());
            m_paginationCancelFlag->store(true);

            auto* oldNode = getNode(m_currentBucket, m_currentPrefix);
            if (oldNode) {
                oldNode->loading = false;
            }
        }
        m_paginationCancelFlag = std::make_shared<std::atomic<bool>>(false);
    }

    m_currentBucket = bucket;
    m_currentPrefix = prefix;
}

bool BrowserModel::parseS3Path(const std::string& path, std::string& bucket, std::string& prefix) {
    bucket.clear();
    prefix.clear();

    std::string p = path;

    if (p.size() >= 5 && p.substr(0, 5) == "s3://") {
        p = p.substr(5);
    } else if (p.size() >= 3 && p.substr(0, 3) == "s3:") {
        p = p.substr(3);
    }

    while (!p.empty() && p[0] == '/') {
        p = p.substr(1);
    }

    if (p.empty()) {
        return true;
    }

    size_t slash = p.find('/');
    if (slash == std::string::npos) {
        bucket = p;
    } else {
        bucket = p.substr(0, slash);
        prefix = p.substr(slash + 1);
    }

    return true;
}

void BrowserModel::triggerPrefetch(const std::string& bucket, const std::vector<S3Object>& objects) {
    if (!m_backend) return;

    constexpr size_t MAX_PREFETCH = 20;
    size_t prefetch_count = 0;

    for (const auto& obj : objects) {
        if (!obj.is_folder) continue;
        if (prefetch_count >= MAX_PREFETCH) break;

        const auto* node = getNode(bucket, obj.key);
        if (node && (node->loaded || node->loading)) continue;

        if (m_backend->hasPendingRequest(bucket, obj.key)) continue;

        LOG_F(INFO, "Prefetching: bucket=%s prefix=%s", bucket.c_str(), obj.key.c_str());
        m_backend->listObjectsPrefetch(bucket, obj.key);
        prefetch_count++;
    }

    if (prefetch_count > 0) {
        LOG_F(INFO, "Queued %zu prefetch requests for bucket=%s", prefetch_count, bucket.c_str());
    }
}
