#include "browser_model.h"
#include "loguru.hpp"
#include <algorithm>
#include <unordered_set>

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
    m_previewCache.clear();
    m_pendingObjectRequests.clear();
    m_lastHoveredFile.clear();
    m_lastHoveredFolder.clear();

    if (m_backend) {
        m_backend->listBuckets();
    }
}

void BrowserModel::loadFolder(const std::string& bucket, const std::string& prefix) {
    auto& node = getOrCreateNode(bucket, prefix);

    // If already loaded or loading, don't reload
    if (node.loaded || node.loading) return;

    LOG_F(INFO, "Loading folder: bucket=%s prefix=%s", bucket.c_str(), prefix.c_str());
    node.objects.clear();
    node.error.clear();
    node.loading = true;

    if (m_backend) {
        // Check if there's already a prefetch request queued for this folder
        // If so, prioritize it instead of making a new request
        if (!m_backend->prioritizeRequest(bucket, prefix)) {
            // No pending request, make a new high-priority one
            m_backend->listObjects(bucket, prefix);
        }
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
        // Already at root
        return;
    }

    if (m_currentPrefix.empty()) {
        // In a bucket root, go back to bucket list
        LOG_F(INFO, "Navigating up to bucket list");
        clearSelection();
        setCurrentPath("", "");
        return;
    }

    // Go up one level in the prefix
    // e.g., "foo/bar/baz/" -> "foo/bar/"
    std::string newPrefix = m_currentPrefix;

    // Remove trailing slash if present
    if (!newPrefix.empty() && newPrefix.back() == '/') {
        newPrefix.pop_back();
    }

    // Find the last slash
    size_t lastSlash = newPrefix.rfind('/');
    if (lastSlash == std::string::npos) {
        // No more slashes, go to bucket root
        newPrefix = "";
    } else {
        newPrefix = newPrefix.substr(0, lastSlash + 1);
    }

    LOG_F(INFO, "Navigating up from %s to %s", m_currentPrefix.c_str(), newPrefix.c_str());
    navigateInto(m_currentBucket, newPrefix);
}

void BrowserModel::navigateInto(const std::string& bucket, const std::string& prefix) {
    LOG_F(INFO, "Navigating into: bucket=%s prefix=%s", bucket.c_str(), prefix.c_str());
    clearSelection();  // Clear any selected file when navigating
    setCurrentPath(bucket, prefix);
    loadFolder(bucket, prefix);

    // If folder is already loaded (e.g. from prefetch), trigger prefetch for subfolders now
    // (since no ObjectsLoaded event will fire)
    const auto* node = getNode(bucket, prefix);
    if (node && node->loaded) {
        triggerPrefetch(bucket, node->objects);
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
    // If same file is already selected, do nothing
    if (m_selectedBucket == bucket && m_selectedKey == key) {
        return;
    }

    LOG_F(INFO, "Selecting file: bucket=%s key=%s", bucket.c_str(), key.c_str());
    m_selectedBucket = bucket;
    m_selectedKey = key;
    m_previewContent.clear();
    m_previewError.clear();
    m_previewSupported = isPreviewSupported(key);

    if (m_previewSupported && m_backend) {
        // Check if we have cached content from prefetch
        std::string cacheKey = makePreviewCacheKey(bucket, key);
        auto it = m_previewCache.find(cacheKey);
        if (it != m_previewCache.end()) {
            LOG_F(INFO, "Using cached preview for bucket=%s key=%s", bucket.c_str(), key.c_str());
            m_previewContent = it->second;
            m_previewLoading = false;
            return;
        }

        // Check if there's a pending prefetch request we can boost
        if (m_backend->prioritizeObjectRequest(bucket, key)) {
            LOG_F(INFO, "Boosted prefetch request for bucket=%s key=%s", bucket.c_str(), key.c_str());
            m_previewLoading = true;
            return;
        }

        // No cache, no pending request - make a new high-priority request
        m_previewLoading = true;
        m_pendingObjectRequests.insert(cacheKey);
        m_backend->getObject(bucket, key, PREVIEW_MAX_BYTES);
    } else {
        m_previewLoading = false;
    }
}

void BrowserModel::prefetchFilePreview(const std::string& bucket, const std::string& key) {
    if (!m_backend) return;

    // Only prefetch supported file types
    if (!isPreviewSupported(key)) return;

    // Skip if already cached
    std::string cacheKey = makePreviewCacheKey(bucket, key);
    if (m_previewCache.find(cacheKey) != m_previewCache.end()) return;

    // Skip if already selected (will be fetched high-priority)
    if (m_selectedBucket == bucket && m_selectedKey == key) return;

    // Skip if this is the same file we're already fetching (avoid re-queueing every frame)
    if (m_lastHoveredFile == cacheKey) return;

    // Queue low-priority prefetch - the backend handles cancellation of stale requests.
    // Tracking the hover target ensures we only queue when it changes, not every frame.
    m_lastHoveredFile = cacheKey;
    LOG_F(INFO, "Prefetching file preview: bucket=%s key=%s", bucket.c_str(), key.c_str());
    m_backend->getObject(bucket, key, PREVIEW_MAX_BYTES, true /* lowPriority */);
}

std::string BrowserModel::makePreviewCacheKey(const std::string& bucket, const std::string& key) {
    return bucket + "/" + key;
}

void BrowserModel::prefetchFolder(const std::string& bucket, const std::string& prefix) {
    if (!m_backend) return;

    // Skip if already loaded or loading
    const auto* node = getNode(bucket, prefix);
    if (node && (node->loaded || node->loading)) return;

    // Skip if this is the same folder we're already fetching (avoid re-queueing every frame)
    std::string folderKey = bucket + "/" + prefix;
    if (m_lastHoveredFolder == folderKey) return;

    // Queue low-priority prefetch - the backend handles cancellation of stale requests.
    // Tracking the hover target ensures we only queue when it changes, not every frame.
    m_lastHoveredFolder = folderKey;
    LOG_F(INFO, "Prefetching folder on hover: bucket=%s prefix=%s", bucket.c_str(), prefix.c_str());
    m_backend->listObjectsPrefetch(bucket, prefix);
}

void BrowserModel::clearSelection() {
    m_selectedBucket.clear();
    m_selectedKey.clear();
    m_previewContent.clear();
    m_previewError.clear();
    m_previewLoading = false;
    m_previewSupported = false;
}

bool BrowserModel::isPreviewSupported(const std::string& key) {
    // Get file extension (lowercase)
    size_t dotPos = key.rfind('.');
    if (dotPos == std::string::npos) {
        return false;
    }
    std::string ext = key.substr(dotPos);
    // Convert to lowercase
    for (char& c : ext) {
        c = std::tolower(static_cast<unsigned char>(c));
    }

    // Supported text extensions
    static const std::unordered_set<std::string> supportedExtensions = {
        // Plain text and documentation
        ".txt", ".md", ".markdown", ".rst", ".rtf", ".tex", ".log", ".readme",

        // Web markup and data
        ".html", ".htm", ".xhtml", ".xml", ".svg", ".css", ".scss", ".sass", ".less",

        // Data formats
        ".json", ".jsonl", ".ndjson", ".yaml", ".yml", ".toml", ".csv", ".tsv",
        ".ini", ".cfg", ".conf", ".properties", ".env",

        // Programming languages - C family
        ".c", ".h", ".cpp", ".hpp", ".cc", ".hh", ".cxx", ".hxx", ".c++", ".h++",
        ".m", ".mm",  // Objective-C

        // Programming languages - JVM
        ".java", ".kt", ".kts", ".scala", ".groovy", ".gradle",

        // Programming languages - Scripting
        ".py", ".pyw", ".pyi",  // Python
        ".js", ".mjs", ".cjs", ".jsx",  // JavaScript
        ".ts", ".tsx", ".mts", ".cts",  // TypeScript
        ".rb", ".rake", ".gemspec",  // Ruby
        ".php", ".phtml",  // PHP
        ".pl", ".pm", ".pod",  // Perl
        ".lua",
        ".r", ".rmd",  // R

        // Programming languages - Systems
        ".go",
        ".rs",  // Rust
        ".swift",
        ".zig",
        ".nim",
        ".v",  // V lang
        ".d",  // D lang

        // Programming languages - Functional
        ".hs", ".lhs",  // Haskell
        ".ml", ".mli",  // OCaml
        ".fs", ".fsi", ".fsx",  // F#
        ".ex", ".exs",  // Elixir
        ".erl", ".hrl",  // Erlang
        ".clj", ".cljs", ".cljc", ".edn",  // Clojure
        ".lisp", ".cl", ".el",  // Lisp variants
        ".scm", ".ss",  // Scheme

        // Shell scripts
        ".sh", ".bash", ".zsh", ".fish", ".ksh", ".csh", ".tcsh",
        ".ps1", ".psm1", ".psd1",  // PowerShell
        ".bat", ".cmd",  // Windows batch

        // Database and query
        ".sql", ".mysql", ".pgsql", ".sqlite",
        ".graphql", ".gql",

        // DevOps and infrastructure
        ".dockerfile", ".tf", ".tfvars", ".hcl",
        ".vagrantfile", ".ansible",

        // Build and config files
        ".cmake", ".make", ".makefile", ".mk",
        ".ninja",
        ".bazel", ".bzl",
        ".sbt",

        // Version control and editor config
        ".gitignore", ".gitattributes", ".gitmodules",
        ".editorconfig", ".prettierrc", ".eslintrc",

        // Serialization and schemas
        ".proto", ".thrift", ".avsc",
        ".xsd", ".dtd", ".wsdl",

        // Diff and patches
        ".diff", ".patch",

        // Assembly
        ".asm", ".s", ".S",

        // Other
        ".vim", ".vimrc",
        ".tmux",
        ".zshrc", ".bashrc", ".profile",
        ".htaccess", ".nginx",
        ".plist",
        ".reg",  // Windows registry
    };

    return supportedExtensions.find(ext) != supportedExtensions.end();
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
                node.loaded = true;
                node.error.clear();

                // Trigger prefetch for subfolders if this is the currently viewed folder
                if (payload.bucket == m_currentBucket && payload.prefix == m_currentPrefix) {
                    triggerPrefetch(payload.bucket, node.objects);
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
                LOG_F(INFO, "Event: ObjectContentLoaded bucket=%s key=%s size=%zu",
                      payload.bucket.c_str(), payload.key.c_str(), payload.content.size());

                // Cache the content for future use
                std::string cacheKey = makePreviewCacheKey(payload.bucket, payload.key);
                m_previewCache[cacheKey] = payload.content;
                m_pendingObjectRequests.erase(cacheKey);

                // Update preview if this is the selected file
                if (payload.bucket == m_selectedBucket && payload.key == m_selectedKey) {
                    m_previewContent = std::move(payload.content);
                    m_previewLoading = false;
                    m_previewError.clear();
                }
                break;
            }
            case EventType::ObjectContentLoadError: {
                auto& payload = std::get<ObjectContentErrorPayload>(event.payload);
                LOG_F(WARNING, "Event: ObjectContentLoadError bucket=%s key=%s error=%s",
                      payload.bucket.c_str(), payload.key.c_str(), payload.error_message.c_str());

                // Remove from pending tracking
                std::string cacheKey = makePreviewCacheKey(payload.bucket, payload.key);
                m_pendingObjectRequests.erase(cacheKey);

                // Only update if this is still the selected file
                if (payload.bucket == m_selectedBucket && payload.key == m_selectedKey) {
                    m_previewLoading = false;
                    m_previewError = payload.error_message;
                }
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

void BrowserModel::triggerPrefetch(const std::string& bucket, const std::vector<S3Object>& objects) {
    if (!m_backend) return;

    // Only prefetch subfolders, limit to first 20 to avoid overwhelming
    constexpr size_t MAX_PREFETCH = 20;
    size_t prefetch_count = 0;

    for (const auto& obj : objects) {
        if (!obj.is_folder) continue;
        if (prefetch_count >= MAX_PREFETCH) break;

        // Skip if already loaded or loading
        const auto* node = getNode(bucket, obj.key);
        if (node && (node->loaded || node->loading)) continue;

        // Skip if already queued
        if (m_backend->hasPendingRequest(bucket, obj.key)) continue;

        // Queue low-priority prefetch request
        LOG_F(INFO, "Prefetching: bucket=%s prefix=%s", bucket.c_str(), obj.key.c_str());
        m_backend->listObjectsPrefetch(bucket, obj.key);
        prefetch_count++;
    }

    if (prefetch_count > 0) {
        LOG_F(INFO, "Queued %zu prefetch requests for bucket=%s", prefetch_count, bucket.c_str());
    }
}
