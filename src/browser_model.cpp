#include "browser_model.h"
#include "loguru.hpp"
#include <unordered_set>
#include <cstdlib>

BrowserModel::BrowserModel() = default;

BrowserModel::~BrowserModel() {
    // Cancel any streaming downloads so worker threads can exit
    if (m_streamingCancelFlag) {
        m_streamingCancelFlag->store(true);
    }

    // Cancel any pending pagination requests
    if (m_paginationCancelFlag) {
        m_paginationCancelFlag->store(true);
    }
}

void BrowserModel::setBackend(std::unique_ptr<IBackend> backend) {
    LOG_F(INFO, "Setting backend");
    m_backend = std::move(backend);
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

    LOG_F(INFO, "Loading more objects: bucket=%s prefix=%s token=%s",
          bucket.c_str(), prefix.c_str(),
          node->next_continuation_token.empty() ? "(none)" : node->next_continuation_token.substr(0, 20).c_str());
    node->loading = true;

    if (m_backend) {
        // Use the shared cancel flag so all pagination requests can be cancelled together
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

    // If folder is already loaded (e.g. from prefetch or returning to a previous folder)
    const auto* node = getNode(bucket, prefix);
    if (node && node->loaded) {
        // Trigger prefetch for subfolders (since no ObjectsLoaded event will fire)
        triggerPrefetch(bucket, node->objects);

        // Resume pagination if the folder was truncated (we navigated away mid-load)
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

std::string BrowserModel::previewContent() const {
    // If we have a streaming preview, return its decompressed content
    if (m_streamingPreview) {
        return m_streamingPreview->getAllContent();
    }
    // Otherwise return the raw preview content
    return m_previewContent;
}

void BrowserModel::selectFile(const std::string& bucket, const std::string& key) {
    // If same file is already selected, do nothing
    if (m_selectedBucket == bucket && m_selectedKey == key) {
        return;
    }

    // Cancel any existing streaming download
    cancelStreamingDownload();

    LOG_F(INFO, "Selecting file: bucket=%s key=%s", bucket.c_str(), key.c_str());
    m_selectedBucket = bucket;
    m_selectedKey = key;
    m_previewContent.clear();
    m_previewError.clear();
    m_previewSupported = isPreviewSupported(key);

    // Find the file size from the folder node
    m_selectedFileSize = 0;
    const auto* node = getNode(m_currentBucket, m_currentPrefix);
    if (node) {
        for (const auto& obj : node->objects) {
            if (!obj.is_folder && obj.key == key) {
                m_selectedFileSize = obj.size;
                break;
            }
        }
    }

    if (m_previewSupported && m_backend) {
        // Check if we have cached content from prefetch
        std::string cacheKey = makePreviewCacheKey(bucket, key);
        auto it = m_previewCache.find(cacheKey);
        if (it != m_previewCache.end()) {
            LOG_F(INFO, "Using cached preview for bucket=%s key=%s", bucket.c_str(), key.c_str());
            m_previewContent = it->second;
            m_previewLoading = false;

            // Start streaming for gzipped files or large files
            bool needsStreaming = isCompressed(key) ||
                static_cast<size_t>(m_selectedFileSize) > STREAMING_THRESHOLD;
            if (needsStreaming) {
                startStreamingDownload(static_cast<size_t>(m_selectedFileSize));
            }
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
    // cancellable=true so newer hover targets cancel this request.
    m_lastHoveredFile = cacheKey;
    LOG_F(INFO, "Prefetching file preview: bucket=%s key=%s", bucket.c_str(), key.c_str());
    m_backend->getObject(bucket, key, PREVIEW_MAX_BYTES, true /* lowPriority */, true /* cancellable */);
}

std::string BrowserModel::makePreviewCacheKey(const std::string& bucket, const std::string& key) {
    return bucket + "/" + key;
}

bool BrowserModel::isCompressed(const std::string& key) {
    size_t dotPos = key.rfind('.');
    if (dotPos == std::string::npos) return false;

    std::string ext = key.substr(dotPos);
    for (char& c : ext) c = std::tolower(static_cast<unsigned char>(c));

    return ext == ".gz" || ext == ".zst" || ext == ".zstd";
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
    // This prevents the folder from being stuck in "loading" state if the request is cancelled
    // before completion. If the request already completed, this is harmless (loaded=true takes precedence).
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

    // Create node and mark as loading to prevent loadFolder() from making a duplicate request
    // if user clicks before the prefetch completes.
    auto& newNode = getOrCreateNode(bucket, prefix);
    newNode.loading = true;

    m_lastHoveredFolder = folderKey;
    LOG_F(INFO, "Prefetching folder on hover: bucket=%s prefix=%s", bucket.c_str(), prefix.c_str());
    m_backend->listObjectsPrefetch(bucket, prefix, true /* cancellable */);
}

void BrowserModel::clearSelection() {
    cancelStreamingDownload();
    m_selectedBucket.clear();
    m_selectedKey.clear();
    m_selectedFileSize = 0;
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

    // If it's a compressed file (.gz, .zst, .zstd), check the underlying extension
    if ((ext == ".gz" || ext == ".zst" || ext == ".zstd") && dotPos > 0) {
        // Strip compression extension and find the real extension
        std::string withoutCompression = key.substr(0, dotPos);
        size_t innerDotPos = withoutCompression.rfind('.');
        if (innerDotPos == std::string::npos) {
            return false;  // No inner extension (e.g., just "file.gz")
        }
        ext = withoutCompression.substr(innerDotPos);
        for (char& c : ext) {
            c = std::tolower(static_cast<unsigned char>(c));
        }
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

        // Images (supported by stb_image)
        ".png", ".jpg", ".jpeg", ".gif", ".bmp", ".psd", ".tga", ".hdr", ".pic", ".pnm", ".pgm", ".ppm",

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
                    // Build a set of existing keys to avoid duplicates
                    // (can happen if multiple requests were in flight for the same folder)
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

                // If this is the currently viewed folder, handle auto-pagination and prefetch
                if (payload.bucket == m_currentBucket && payload.prefix == m_currentPrefix) {
                    // Auto-continue pagination if there are more results
                    if (node.is_truncated) {
                        LOG_F(INFO, "Auto-continuing pagination for current folder: %s/%s",
                              payload.bucket.c_str(), payload.prefix.c_str());
                        loadMore(payload.bucket, payload.prefix);
                    }

                    // Only trigger prefetch on initial load, not on pagination continuations
                    // This prevents prefetching more and more folders as pagination progresses
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
                LOG_F(INFO, "Event: ObjectContentLoaded bucket=%s key=%s size=%zu",
                      payload.bucket.c_str(), payload.key.c_str(), payload.content.size());

                // Cache the raw content for future use
                std::string cacheKey = makePreviewCacheKey(payload.bucket, payload.key);
                m_previewCache[cacheKey] = payload.content;
                m_pendingObjectRequests.erase(cacheKey);

                // Update preview if this is the selected file
                if (payload.bucket == m_selectedBucket && payload.key == m_selectedKey) {
                    m_previewContent = payload.content;
                    m_previewLoading = false;
                    m_previewError.clear();

                    // Start streaming for:
                    // 1. Gzipped files (always, for transparent decompression)
                    // 2. Large files that need more data
                    bool needsStreaming = isCompressed(payload.key) ||
                        (static_cast<size_t>(m_selectedFileSize) > STREAMING_THRESHOLD &&
                         static_cast<size_t>(m_selectedFileSize) > payload.content.size());

                    if (needsStreaming) {
                        startStreamingDownload(static_cast<size_t>(m_selectedFileSize));
                    }
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
            case EventType::ObjectRangeLoaded: {
                auto& payload = std::get<ObjectRangeLoadedPayload>(event.payload);
                LOG_F(INFO, "Event: ObjectRangeLoaded bucket=%s key=%s offset=%zu size=%zu total=%zu",
                      payload.bucket.c_str(), payload.key.c_str(),
                      payload.startByte, payload.data.size(), payload.totalSize);

                // Only process if this is for the current streaming preview
                if (m_streamingPreview &&
                    payload.bucket == m_streamingPreview->bucket() &&
                    payload.key == m_streamingPreview->key()) {

                    m_streamingPreview->appendChunk(payload.data, payload.startByte);
                    // Chunks arrive automatically from the single streaming request
                    // No need to request next chunk - CURL streams them as they arrive
                }
                break;
            }
            case EventType::ObjectRangeLoadError: {
                auto& payload = std::get<ObjectRangeErrorPayload>(event.payload);
                LOG_F(WARNING, "Event: ObjectRangeLoadError bucket=%s key=%s offset=%zu error=%s",
                      payload.bucket.c_str(), payload.key.c_str(),
                      payload.startByte, payload.error_message.c_str());

                // Only process if this is for the current streaming preview
                if (m_streamingPreview &&
                    payload.bucket == m_streamingPreview->bucket() &&
                    payload.key == m_streamingPreview->key()) {
                    // Log error but don't stop streaming - the partial data is still usable
                    LOG_F(WARNING, "Streaming error at offset %zu, partial data available",
                          payload.startByte);
                }
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
    // If changing folders, cancel any pending pagination requests for the old folder
    if (bucket != m_currentBucket || prefix != m_currentPrefix) {
        if (m_paginationCancelFlag) {
            LOG_F(INFO, "Cancelling pagination for old folder: %s/%s",
                  m_currentBucket.c_str(), m_currentPrefix.c_str());
            m_paginationCancelFlag->store(true);

            // Reset loading state for the old folder since we cancelled its requests
            auto* oldNode = getNode(m_currentBucket, m_currentPrefix);
            if (oldNode) {
                oldNode->loading = false;
            }
        }
        // Create a new cancel flag for the new folder's pagination
        m_paginationCancelFlag = std::make_shared<std::atomic<bool>>(false);
    }

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

void BrowserModel::startStreamingDownload(size_t totalFileSize) {
    if (!m_backend || m_selectedBucket.empty() || m_selectedKey.empty()) {
        return;
    }

    // Cancel any existing streaming
    cancelStreamingDownload();

    LOG_F(INFO, "Starting streaming download: bucket=%s key=%s totalSize=%zu",
          m_selectedBucket.c_str(), m_selectedKey.c_str(), totalFileSize);

    // Check if the file is compressed and needs decompression
    std::unique_ptr<IStreamTransform> transform;
    size_t dotPos = m_selectedKey.rfind('.');
    if (dotPos != std::string::npos) {
        std::string ext = m_selectedKey.substr(dotPos);
        for (char& c : ext) c = std::tolower(static_cast<unsigned char>(c));

        if (ext == ".gz") {
            LOG_F(INFO, "Using GzipTransform for gzipped file: %s", m_selectedKey.c_str());
            transform = std::make_unique<GzipTransform>();
        } else if (ext == ".zst" || ext == ".zstd") {
            LOG_F(INFO, "Using ZstdTransform for zstd file: %s", m_selectedKey.c_str());
            transform = std::make_unique<ZstdTransform>();
        }
    }

    // Create streaming preview with the initial preview content
    m_streamingPreview = std::make_unique<StreamingFilePreview>(
        m_selectedBucket, m_selectedKey, m_previewContent, totalFileSize, std::move(transform));

    m_streamingEnabled = true;
    m_streamingCancelFlag = std::make_shared<std::atomic<bool>>(false);

    // Start streaming from where the initial preview left off
    // Use single streaming request instead of multiple chunk requests
    size_t startByte = m_streamingPreview->nextByteNeeded();
    if (startByte < totalFileSize) {
        LOG_F(INFO, "Starting single streaming request from byte %zu", startByte);
        m_backend->getObjectStreaming(
            m_selectedBucket,
            m_selectedKey,
            startByte,
            totalFileSize,
            m_streamingCancelFlag);
    }
}

void BrowserModel::cancelStreamingDownload() {
    if (m_streamingCancelFlag) {
        m_streamingCancelFlag->store(true);
    }
    m_streamingCancelFlag.reset();
    m_streamingPreview.reset();
    m_streamingEnabled = false;
}
