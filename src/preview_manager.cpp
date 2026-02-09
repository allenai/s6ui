#include "preview_manager.h"
#include "aws/s3_backend.h"
#include "loguru.hpp"
#include <unordered_set>
#include <algorithm>
#include <cctype>

PreviewManager::PreviewManager() = default;

PreviewManager::~PreviewManager() {
    if (m_streamingCancelFlag) {
        m_streamingCancelFlag->store(true);
    }
}

void PreviewManager::setBackend(IBackend* backend) {
    m_backend = backend;
}

std::string PreviewManager::previewContent() const {
    if (m_streamingPreview) {
        return m_streamingPreview->getAllContent();
    }
    return m_previewContent;
}

void PreviewManager::selectFile(const std::string& bucket, const std::string& key, int64_t fileSize) {
    // If same file is already selected, do nothing
    if (m_selectedBucket == bucket && m_selectedKey == key) {
        return;
    }

    // Cancel any existing streaming download
    cancelStreamingDownload();

    LOG_F(INFO, "Selecting file: bucket=%s key=%s", bucket.c_str(), key.c_str());
    m_selectedBucket = bucket;
    m_selectedKey = key;
    m_selectedFileSize = fileSize;
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

            // Always create StreamingFilePreview for unified data access
            startStreamingDownload(static_cast<size_t>(m_selectedFileSize));
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

void PreviewManager::clearSelection() {
    cancelStreamingDownload();
    m_selectedBucket.clear();
    m_selectedKey.clear();
    m_selectedFileSize = 0;
    m_previewContent.clear();
    m_previewError.clear();
    m_previewLoading = false;
    m_previewSupported = false;
}

void PreviewManager::prefetchFilePreview(const std::string& bucket, const std::string& key) {
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

    // Queue low-priority prefetch
    m_lastHoveredFile = cacheKey;
    LOG_F(INFO, "Prefetching file preview: bucket=%s key=%s", bucket.c_str(), key.c_str());
    m_backend->getObject(bucket, key, PREVIEW_MAX_BYTES, true /* lowPriority */, true /* cancellable */);
}

void PreviewManager::clearAll() {
    cancelStreamingDownload();
    clearSelection();
    m_previewCache.clear();
    m_pendingObjectRequests.clear();
    m_lastHoveredFile.clear();
}

void PreviewManager::onObjectContentLoaded(ObjectContentLoadedPayload& payload) {
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

        // Only start streaming if not already streaming this file.
        if (!m_streamingPreview ||
            m_streamingPreview->bucket() != payload.bucket ||
            m_streamingPreview->key() != payload.key) {
            startStreamingDownload(static_cast<size_t>(m_selectedFileSize));
        }
    }
}

void PreviewManager::onObjectContentLoadError(ObjectContentErrorPayload& payload) {
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
}

void PreviewManager::onObjectRangeLoaded(ObjectRangeLoadedPayload& payload) {
    LOG_F(INFO, "Event: ObjectRangeLoaded bucket=%s key=%s offset=%zu size=%zu total=%zu",
          payload.bucket.c_str(), payload.key.c_str(),
          payload.startByte, payload.data.size(), payload.totalSize);

    // Only process if this is for the current streaming preview
    if (m_streamingPreview &&
        payload.bucket == m_streamingPreview->bucket() &&
        payload.key == m_streamingPreview->key()) {
        m_streamingPreview->appendChunk(payload.data, payload.startByte);
    }
}

void PreviewManager::onObjectRangeLoadError(ObjectRangeErrorPayload& payload) {
    LOG_F(WARNING, "Event: ObjectRangeLoadError bucket=%s key=%s offset=%zu error=%s",
          payload.bucket.c_str(), payload.key.c_str(),
          payload.startByte, payload.error_message.c_str());

    // Only process if this is for the current streaming preview
    if (m_streamingPreview &&
        payload.bucket == m_streamingPreview->bucket() &&
        payload.key == m_streamingPreview->key()) {
        LOG_F(WARNING, "Streaming error at offset %zu, partial data available",
              payload.startByte);
    }
}

std::string PreviewManager::makePreviewCacheKey(const std::string& bucket, const std::string& key) {
    return bucket + "/" + key;
}

bool PreviewManager::isCompressed(const std::string& key) {
    size_t dotPos = key.rfind('.');
    if (dotPos == std::string::npos) return false;

    std::string ext = key.substr(dotPos);
    for (char& c : ext) c = std::tolower(static_cast<unsigned char>(c));

    return ext == ".gz" || ext == ".zst" || ext == ".zstd";
}

bool PreviewManager::isPreviewSupported(const std::string& key) {
    // Get file extension (lowercase)
    size_t dotPos = key.rfind('.');
    if (dotPos == std::string::npos) {
        return false;
    }
    std::string ext = key.substr(dotPos);
    for (char& c : ext) {
        c = std::tolower(static_cast<unsigned char>(c));
    }

    // If it's a compressed file (.gz, .zst, .zstd), check the underlying extension
    if ((ext == ".gz" || ext == ".zst" || ext == ".zstd") && dotPos > 0) {
        std::string withoutCompression = key.substr(0, dotPos);
        size_t innerDotPos = withoutCompression.rfind('.');
        if (innerDotPos == std::string::npos) {
            return false;
        }
        ext = withoutCompression.substr(innerDotPos);
        for (char& c : ext) {
            c = std::tolower(static_cast<unsigned char>(c));
        }
    }

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

void PreviewManager::startStreamingDownload(size_t totalFileSize) {
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
    m_streamingPreview = std::make_shared<StreamingFilePreview>(
        m_selectedBucket, m_selectedKey, m_previewContent, totalFileSize, std::move(transform));

    m_streamingEnabled = true;
    m_streamingCancelFlag = std::make_shared<std::atomic<bool>>(false);

    // Start streaming from where the initial preview left off
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

void PreviewManager::cancelStreamingDownload() {
    if (m_streamingCancelFlag) {
        m_streamingCancelFlag->store(true);
    }
    m_streamingCancelFlag.reset();
    m_streamingPreview.reset();
    m_streamingEnabled = false;
}
