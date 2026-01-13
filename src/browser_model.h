#pragma once

#include "backend.h"
#include "aws/s3_backend.h"
#include "aws/aws_credentials.h"
#include "streaming_preview.h"
#include <string>
#include <vector>
#include <map>
#include <set>
#include <memory>
#include <atomic>

// Node representing a folder's contents
struct FolderNode {
    std::string bucket;
    std::string prefix;
    std::vector<S3Object> objects;
    std::string next_continuation_token;
    bool is_truncated = false;
    bool loading = false;
    bool loaded = false;  // True if we've fetched this folder at least once
    std::string error;
};

// The browser model - owns state and processes commands
class BrowserModel {
public:
    BrowserModel();
    ~BrowserModel();

    // Initialize with a backend
    void setBackend(std::unique_ptr<IBackend> backend);

    // Profile management
    void loadProfiles();
    void selectProfile(int index);
    int selectedProfileIndex() const { return m_selectedProfileIdx; }
    const std::vector<AWSProfile>& profiles() const { return m_profiles; }
    std::vector<AWSProfile>& profiles() { return m_profiles; }

    // Commands (call from UI thread)
    void refresh();
    void loadFolder(const std::string& bucket, const std::string& prefix);
    void loadMore(const std::string& bucket, const std::string& prefix);
    void navigateTo(const std::string& s3_path);
    void navigateUp();
    void navigateInto(const std::string& bucket, const std::string& prefix);
    void addManualBucket(const std::string& bucket_name);

    // File selection and preview
    void selectFile(const std::string& bucket, const std::string& key);
    void clearSelection();

    // Prefetch file preview on hover (low priority)
    void prefetchFilePreview(const std::string& bucket, const std::string& key);

    // Prefetch folder contents on hover (low priority)
    void prefetchFolder(const std::string& bucket, const std::string& prefix);

    // Check if at root (bucket list view)
    bool isAtRoot() const { return m_currentBucket.empty(); }

    // Preview state accessors
    bool hasSelection() const { return !m_selectedKey.empty(); }
    const std::string& selectedBucket() const { return m_selectedBucket; }
    const std::string& selectedKey() const { return m_selectedKey; }
    bool previewLoading() const { return m_previewLoading; }
    std::string previewContent() const;  // Returns from StreamingFilePreview if available
    const std::string& previewError() const { return m_previewError; }
    bool previewSupported() const { return m_previewSupported; }

    // Streaming preview accessors
    bool hasStreamingPreview() const { return m_streamingPreview != nullptr; }
    StreamingFilePreview* streamingPreview() { return m_streamingPreview.get(); }
    const StreamingFilePreview* streamingPreview() const { return m_streamingPreview.get(); }
    bool isStreamingEnabled() const { return m_streamingEnabled; }
    int64_t selectedFileSize() const { return m_selectedFileSize; }

    // Call once per frame to process pending events from backend
    void processEvents();

    // State accessors (call from UI thread after processEvents)
    const std::vector<S3Bucket>& buckets() const { return m_buckets; }
    bool bucketsLoading() const { return m_bucketsLoading; }
    const std::string& bucketsError() const { return m_bucketsError; }

    FolderNode* getNode(const std::string& bucket, const std::string& prefix);
    const FolderNode* getNode(const std::string& bucket, const std::string& prefix) const;

    // Current path (for path bar display)
    const std::string& currentBucket() const { return m_currentBucket; }
    const std::string& currentPrefix() const { return m_currentPrefix; }
    void setCurrentPath(const std::string& bucket, const std::string& prefix);

private:
    FolderNode& getOrCreateNode(const std::string& bucket, const std::string& prefix);
    static std::string makeNodeKey(const std::string& bucket, const std::string& prefix);
    static bool parseS3Path(const std::string& path, std::string& bucket, std::string& prefix);
    static bool isPreviewSupported(const std::string& key);

    // Prefetch support - queue low-priority requests for subfolders
    void triggerPrefetch(const std::string& bucket, const std::vector<S3Object>& objects);

    std::unique_ptr<IBackend> m_backend;

    // Profiles
    std::vector<AWSProfile> m_profiles;
    int m_selectedProfileIdx = 0;

    // Buckets
    std::vector<S3Bucket> m_buckets;
    bool m_bucketsLoading = false;
    std::string m_bucketsError;

    // Folder nodes
    std::map<std::string, FolderNode> m_nodes;

    // Current navigation path
    std::string m_currentBucket;
    std::string m_currentPrefix;

    // File selection and preview
    std::string m_selectedBucket;
    std::string m_selectedKey;
    int64_t m_selectedFileSize = 0;
    bool m_previewLoading = false;
    bool m_previewSupported = false;
    std::string m_previewContent;
    std::string m_previewError;

    // Streaming preview for large files
    std::unique_ptr<StreamingFilePreview> m_streamingPreview;
    std::shared_ptr<std::atomic<bool>> m_streamingCancelFlag;
    bool m_streamingEnabled = false;  // Whether we're in streaming mode
    void startStreamingDownload(size_t totalFileSize);
    void requestNextStreamingChunk();
    void cancelStreamingDownload();
    static constexpr size_t STREAMING_CHUNK_SIZE = 1024 * 1024;  // 1MB chunks
    static constexpr size_t STREAMING_THRESHOLD = 64 * 1024;     // Stream files > 64KB

    // Cache for prefetched file previews (bucket/key -> content)
    std::map<std::string, std::string> m_previewCache;
    std::set<std::string> m_pendingObjectRequests;  // Track requests until event processed
    static std::string makePreviewCacheKey(const std::string& bucket, const std::string& key);
    static constexpr size_t PREVIEW_MAX_BYTES = 64 * 1024;  // 64KB

    // Gzip helper
    static bool isGzipped(const std::string& key);

    // Track current hover targets to avoid re-queueing the same request every frame
    std::string m_lastHoveredFile;    // bucket/key of last hovered file
    std::string m_lastHoveredFolder;  // bucket/prefix of last hovered folder

    // Auto-pagination: cancel flag for current folder's pagination requests
    // All continuation requests for the current folder share this flag
    // When navigating away, we set the flag to cancel pending requests
    std::shared_ptr<std::atomic<bool>> m_paginationCancelFlag;
};
