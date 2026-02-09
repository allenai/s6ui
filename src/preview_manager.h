#pragma once

#include "streaming_preview.h"
#include "events.h"
#include <string>
#include <map>
#include <set>
#include <memory>
#include <atomic>

class IBackend;

class PreviewManager {
public:
    PreviewManager();
    ~PreviewManager();

    void setBackend(IBackend* backend);  // non-owning

    // Actions
    void selectFile(const std::string& bucket, const std::string& key, int64_t fileSize);
    void clearSelection();
    void prefetchFilePreview(const std::string& bucket, const std::string& key);
    void clearAll();  // Called on refresh/profile switch

    // Event handlers (called from BrowserModel::processEvents)
    void onObjectContentLoaded(ObjectContentLoadedPayload& payload);
    void onObjectContentLoadError(ObjectContentErrorPayload& payload);
    void onObjectRangeLoaded(ObjectRangeLoadedPayload& payload);
    void onObjectRangeLoadError(ObjectRangeErrorPayload& payload);

    // Read accessors
    bool hasSelection() const { return !m_selectedKey.empty(); }
    const std::string& selectedBucket() const { return m_selectedBucket; }
    const std::string& selectedKey() const { return m_selectedKey; }
    int64_t selectedFileSize() const { return m_selectedFileSize; }
    bool previewLoading() const { return m_previewLoading; }
    std::string previewContent() const;
    const std::string& previewError() const { return m_previewError; }
    bool previewSupported() const { return m_previewSupported; }
    bool hasStreamingPreview() const { return m_streamingPreview != nullptr; }
    std::shared_ptr<StreamingFilePreview> streamingPreview() { return m_streamingPreview; }
    std::shared_ptr<const StreamingFilePreview> streamingPreview() const { return m_streamingPreview; }
    bool isStreamingEnabled() const { return m_streamingEnabled; }

    // Utilities
    static bool isPreviewSupported(const std::string& key);
    static bool isCompressed(const std::string& key);

private:
    void startStreamingDownload(size_t totalFileSize);
    void cancelStreamingDownload();
    static std::string makePreviewCacheKey(const std::string& bucket, const std::string& key);

    IBackend* m_backend = nullptr;

    // File selection and preview
    std::string m_selectedBucket;
    std::string m_selectedKey;
    int64_t m_selectedFileSize = 0;
    bool m_previewLoading = false;
    bool m_previewSupported = false;
    std::string m_previewContent;
    std::string m_previewError;

    // Streaming preview for large files
    std::shared_ptr<StreamingFilePreview> m_streamingPreview;
    std::shared_ptr<std::atomic<bool>> m_streamingCancelFlag;
    bool m_streamingEnabled = false;
    static constexpr size_t STREAMING_THRESHOLD = 64 * 1024;

    // Cache for prefetched file previews (bucket/key -> content)
    std::map<std::string, std::string> m_previewCache;
    std::set<std::string> m_pendingObjectRequests;
    static constexpr size_t PREVIEW_MAX_BYTES = 64 * 1024;

    // Track current hover target to avoid re-queueing the same request every frame
    std::string m_lastHoveredFile;
};
