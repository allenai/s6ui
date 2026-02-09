#pragma once

#include "backend.h"
#include "aws/s3_backend.h"
#include "aws/aws_credentials.h"
#include "preview_manager.h"
#include "settings.h"
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

    // Cached view for virtual scrolling: indices into objects[]
    // First folderCount indices are folders, rest are files
    std::vector<size_t> sortedView;
    size_t folderCount = 0;
    size_t cachedObjectsSize = 0;  // Invalidation check

    void rebuildSortedViewIfNeeded() {
        if (cachedObjectsSize == objects.size()) return;

        sortedView.clear();
        sortedView.reserve(objects.size());

        // Folders first
        for (size_t i = 0; i < objects.size(); ++i) {
            if (objects[i].is_folder) sortedView.push_back(i);
        }
        folderCount = sortedView.size();

        // Files second
        for (size_t i = 0; i < objects.size(); ++i) {
            if (!objects[i].is_folder) sortedView.push_back(i);
        }
        cachedObjectsSize = objects.size();
    }
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

    // Settings (frecent paths persistence)
    void setSettings(AppSettings settings);
    AppSettings& settings() { return m_settings; }
    void recordRecentPath(const std::string& path);
    std::vector<std::string> topFrecentPaths(size_t count = 20) const;

    // Commands (call from UI thread)
    void refresh();
    void loadFolder(const std::string& bucket, const std::string& prefix);
    void loadMore(const std::string& bucket, const std::string& prefix);
    void navigateTo(const std::string& s3_path);
    void navigateUp();
    void navigateInto(const std::string& bucket, const std::string& prefix);
    void addManualBucket(const std::string& bucket_name);

    // File selection and preview (thin wrappers that delegate to PreviewManager)
    void selectFile(const std::string& bucket, const std::string& key);
    void clearSelection();
    void prefetchFilePreview(const std::string& bucket, const std::string& key);

    // Prefetch folder contents on hover (low priority)
    void prefetchFolder(const std::string& bucket, const std::string& prefix);

    // Check if at root (bucket list view)
    bool isAtRoot() const { return m_currentBucket.empty(); }

    // Preview manager access
    PreviewManager& preview() { return m_preview; }
    const PreviewManager& preview() const { return m_preview; }

    // Call once per frame to process pending events from backend
    // Returns true if any events were processed (UI should redraw)
    bool processEvents();

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

    // Prefetch support - queue low-priority requests for subfolders
    void triggerPrefetch(const std::string& bucket, const std::vector<S3Object>& objects);

    std::unique_ptr<IBackend> m_backend;
    AppSettings m_settings;
    PreviewManager m_preview;

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

    // Track current hover target for folder prefetch
    std::string m_lastHoveredFolder;

    // Auto-pagination: cancel flag for current folder's pagination requests
    std::shared_ptr<std::atomic<bool>> m_paginationCancelFlag;
};
