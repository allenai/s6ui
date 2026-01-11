#pragma once

#include "backend.h"
#include "s3_backend.h"
#include "aws_credentials.h"
#include <string>
#include <vector>
#include <map>
#include <memory>
#include <mutex>

// Node representing a folder in the tree
struct FolderNode {
    std::string bucket;
    std::string prefix;
    std::vector<S3Object> objects;
    std::string next_continuation_token;
    bool is_truncated = false;
    bool loading = false;
    std::string error;

    // UI state
    bool expanded = false;
    bool pending_expand = false;  // One-shot flag for programmatic expansion
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

    // Commands (call from UI thread)
    void refresh();
    void expandNode(const std::string& bucket, const std::string& prefix);
    void collapseNode(const std::string& bucket, const std::string& prefix);
    void loadMore(const std::string& bucket, const std::string& prefix);
    void navigateTo(const std::string& s3_path);
    void addManualBucket(const std::string& bucket_name);

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

    // Scroll target (for navigation)
    bool hasScrollTarget() const { return m_scrollToTarget; }
    const std::string& scrollTargetBucket() const { return m_scrollTargetBucket; }
    const std::string& scrollTargetPrefix() const { return m_scrollTargetPrefix; }
    void clearScrollTarget() { m_scrollToTarget = false; }

private:
    void onEvent(StateEvent event);
    FolderNode& getOrCreateNode(const std::string& bucket, const std::string& prefix);
    static std::string makeNodeKey(const std::string& bucket, const std::string& prefix);
    static bool parseS3Path(const std::string& path, std::string& bucket, std::string& prefix);

    std::unique_ptr<IBackend> m_backend;

    // Event queue (thread-safe)
    std::mutex m_eventMutex;
    std::vector<StateEvent> m_pendingEvents;

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

    // Scroll target
    bool m_scrollToTarget = false;
    std::string m_scrollTargetBucket;
    std::string m_scrollTargetPrefix;
};
