#pragma once

#include "backend.h"
#include "s3_backend.h"
#include "aws_credentials.h"
#include <string>
#include <vector>
#include <map>
#include <memory>

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

    // Check if at root (bucket list view)
    bool isAtRoot() const { return m_currentBucket.empty(); }

    // Preview state accessors
    bool hasSelection() const { return !m_selectedKey.empty(); }
    const std::string& selectedBucket() const { return m_selectedBucket; }
    const std::string& selectedKey() const { return m_selectedKey; }
    bool previewLoading() const { return m_previewLoading; }
    const std::string& previewContent() const { return m_previewContent; }
    const std::string& previewError() const { return m_previewError; }
    bool previewSupported() const { return m_previewSupported; }

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
    bool m_previewLoading = false;
    bool m_previewSupported = false;
    std::string m_previewContent;
    std::string m_previewError;
};
