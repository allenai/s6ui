#pragma once

#include "aws_credentials.h"
#include <string>
#include <vector>
#include <map>
#include <mutex>
#include <atomic>
#include <functional>
#include <thread>
#include <memory>

struct S3Bucket {
    std::string name;
    std::string creation_date;
};

struct S3Object {
    std::string key;
    std::string display_name;  // Just the filename or folder name
    int64_t size;
    std::string last_modified;
    bool is_folder;
};

struct S3ListResult {
    std::vector<S3Object> objects;
    std::string continuation_token;
    bool is_truncated;
    std::string error;
};

class S3Client {
public:
    S3Client();
    ~S3Client();

    // Async list buckets for a profile
    void list_buckets_async(const AWSProfile& profile,
                            std::function<void(std::vector<S3Bucket>, std::string error)> callback);

    // Async list objects in bucket with prefix
    void list_objects_async(const AWSProfile& profile,
                           const std::string& bucket,
                           const std::string& prefix,
                           const std::string& continuation_token,
                           std::function<void(S3ListResult)> callback);

    // Synchronous versions (used internally)
    std::pair<std::vector<S3Bucket>, std::string> list_buckets(const AWSProfile& profile);
    S3ListResult list_objects(const AWSProfile& profile,
                              const std::string& bucket,
                              const std::string& prefix,
                              const std::string& continuation_token);

private:
    std::string http_get(const std::string& url,
                        const std::map<std::string, std::string>& headers);
    std::vector<S3Bucket> parse_list_buckets_xml(const std::string& xml);
    S3ListResult parse_list_objects_xml(const std::string& xml);
};

// Global async state for UI to read
struct S3BrowserState {
    std::mutex mutex;

    // Profiles and buckets
    std::vector<AWSProfile> profiles;
    int selected_profile_idx = 0;

    // Buckets for current profile
    std::vector<S3Bucket> buckets;
    std::atomic<bool> buckets_loading{false};
    std::string buckets_error;

    // Current path state
    struct PathNode {
        std::string bucket;
        std::string prefix;
        std::vector<S3Object> objects;
        std::string continuation_token;
        bool is_truncated = false;
        std::atomic<bool> loading{false};
        std::string error;
        bool expanded = false;
        bool pending_expand = false;  // One-shot flag for programmatic expansion
    };

    // Tree of expanded paths
    std::map<std::string, std::shared_ptr<PathNode>> path_nodes;

    // Current navigation path
    std::string current_bucket;
    std::string current_prefix;

    // Get or create a path node
    std::shared_ptr<PathNode> get_path_node(const std::string& bucket, const std::string& prefix);
};
