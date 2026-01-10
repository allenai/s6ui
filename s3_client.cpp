#include "s3_client.h"
#include "aws_signer.h"
#include <curl/curl.h>
#include <regex>
#include <sstream>

static size_t write_callback(void* contents, size_t size, size_t nmemb, std::string* userp) {
    size_t total = size * nmemb;
    userp->append(static_cast<char*>(contents), total);
    return total;
}

S3Client::S3Client() {
    curl_global_init(CURL_GLOBAL_DEFAULT);
}

S3Client::~S3Client() {
    curl_global_cleanup();
}

std::string S3Client::http_get(const std::string& url,
                               const std::map<std::string, std::string>& headers) {
    CURL* curl = curl_easy_init();
    if (!curl) return "";

    std::string response;
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 30L);

    struct curl_slist* header_list = nullptr;
    for (const auto& [key, value] : headers) {
        std::string header = key + ": " + value;
        header_list = curl_slist_append(header_list, header.c_str());
    }
    if (header_list) {
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, header_list);
    }

    CURLcode res = curl_easy_perform(curl);

    if (header_list) curl_slist_free_all(header_list);
    curl_easy_cleanup(curl);

    if (res != CURLE_OK) {
        return "ERROR: " + std::string(curl_easy_strerror(res));
    }

    return response;
}

static std::string extract_tag(const std::string& xml, const std::string& tag) {
    std::string open = "<" + tag + ">";
    std::string close = "</" + tag + ">";
    size_t start = xml.find(open);
    if (start == std::string::npos) return "";
    start += open.size();
    size_t end = xml.find(close, start);
    if (end == std::string::npos) return "";
    return xml.substr(start, end - start);
}

static std::string extract_error(const std::string& xml) {
    std::string code = extract_tag(xml, "Code");
    std::string message = extract_tag(xml, "Message");
    if (!code.empty()) {
        return code + ": " + message;
    }
    return "";
}

std::vector<S3Bucket> S3Client::parse_list_buckets_xml(const std::string& xml) {
    std::vector<S3Bucket> buckets;

    // Find all <Bucket>...</Bucket> sections
    std::string search = "<Bucket>";
    std::string end_search = "</Bucket>";
    size_t pos = 0;

    while (true) {
        size_t start = xml.find(search, pos);
        if (start == std::string::npos) break;
        size_t end = xml.find(end_search, start);
        if (end == std::string::npos) break;

        std::string bucket_xml = xml.substr(start, end - start + end_search.size());

        S3Bucket bucket;
        bucket.name = extract_tag(bucket_xml, "Name");
        bucket.creation_date = extract_tag(bucket_xml, "CreationDate");

        if (!bucket.name.empty()) {
            buckets.push_back(bucket);
        }

        pos = end + end_search.size();
    }

    return buckets;
}

S3ListResult S3Client::parse_list_objects_xml(const std::string& xml) {
    S3ListResult result;

    // Check for error
    result.error = extract_error(xml);
    if (!result.error.empty()) {
        return result;
    }

    // Check truncation
    std::string truncated = extract_tag(xml, "IsTruncated");
    result.is_truncated = (truncated == "true");

    // Get continuation token
    result.continuation_token = extract_tag(xml, "NextContinuationToken");

    // Get common prefixes (folders)
    std::string prefix_search = "<CommonPrefixes>";
    std::string prefix_end = "</CommonPrefixes>";
    size_t pos = 0;
    while (true) {
        size_t start = xml.find(prefix_search, pos);
        if (start == std::string::npos) break;
        size_t end = xml.find(prefix_end, start);
        if (end == std::string::npos) break;

        std::string prefix_xml = xml.substr(start, end - start);
        std::string prefix = extract_tag(prefix_xml, "Prefix");

        if (!prefix.empty()) {
            S3Object obj;
            obj.key = prefix;
            obj.is_folder = true;
            obj.size = 0;
            // Get display name (last component)
            if (prefix.back() == '/') prefix.pop_back();
            size_t last_slash = prefix.rfind('/');
            obj.display_name = (last_slash != std::string::npos) ?
                prefix.substr(last_slash + 1) : prefix;
            result.objects.push_back(obj);
        }
        pos = end + prefix_end.size();
    }

    // Get objects (files)
    std::string contents_search = "<Contents>";
    std::string contents_end = "</Contents>";
    pos = 0;
    while (true) {
        size_t start = xml.find(contents_search, pos);
        if (start == std::string::npos) break;
        size_t end = xml.find(contents_end, start);
        if (end == std::string::npos) break;

        std::string contents_xml = xml.substr(start, end - start);

        S3Object obj;
        obj.key = extract_tag(contents_xml, "Key");
        obj.is_folder = false;

        std::string size_str = extract_tag(contents_xml, "Size");
        obj.size = size_str.empty() ? 0 : std::stoll(size_str);

        obj.last_modified = extract_tag(contents_xml, "LastModified");

        // Get display name
        std::string key = obj.key;
        size_t last_slash = key.rfind('/');
        obj.display_name = (last_slash != std::string::npos) ?
            key.substr(last_slash + 1) : key;

        // Skip if this is a "folder" marker (empty object ending with /)
        if (!obj.key.empty() && obj.key.back() != '/') {
            result.objects.push_back(obj);
        }

        pos = end + contents_end.size();
    }

    return result;
}

std::pair<std::vector<S3Bucket>, std::string> S3Client::list_buckets(const AWSProfile& profile) {
    std::string host = "s3." + profile.region + ".amazonaws.com";

    auto signed_req = aws_sign_request(
        "GET", host, "/", "", profile.region, "s3",
        profile.access_key_id, profile.secret_access_key
    );

    std::string response = http_get(signed_req.url, signed_req.headers);

    if (response.find("ERROR:") == 0) {
        return {{}, response};
    }

    std::string error = extract_error(response);
    if (!error.empty()) {
        return {{}, error};
    }

    return {parse_list_buckets_xml(response), ""};
}

S3ListResult S3Client::list_objects(const AWSProfile& profile,
                                    const std::string& bucket,
                                    const std::string& prefix,
                                    const std::string& continuation_token) {
    // Use virtual-hosted style URL
    std::string host = bucket + ".s3." + profile.region + ".amazonaws.com";

    // Build query string
    std::ostringstream query;
    query << "list-type=2";
    query << "&delimiter=%2F";  // URL-encoded /
    query << "&max-keys=1000";
    if (!prefix.empty()) {
        // URL encode prefix
        std::ostringstream encoded;
        for (char c : prefix) {
            if (isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') {
                encoded << c;
            } else if (c == '/') {
                encoded << "%2F";
            } else {
                encoded << '%' << std::hex << std::uppercase
                       << static_cast<int>(static_cast<unsigned char>(c));
            }
        }
        query << "&prefix=" << encoded.str();
    }
    if (!continuation_token.empty()) {
        // URL encode continuation token
        std::ostringstream encoded;
        for (char c : continuation_token) {
            if (isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') {
                encoded << c;
            } else {
                encoded << '%' << std::hex << std::uppercase
                       << static_cast<int>(static_cast<unsigned char>(c));
            }
        }
        query << "&continuation-token=" << encoded.str();
    }

    auto signed_req = aws_sign_request(
        "GET", host, "/", query.str(), profile.region, "s3",
        profile.access_key_id, profile.secret_access_key
    );

    std::string response = http_get(signed_req.url, signed_req.headers);

    if (response.find("ERROR:") == 0) {
        S3ListResult result;
        result.error = response;
        return result;
    }

    return parse_list_objects_xml(response);
}

void S3Client::list_buckets_async(const AWSProfile& profile,
                                  std::function<void(std::vector<S3Bucket>, std::string)> callback) {
    std::thread([this, profile, callback]() {
        auto [buckets, error] = list_buckets(profile);
        callback(buckets, error);
    }).detach();
}

void S3Client::list_objects_async(const AWSProfile& profile,
                                  const std::string& bucket,
                                  const std::string& prefix,
                                  const std::string& continuation_token,
                                  std::function<void(S3ListResult)> callback) {
    std::thread([this, profile, bucket, prefix, continuation_token, callback]() {
        auto result = list_objects(profile, bucket, prefix, continuation_token);
        callback(result);
    }).detach();
}

std::shared_ptr<S3BrowserState::PathNode> S3BrowserState::get_path_node(
    const std::string& bucket, const std::string& prefix) {
    std::string key = bucket + "/" + prefix;
    std::lock_guard<std::mutex> lock(mutex);
    auto it = path_nodes.find(key);
    if (it != path_nodes.end()) {
        return it->second;
    }
    auto node = std::make_shared<PathNode>();
    node->bucket = bucket;
    node->prefix = prefix;
    path_nodes[key] = node;
    return node;
}
