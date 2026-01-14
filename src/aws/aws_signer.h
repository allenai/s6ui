#pragma once

#include <string>
#include <map>

struct AWSSignedRequest {
    std::string url;
    std::map<std::string, std::string> headers;
};

// Sign an AWS request using Signature Version 4
AWSSignedRequest aws_sign_request(
    const std::string& method,
    const std::string& host,
    const std::string& path,
    const std::string& query,
    const std::string& region,
    const std::string& service,
    const std::string& access_key,
    const std::string& secret_key,
    const std::string& payload = "",
    const std::string& session_token = ""
);

// Generate a presigned URL for S3 object download
// expires_seconds: URL validity duration (max 604800 = 7 days for IAM credentials)
std::string aws_generate_presigned_url(
    const std::string& bucket,
    const std::string& key,
    const std::string& region,
    const std::string& access_key,
    const std::string& secret_key,
    const std::string& session_token = "",
    int expires_seconds = 604800  // Default to max 7 days
);
