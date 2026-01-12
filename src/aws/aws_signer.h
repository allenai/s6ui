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
    const std::string& payload = ""
);
