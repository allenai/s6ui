#pragma once

#include <string>
#include <vector>

struct AWSProfile {
    std::string name;
    std::string access_key_id;
    std::string secret_access_key;
    std::string region;
    std::string endpoint_url;  // Custom S3 endpoint (e.g., https://weka-aus.beaker.org:9000)
};

// Load all AWS profiles from ~/.aws/credentials and ~/.aws/config
std::vector<AWSProfile> load_aws_profiles();
