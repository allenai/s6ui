#pragma once

#include <string>
#include <vector>

struct AWSProfile {
    std::string name;
    std::string access_key_id;
    std::string secret_access_key;
    std::string region;
};

// Load all AWS profiles from ~/.aws/credentials and ~/.aws/config
std::vector<AWSProfile> load_aws_profiles();
