#pragma once

#include <string>
#include <vector>
#include <ctime>

struct AWSProfile {
    std::string name;
    std::string access_key_id;
    std::string secret_access_key;
    std::string region;
    std::string endpoint_url;  // Custom S3 endpoint (e.g., https://weka-aus.beaker.org:9000)

    // Session token for temporary credentials (SSO or manually-added)
    std::string session_token;
    time_t expiration = 0;  // 0 if no expiration (static credentials)

    // SSO-specific fields (empty for non-SSO profiles)
    std::string sso_start_url;
    std::string sso_region;
    std::string sso_account_id;
    std::string sso_role_name;
    std::string sso_session_name;  // Session name for AWS CLI v2 sso-session format
};

// Load all AWS profiles from ~/.aws/credentials and ~/.aws/config
std::vector<AWSProfile> load_aws_profiles();
