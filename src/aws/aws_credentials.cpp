#include "aws_credentials.h"
#include "loguru.hpp"
#include <json.hpp>
#include <fstream>
#include <sstream>
#include <cstdlib>
#include <map>
#include <algorithm>
#include <iomanip>
#include <openssl/sha.h>
#include <curl/curl.h>

static std::string get_home_dir() {
    const char* home = std::getenv("HOME");
    return home ? home : "";
}

static std::string trim(const std::string& s) {
    size_t start = s.find_first_not_of(" \t\r\n");
    if (start == std::string::npos) return "";
    size_t end = s.find_last_not_of(" \t\r\n");
    return s.substr(start, end - start + 1);
}

static std::map<std::string, std::map<std::string, std::string>> parse_ini_file(const std::string& path) {
    std::map<std::string, std::map<std::string, std::string>> sections;
    std::ifstream file(path);
    if (!file.is_open()) return sections;

    std::string line;
    std::string current_section;

    while (std::getline(file, line)) {
        line = trim(line);
        if (line.empty() || line[0] == '#' || line[0] == ';') continue;

        if (line[0] == '[' && line.back() == ']') {
            current_section = line.substr(1, line.size() - 2);
            // Remove "profile " prefix if present (from config file)
            if (current_section.find("profile ") == 0) {
                current_section = current_section.substr(8);
            }
            current_section = trim(current_section);
        } else {
            size_t eq_pos = line.find('=');
            if (eq_pos != std::string::npos && !current_section.empty()) {
                std::string key = trim(line.substr(0, eq_pos));
                std::string value = trim(line.substr(eq_pos + 1));
                sections[current_section][key] = value;
            }
        }
    }

    return sections;
}

// Compute SHA1 hash of string and return as hex string
static std::string sha1_hex(const std::string& input) {
    unsigned char hash[SHA_DIGEST_LENGTH];
    SHA1(reinterpret_cast<const unsigned char*>(input.c_str()), input.length(), hash);

    std::ostringstream oss;
    for (int i = 0; i < SHA_DIGEST_LENGTH; i++) {
        oss << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(hash[i]);
    }
    return oss.str();
}

// Parse ISO 8601 timestamp to time_t
static time_t parse_iso8601(const std::string& timestamp) {
    struct tm tm = {};
    std::istringstream ss(timestamp);
    ss >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%S");
    if (ss.fail()) {
        return 0;
    }
    // Convert to UTC time
    return timegm(&tm);
}

// Get cached SSO token from ~/.aws/sso/cache/<hash>.json
static std::string get_sso_cached_token(const std::string& sso_start_url, const std::string& sso_session_name = "") {
    std::string home = get_home_dir();
    if (home.empty()) return "";

    // AWS CLI v2 with sso-session uses hash of session name, v1 uses hash of start URL
    std::string hash_input = sso_session_name.empty() ? sso_start_url : sso_session_name;
    std::string hash = sha1_hex(hash_input);
    std::string cache_path = home + "/.aws/sso/cache/" + hash + ".json";

    // Read cache file
    std::ifstream file(cache_path);
    if (!file.is_open()) {
        LOG_F(WARNING, "SSO cache file not found: %s", cache_path.c_str());
        return "";
    }

    try {
        nlohmann::json cache_data;
        file >> cache_data;

        // Check if token is expired
        if (cache_data.contains("expiresAt")) {
            std::string expires_at = cache_data["expiresAt"];
            time_t expiration = parse_iso8601(expires_at);
            time_t now = time(nullptr);

            if (expiration > 0 && now >= expiration) {
                LOG_F(WARNING, "SSO token expired at %s", expires_at.c_str());
                return "";
            }
        }

        // Return access token
        if (cache_data.contains("accessToken")) {
            return cache_data["accessToken"];
        }
    } catch (const std::exception& e) {
        LOG_F(ERROR, "Failed to parse SSO cache file %s: %s", cache_path.c_str(), e.what());
    }

    return "";
}

// libcurl write callback
static size_t write_callback(void* contents, size_t size, size_t nmemb, void* userp) {
    size_t total_size = size * nmemb;
    std::string* response = static_cast<std::string*>(userp);
    response->append(static_cast<char*>(contents), total_size);
    return total_size;
}

// Get temporary credentials from AWS SSO GetRoleCredentials API
static bool get_sso_credentials(AWSProfile& profile) {
    // Get cached SSO token (use session name for v2, start URL for v1)
    std::string access_token = get_sso_cached_token(profile.sso_start_url, profile.sso_session_name);
    if (access_token.empty()) {
        LOG_F(ERROR, "SSO credentials expired for profile '%s'. Run: aws sso login --profile %s",
              profile.name.c_str(), profile.name.c_str());
        return false;
    }

    // Build API URL
    std::ostringstream url;
    url << "https://portal.sso." << profile.sso_region << ".amazonaws.com/federation/credentials"
        << "?account_id=" << profile.sso_account_id
        << "&role_name=" << profile.sso_role_name;

    // Make HTTP request
    CURL* curl = curl_easy_init();
    if (!curl) {
        LOG_F(ERROR, "Failed to initialize curl for SSO API");
        return false;
    }

    std::string response;
    struct curl_slist* headers = nullptr;
    std::string bearer_header = "x-amz-sso_bearer_token: " + access_token;
    headers = curl_slist_append(headers, bearer_header.c_str());

    curl_easy_setopt(curl, CURLOPT_URL, url.str().c_str());
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 30L);

    CURLcode res = curl_easy_perform(curl);
    long http_code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);

    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);

    if (res != CURLE_OK) {
        LOG_F(ERROR, "SSO API request failed for profile '%s': %s",
              profile.name.c_str(), curl_easy_strerror(res));
        return false;
    }

    if (http_code != 200) {
        LOG_F(ERROR, "SSO API returned HTTP %ld for profile '%s'", http_code, profile.name.c_str());
        return false;
    }

    // Parse JSON response
    try {
        nlohmann::json resp_data = nlohmann::json::parse(response);

        if (!resp_data.contains("roleCredentials")) {
            LOG_F(ERROR, "SSO API response missing roleCredentials");
            return false;
        }

        auto creds = resp_data["roleCredentials"];
        profile.access_key_id = creds["accessKeyId"];
        profile.secret_access_key = creds["secretAccessKey"];
        profile.session_token = creds["sessionToken"];

        // Convert expiration from milliseconds to seconds
        if (creds.contains("expiration")) {
            int64_t exp_millis = creds["expiration"];
            profile.expiration = exp_millis / 1000;
        }

        LOG_F(INFO, "Successfully retrieved SSO credentials for profile '%s'", profile.name.c_str());
        return true;
    } catch (const std::exception& e) {
        LOG_F(ERROR, "Failed to parse SSO API response: %s", e.what());
        return false;
    }
}

// Helper to resolve SSO configuration from sso_session reference
static void resolve_sso_session(AWSProfile& profile, const std::map<std::string, std::map<std::string, std::string>>& config) {
    // If profile already has direct SSO config, don't override
    if (!profile.sso_start_url.empty()) {
        return;
    }

    // Look for sso_session reference in profile config
    auto cfg_it = config.find(profile.name);
    if (cfg_it == config.end()) {
        return;
    }

    auto sso_session_it = cfg_it->second.find("sso_session");
    if (sso_session_it == cfg_it->second.end()) {
        return;
    }

    // Look up the sso-session section
    std::string session_name_key = "sso-session " + sso_session_it->second;
    auto session_it = config.find(session_name_key);
    if (session_it == config.end()) {
        LOG_F(WARNING, "Profile '%s' references sso_session '%s' but it doesn't exist",
              profile.name.c_str(), sso_session_it->second.c_str());
        return;
    }

    // Save the session name for AWS CLI v2 cache lookup
    profile.sso_session_name = sso_session_it->second;

    // Copy SSO configuration from the session
    auto start_url_it = session_it->second.find("sso_start_url");
    if (start_url_it != session_it->second.end()) {
        profile.sso_start_url = start_url_it->second;
    }

    auto region_it = session_it->second.find("sso_region");
    if (region_it != session_it->second.end()) {
        profile.sso_region = region_it->second;
    }

    LOG_F(INFO, "Resolved SSO session '%s' for profile '%s'",
          sso_session_it->second.c_str(), profile.name.c_str());
}

std::vector<AWSProfile> load_aws_profiles() {
    std::vector<AWSProfile> profiles;
    std::string home = get_home_dir();
    if (home.empty()) return profiles;

    // Parse credentials file
    auto creds = parse_ini_file(home + "/.aws/credentials");

    // Parse config file for regions
    auto config = parse_ini_file(home + "/.aws/config");

    // Build profile list from credentials
    for (const auto& [name, values] : creds) {
        AWSProfile profile;
        profile.name = name;

        auto it = values.find("aws_access_key_id");
        if (it != values.end()) profile.access_key_id = it->second;

        it = values.find("aws_secret_access_key");
        if (it != values.end()) profile.secret_access_key = it->second;

        // Parse session token from credentials (for manually-added temp credentials)
        it = values.find("aws_session_token");
        if (it != values.end()) profile.session_token = it->second;

        // Look for region, endpoint_url, and SSO config in config
        auto cfg_it = config.find(name);
        if (cfg_it != config.end()) {
            auto reg_it = cfg_it->second.find("region");
            if (reg_it != cfg_it->second.end()) {
                profile.region = reg_it->second;
            }
            auto endpoint_it = cfg_it->second.find("endpoint_url");
            if (endpoint_it != cfg_it->second.end()) {
                profile.endpoint_url = endpoint_it->second;
            }

            // Parse SSO configuration (inline format)
            auto sso_start_it = cfg_it->second.find("sso_start_url");
            if (sso_start_it != cfg_it->second.end()) {
                profile.sso_start_url = sso_start_it->second;
            }
            auto sso_region_it = cfg_it->second.find("sso_region");
            if (sso_region_it != cfg_it->second.end()) {
                profile.sso_region = sso_region_it->second;
            }
            auto sso_account_it = cfg_it->second.find("sso_account_id");
            if (sso_account_it != cfg_it->second.end()) {
                profile.sso_account_id = sso_account_it->second;
            }
            auto sso_role_it = cfg_it->second.find("sso_role_name");
            if (sso_role_it != cfg_it->second.end()) {
                profile.sso_role_name = sso_role_it->second;
            }

            // Resolve sso_session reference (newer AWS CLI v2 format)
            resolve_sso_session(profile, config);

            // Parse sso_account_id and sso_role_name from profile (may be in profile, not session)
            auto profile_sso_account_it = cfg_it->second.find("sso_account_id");
            if (profile_sso_account_it != cfg_it->second.end()) {
                profile.sso_account_id = profile_sso_account_it->second;
            }
            auto profile_sso_role_it = cfg_it->second.find("sso_role_name");
            if (profile_sso_role_it != cfg_it->second.end()) {
                profile.sso_role_name = profile_sso_role_it->second;
            }
        }

        // Default region if not specified
        if (profile.region.empty()) {
            profile.region = "us-east-1";
        }

        // Only add if we have credentials OR SSO configuration
        if (!profile.access_key_id.empty() && !profile.secret_access_key.empty()) {
            profiles.push_back(profile);
        } else if (!profile.sso_start_url.empty() && !profile.sso_account_id.empty() && !profile.sso_role_name.empty()) {
            // SSO profile without static credentials (but has all required SSO fields)
            profiles.push_back(profile);
        }
    }

    // Also check for SSO-only profiles in config (not in credentials file)
    for (const auto& [name, values] : config) {
        // Skip sso-session sections
        if (name.find("sso-session ") == 0) {
            continue;
        }

        // Skip if already processed from credentials
        bool already_exists = false;
        for (const auto& p : profiles) {
            if (p.name == name) {
                already_exists = true;
                break;
            }
        }
        if (already_exists) continue;

        AWSProfile profile;
        profile.name = name;

        // Check if this is an SSO profile (inline format)
        auto sso_start_it = values.find("sso_start_url");
        if (sso_start_it != values.end()) {
            profile.sso_start_url = sso_start_it->second;

            auto sso_region_it = values.find("sso_region");
            if (sso_region_it != values.end()) {
                profile.sso_region = sso_region_it->second;
            }
        }

        // Resolve sso_session reference
        resolve_sso_session(profile, config);

        // Parse SSO account and role from profile
        auto sso_account_it = values.find("sso_account_id");
        if (sso_account_it != values.end()) {
            profile.sso_account_id = sso_account_it->second;
        }
        auto sso_role_it = values.find("sso_role_name");
        if (sso_role_it != values.end()) {
            profile.sso_role_name = sso_role_it->second;
        }

        auto reg_it = values.find("region");
        if (reg_it != values.end()) {
            profile.region = reg_it->second;
        } else {
            profile.region = "us-east-1";
        }

        // Only add if it has complete SSO configuration
        if (!profile.sso_start_url.empty() && !profile.sso_account_id.empty() && !profile.sso_role_name.empty()) {
            profiles.push_back(profile);
        }
    }

    // Resolve SSO credentials for SSO profiles
    for (auto& profile : profiles) {
        if (!profile.sso_start_url.empty() && profile.access_key_id.empty()) {
            // This is an SSO profile without static credentials - attempt to get temporary credentials
            if (!get_sso_credentials(profile)) {
                LOG_F(WARNING, "Profile '%s' uses SSO but credentials could not be loaded. Run: aws sso login --profile %s",
                      profile.name.c_str(), profile.name.c_str());
            }
        }
    }

    // Remove profiles that still don't have valid credentials
    profiles.erase(
        std::remove_if(profiles.begin(), profiles.end(), [](const AWSProfile& profile) {
            bool has_credentials = !profile.access_key_id.empty() && !profile.secret_access_key.empty();
            if (!has_credentials) {
                LOG_F(WARNING, "Removing profile '%s' - no valid credentials available", profile.name.c_str());
            }
            return !has_credentials;
        }),
        profiles.end()
    );

    return profiles;
}

bool refresh_profile_credentials(AWSProfile& profile) {
    LOG_F(INFO, "Refreshing credentials for profile '%s'", profile.name.c_str());

    std::string home = get_home_dir();
    if (home.empty()) {
        LOG_F(ERROR, "Cannot refresh credentials: HOME directory not found");
        return false;
    }

    // Parse credentials and config files
    auto creds = parse_ini_file(home + "/.aws/credentials");
    auto config = parse_ini_file(home + "/.aws/config");

    // Find the profile in credentials
    auto cred_it = creds.find(profile.name);
    if (cred_it == creds.end()) {
        // Profile might be SSO-only (in config but not credentials)
        LOG_F(INFO, "Profile '%s' not found in credentials file, checking config", profile.name.c_str());
    } else {
        // Update credentials from file
        auto it = cred_it->second.find("aws_access_key_id");
        if (it != cred_it->second.end()) {
            profile.access_key_id = it->second;
        } else {
            profile.access_key_id.clear();
        }

        it = cred_it->second.find("aws_secret_access_key");
        if (it != cred_it->second.end()) {
            profile.secret_access_key = it->second;
        } else {
            profile.secret_access_key.clear();
        }

        it = cred_it->second.find("aws_session_token");
        if (it != cred_it->second.end()) {
            profile.session_token = it->second;
        } else {
            profile.session_token.clear();
        }
    }

    // Update config from config file
    auto cfg_it = config.find(profile.name);
    if (cfg_it != config.end()) {
        auto reg_it = cfg_it->second.find("region");
        if (reg_it != cfg_it->second.end()) {
            profile.region = reg_it->second;
        }

        auto endpoint_it = cfg_it->second.find("endpoint_url");
        if (endpoint_it != cfg_it->second.end()) {
            profile.endpoint_url = endpoint_it->second;
        } else {
            profile.endpoint_url.clear();
        }

        // Update SSO configuration
        auto sso_start_it = cfg_it->second.find("sso_start_url");
        if (sso_start_it != cfg_it->second.end()) {
            profile.sso_start_url = sso_start_it->second;
        } else {
            profile.sso_start_url.clear();
        }

        auto sso_region_it = cfg_it->second.find("sso_region");
        if (sso_region_it != cfg_it->second.end()) {
            profile.sso_region = sso_region_it->second;
        } else {
            profile.sso_region.clear();
        }

        auto sso_account_it = cfg_it->second.find("sso_account_id");
        if (sso_account_it != cfg_it->second.end()) {
            profile.sso_account_id = sso_account_it->second;
        } else {
            profile.sso_account_id.clear();
        }

        auto sso_role_it = cfg_it->second.find("sso_role_name");
        if (sso_role_it != cfg_it->second.end()) {
            profile.sso_role_name = sso_role_it->second;
        } else {
            profile.sso_role_name.clear();
        }

        // Resolve sso_session reference
        profile.sso_session_name.clear();
        resolve_sso_session(profile, config);

        // Re-parse SSO account and role from profile (may override session)
        auto profile_sso_account_it = cfg_it->second.find("sso_account_id");
        if (profile_sso_account_it != cfg_it->second.end()) {
            profile.sso_account_id = profile_sso_account_it->second;
        }
        auto profile_sso_role_it = cfg_it->second.find("sso_role_name");
        if (profile_sso_role_it != cfg_it->second.end()) {
            profile.sso_role_name = profile_sso_role_it->second;
        }
    }

    // Default region if not specified
    if (profile.region.empty()) {
        profile.region = "us-east-1";
    }

    // If this is an SSO profile without static credentials, resolve SSO credentials
    if (!profile.sso_start_url.empty() && profile.access_key_id.empty()) {
        if (!get_sso_credentials(profile)) {
            LOG_F(ERROR, "Failed to refresh SSO credentials for profile '%s'", profile.name.c_str());
            return false;
        }
    }

    // Verify we have valid credentials
    if (profile.access_key_id.empty() || profile.secret_access_key.empty()) {
        LOG_F(ERROR, "Profile '%s' has no valid credentials after refresh", profile.name.c_str());
        return false;
    }

    LOG_F(INFO, "Successfully refreshed credentials for profile '%s'", profile.name.c_str());
    return true;
}
