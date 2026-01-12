#include "aws_credentials.h"
#include <fstream>
#include <sstream>
#include <cstdlib>
#include <map>
#include <algorithm>

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

        // Look for region in config
        auto cfg_it = config.find(name);
        if (cfg_it != config.end()) {
            auto reg_it = cfg_it->second.find("region");
            if (reg_it != cfg_it->second.end()) {
                profile.region = reg_it->second;
            }
        }

        // Default region if not specified
        if (profile.region.empty()) {
            profile.region = "us-east-1";
        }

        // Only add if we have credentials
        if (!profile.access_key_id.empty() && !profile.secret_access_key.empty()) {
            profiles.push_back(profile);
        }
    }

    return profiles;
}
