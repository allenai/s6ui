#pragma once

#include <string>
#include <vector>
#include <map>
#include <cstdint>

struct PathEntry {
    std::string path;
    double score = 0.0;
    int64_t last_accessed = 0;  // unix timestamp
};

struct AppSettings {
    std::string profile_name;
    std::string bucket;
    std::string prefix;
    std::map<std::string, std::vector<PathEntry>> frecent_paths;  // per-profile frecency data
};

// Load settings from ~/.config/s6ui/settings.json
// Returns empty defaults if file missing or invalid
AppSettings loadSettings();

// Save settings to ~/.config/s6ui/settings.json
// Creates directory if needed, logs warning on failure
void saveSettings(const AppSettings& settings);
