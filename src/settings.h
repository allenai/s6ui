#pragma once

#include <string>

struct AppSettings {
    std::string profile_name;
    std::string bucket;
    std::string prefix;
};

// Load settings from ~/.config/s6ui/settings.json
// Returns empty defaults if file missing or invalid
AppSettings loadSettings();

// Save settings to ~/.config/s6ui/settings.json
// Creates directory if needed, logs warning on failure
void saveSettings(const AppSettings& settings);
