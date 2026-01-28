#include "settings.h"
#include "loguru.hpp"
#include <nlohmann/json.hpp>
#include <fstream>
#include <cstdlib>
#include <sys/stat.h>

using json = nlohmann::json;

static std::string getSettingsDir() {
    const char* home = std::getenv("HOME");
    if (!home) {
        return "";
    }

#ifdef __APPLE__
    // macOS: ~/Library/Application Support/s6ui/
    return std::string(home) + "/Library/Application Support/s6ui";
#else
    // Linux: ~/.config/s6ui/ (XDG standard)
    // Respect XDG_CONFIG_HOME if set
    const char* xdg_config = std::getenv("XDG_CONFIG_HOME");
    if (xdg_config && xdg_config[0] != '\0') {
        return std::string(xdg_config) + "/s6ui";
    }
    return std::string(home) + "/.config/s6ui";
#endif
}

static std::string getSettingsPath() {
    std::string dir = getSettingsDir();
    if (dir.empty()) {
        return "";
    }
    return dir + "/settings.json";
}

static bool createDirRecursive(const std::string& dir) {
    struct stat st;
    if (stat(dir.c_str(), &st) == 0) {
        return S_ISDIR(st.st_mode);
    }

    // Find parent directory
    size_t pos = dir.rfind('/');
    if (pos != std::string::npos && pos > 0) {
        std::string parent = dir.substr(0, pos);
        if (!createDirRecursive(parent)) {
            return false;
        }
    }

    return mkdir(dir.c_str(), 0755) == 0;
}

AppSettings loadSettings() {
    AppSettings settings;

    std::string path = getSettingsPath();
    if (path.empty()) {
        LOG_F(INFO, "Cannot determine settings path (HOME not set)");
        return settings;
    }

    std::ifstream file(path);
    if (!file.is_open()) {
        LOG_F(INFO, "No settings file found at %s", path.c_str());
        return settings;
    }

    try {
        json j = json::parse(file);
        settings.profile_name = j.value("profile", "");
        settings.bucket = j.value("bucket", "");
        settings.prefix = j.value("prefix", "");
        if (j.contains("frecent_paths") && j["frecent_paths"].is_object()) {
            for (auto& [profile, entries] : j["frecent_paths"].items()) {
                if (entries.is_array()) {
                    for (auto& e : entries) {
                        if (e.is_object() && e.contains("path")) {
                            PathEntry entry;
                            entry.path = e.value("path", "");
                            entry.score = e.value("score", 0.0);
                            entry.last_accessed = e.value("last_accessed", static_cast<int64_t>(0));
                            if (!entry.path.empty()) {
                                settings.frecent_paths[profile].push_back(std::move(entry));
                            }
                        }
                    }
                }
            }
        }
        LOG_F(INFO, "Loaded settings: profile=%s bucket=%s prefix=%s",
              settings.profile_name.c_str(), settings.bucket.c_str(), settings.prefix.c_str());
    } catch (const json::exception& e) {
        LOG_F(WARNING, "Failed to parse settings file: %s", e.what());
    }

    return settings;
}

void saveSettings(const AppSettings& settings) {
    std::string dir = getSettingsDir();
    if (dir.empty()) {
        LOG_F(WARNING, "Cannot determine settings directory (HOME not set)");
        return;
    }

    if (!createDirRecursive(dir)) {
        LOG_F(WARNING, "Failed to create settings directory: %s", dir.c_str());
        return;
    }

    std::string path = getSettingsPath();
    std::ofstream file(path);
    if (!file.is_open()) {
        LOG_F(WARNING, "Failed to open settings file for writing: %s", path.c_str());
        return;
    }

    json j;
    j["profile"] = settings.profile_name;
    j["bucket"] = settings.bucket;
    j["prefix"] = settings.prefix;
    j["frecent_paths"] = json::object();
    for (const auto& [profile, entries] : settings.frecent_paths) {
        json arr = json::array();
        for (const auto& e : entries) {
            arr.push_back({{"path", e.path}, {"score", e.score}, {"last_accessed", e.last_accessed}});
        }
        j["frecent_paths"][profile] = arr;
    }

    file << j.dump(2) << std::endl;
    LOG_F(INFO, "Saved settings to %s", path.c_str());
}
