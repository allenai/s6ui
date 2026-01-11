#pragma once

#include "browser_model.h"

// Handles all ImGui rendering for the S3 browser
class BrowserUI {
public:
    explicit BrowserUI(BrowserModel& model);

    // Render the full browser UI
    // Call once per frame within ImGui context
    void render(int windowWidth, int windowHeight);

private:
    void renderTopBar();
    void renderBucketTree();
    void renderFolder(const std::string& bucket, const std::string& prefix);

    static std::string formatSize(int64_t bytes);
    static std::string buildS3Path(const std::string& bucket, const std::string& prefix);

    BrowserModel& m_model;

    // Path input buffer
    char m_pathInput[2048] = "s3://";
};
