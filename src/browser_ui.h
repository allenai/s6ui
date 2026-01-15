#pragma once

#include "browser_model.h"
#include "preview/preview_renderer.h"
#include <memory>
#include <vector>

// Handles all ImGui rendering for the S3 browser
class BrowserUI {
public:
    explicit BrowserUI(BrowserModel& model);

    // Render the full browser UI
    // Call once per frame within ImGui context
    void render(int windowWidth, int windowHeight);

private:
    void renderTopBar();
    void renderLeftPane(float width, float height);
    void renderContent();
    void renderBucketList();
    void renderFolderContents();
    void renderStatusBar();
    void renderPreviewPane(float width, float height);

    static std::string formatSize(int64_t bytes);
    static std::string formatNumber(int64_t number);
    static std::string buildS3Path(const std::string& bucket, const std::string& prefix);

    BrowserModel& m_model;

    // Path input buffer
    char m_pathInput[2048] = "s3://";

    // Preview renderers
    std::vector<std::unique_ptr<IPreviewRenderer>> m_previewRenderers;
    IPreviewRenderer* m_activeRenderer = nullptr;
};
