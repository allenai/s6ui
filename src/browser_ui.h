#pragma once

#include "browser_model.h"
#include "imguicolortextedit/TextEditor.h"

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

    // JSONL viewer
    void renderJsonlViewer(float width, float height);
    void navigateJsonlLine(int delta);
    std::string formatJson(const std::string& json);
    static bool isJsonlFile(const std::string& key);

    void updateEditorLanguage(const std::string& filename);

    static std::string formatSize(int64_t bytes);
    static std::string buildS3Path(const std::string& bucket, const std::string& prefix);

    BrowserModel& m_model;

    // Path input buffer
    char m_pathInput[2048] = "s3://";

    // Text editor for preview with syntax highlighting
    TextEditor m_editor;
    std::string m_editorCurrentKey;  // Track which file is loaded in editor

    // JSONL viewer state
    size_t m_currentJsonlLine = 0;
    bool m_jsonlRawMode = false;  // Toggle between formatted JSON and raw text
    std::string m_jsonlCurrentKey;  // Track which file is in JSONL viewer
    std::string m_formattedJsonCache;  // Cache of formatted current line
    size_t m_formattedJsonLineIndex = SIZE_MAX;  // Which line is cached
};
