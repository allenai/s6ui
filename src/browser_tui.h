#pragma once

#include "browser_model.h"
#include "tui_preview_renderer.h"
#include <ncurses.h>
#include <string>
#include <vector>
#include <memory>

// Text-mode UI (TUI) interface for the S3 browser using ncurses
// Parallel to BrowserUI but renders to terminal instead of ImGui
class BrowserTUI {
public:
    explicit BrowserTUI(BrowserModel& model);
    ~BrowserTUI();

    // Main render function called each frame
    void render();

    // Process keyboard input (non-blocking)
    // Returns true if input was handled
    bool handleInput(int ch);

    // Check if user requested quit
    bool shouldQuit() const { return m_shouldQuit; }

private:
    // Initialization
    void initializeNcurses();
    void setupColors();
    void createWindows();
    void destroyWindows();
    void initializeRenderers();

    // Window management
    void handleResize();

    // Render components
    void renderTopBar();
    void renderLeftPane();
    void renderRightPane();
    void renderStatusBar();

    // Content rendering
    void renderBucketList();
    void renderFolderContents();
    void renderPreview();

    // Navigation helpers
    void moveSelection(int delta);
    void handleEnter();
    void handleBackspace();
    void handleRefresh();

    // Profile selector
    void renderProfileSelectorModal();
    bool handleProfileSelectorInput(int ch);
    void moveProfileSelection(int delta);
    void selectProfile();

    // Display helpers
    static std::string formatSize(int64_t bytes);
    static std::string formatNumber(int64_t number);
    static std::string truncateString(const std::string& str, int maxLen);
    static std::string buildS3Path(const std::string& bucket, const std::string& prefix);

    BrowserModel& m_model;

    // Window pointers
    WINDOW* m_topBar = nullptr;
    WINDOW* m_leftPane = nullptr;
    WINDOW* m_rightPane = nullptr;
    WINDOW* m_statusBar = nullptr;

    // UI state
    int m_selectedIndex = 0;
    int m_scrollOffset = 0;
    int m_previewScrollOffset = 0;
    bool m_shouldQuit = false;
    bool m_focusOnRight = false;  // false = left pane, true = right pane

    // Preview renderers
    std::vector<std::unique_ptr<TUIPreviewRenderer>> m_previewRenderers;
    TUIPreviewRenderer* m_activeRenderer = nullptr;

    // Profile selector state
    bool m_showProfileSelector = false;
    int m_profileSelectorIndex = 0;
    int m_profileSelectorScrollOffset = 0;

    // Terminal dimensions
    int m_termHeight = 0;
    int m_termWidth = 0;

    // Color pairs
    enum ColorPairs {
        COLOR_NORMAL = 1,
        COLOR_SELECTED = 2,
        COLOR_FOLDER = 3,
        COLOR_ERROR = 4,
        COLOR_HEADER = 5,
        COLOR_STATUS = 6,
    };
};
