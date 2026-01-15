#pragma once

#include "browser_model.h"
#include <ncurses.h>
#include <string>
#include <vector>

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
