#include "browser_tui.h"
#include "loguru.hpp"
#include <cstring>
#include <cstdlib>
#include <sstream>
#include <iomanip>
#include <algorithm>
#include <signal.h>
#include <unistd.h>

BrowserTUI::BrowserTUI(BrowserModel& model)
    : m_model(model)
{
    initializeNcurses();
    setupColors();
    createWindows();
}

BrowserTUI::~BrowserTUI()
{
    destroyWindows();
    endwin();
}

void BrowserTUI::initializeNcurses()
{
    // Initialize ncurses
    initscr();

    // Configure ncurses behavior
    cbreak();              // Disable line buffering
    noecho();              // Don't echo input
    keypad(stdscr, TRUE);  // Enable special keys (arrows, F-keys, etc.)
    nodelay(stdscr, TRUE); // Make getch() non-blocking
    curs_set(0);           // Hide cursor

    // Get terminal size
    getmaxyx(stdscr, m_termHeight, m_termWidth);

    LOG_F(INFO, "TUI initialized: %dx%d", m_termWidth, m_termHeight);
}

void BrowserTUI::setupColors()
{
    if (has_colors()) {
        start_color();
        use_default_colors();

        // Define color pairs
        init_pair(COLOR_NORMAL, COLOR_WHITE, -1);
        init_pair(COLOR_SELECTED, COLOR_BLACK, COLOR_CYAN);
        init_pair(COLOR_FOLDER, COLOR_BLUE, -1);
        init_pair(COLOR_ERROR, COLOR_RED, -1);
        init_pair(COLOR_HEADER, COLOR_YELLOW, -1);
        init_pair(COLOR_STATUS, COLOR_GREEN, -1);

        LOG_F(INFO, "TUI colors initialized (%d colors, %d pairs)", COLORS, COLOR_PAIRS);
    }
}

void BrowserTUI::createWindows()
{
    // Destroy existing windows if any
    destroyWindows();

    // Get current terminal size
    getmaxyx(stdscr, m_termHeight, m_termWidth);

    // Calculate dimensions
    int topBarHeight = 1;
    int statusBarHeight = 1;
    int contentHeight = m_termHeight - topBarHeight - statusBarHeight;
    int leftPaneWidth = m_termWidth / 2;
    int rightPaneWidth = m_termWidth - leftPaneWidth;

    // Create windows
    m_topBar = newwin(topBarHeight, m_termWidth, 0, 0);
    m_leftPane = newwin(contentHeight, leftPaneWidth, topBarHeight, 0);
    m_rightPane = newwin(contentHeight, rightPaneWidth, topBarHeight, leftPaneWidth);
    m_statusBar = newwin(statusBarHeight, m_termWidth, m_termHeight - 1, 0);

    // Enable scrolling for content panes
    scrollok(m_leftPane, TRUE);
    scrollok(m_rightPane, TRUE);

    LOG_F(INFO, "TUI windows created: %dx%d", m_termWidth, m_termHeight);
}

void BrowserTUI::destroyWindows()
{
    if (m_topBar) { delwin(m_topBar); m_topBar = nullptr; }
    if (m_leftPane) { delwin(m_leftPane); m_leftPane = nullptr; }
    if (m_rightPane) { delwin(m_rightPane); m_rightPane = nullptr; }
    if (m_statusBar) { delwin(m_statusBar); m_statusBar = nullptr; }
}

void BrowserTUI::handleResize()
{
    // Handle terminal resize
    endwin();
    refresh();
    clear();

    getmaxyx(stdscr, m_termHeight, m_termWidth);
    createWindows();

    LOG_F(INFO, "TUI resized to: %dx%d", m_termWidth, m_termHeight);
}

void BrowserTUI::render()
{
    // Check if terminal was resized
    int newHeight, newWidth;
    getmaxyx(stdscr, newHeight, newWidth);
    if (newHeight != m_termHeight || newWidth != m_termWidth) {
        handleResize();
    }

    // Clear all windows
    werase(m_topBar);
    werase(m_leftPane);
    werase(m_rightPane);
    werase(m_statusBar);

    // Render components
    renderTopBar();
    renderLeftPane();
    renderRightPane();
    renderStatusBar();

    // Refresh all windows
    wnoutrefresh(m_topBar);
    wnoutrefresh(m_leftPane);
    wnoutrefresh(m_rightPane);
    wnoutrefresh(m_statusBar);
    doupdate();
}

void BrowserTUI::renderTopBar()
{
    if (!m_topBar) return;

    wattron(m_topBar, COLOR_PAIR(COLOR_HEADER) | A_BOLD);

    // Show profile name
    const auto& profiles = m_model.profiles();
    std::string profileName = "No Profile";
    if (!profiles.empty() && m_model.selectedProfileIndex() < (int)profiles.size()) {
        profileName = profiles[m_model.selectedProfileIndex()].name;
    }

    // Build current path
    std::string path = "s3://";
    if (!m_model.currentBucket().empty()) {
        path += m_model.currentBucket();
        if (!m_model.currentPrefix().empty()) {
            path += "/" + m_model.currentPrefix();
        }
    }

    // Show loading indicator
    std::string loading = "";
    if (m_model.bucketsLoading() || m_model.previewLoading()) {
        loading = " [Loading...]";
    }

    // Format: "Profile: name | Path: s3://bucket/prefix | [Loading...]"
    std::string header = "Profile: " + profileName + " | Path: " + path + loading;
    header = truncateString(header, m_termWidth - 2);

    mvwprintw(m_topBar, 0, 1, "%s", header.c_str());

    wattroff(m_topBar, COLOR_PAIR(COLOR_HEADER) | A_BOLD);
}

void BrowserTUI::renderLeftPane()
{
    if (!m_leftPane) return;

    // Draw border
    box(m_leftPane, 0, 0);

    // Render content based on current view
    if (m_model.isAtRoot()) {
        renderBucketList();
    } else {
        renderFolderContents();
    }
}

void BrowserTUI::renderBucketList()
{
    const auto& buckets = m_model.buckets();
    const std::string& error = m_model.bucketsError();

    // Show title
    wattron(m_leftPane, COLOR_PAIR(COLOR_HEADER));
    mvwprintw(m_leftPane, 0, 2, " Buckets ");
    wattroff(m_leftPane, COLOR_PAIR(COLOR_HEADER));

    // Show error if any
    if (!error.empty()) {
        wattron(m_leftPane, COLOR_PAIR(COLOR_ERROR));
        mvwprintw(m_leftPane, 2, 2, "Error: %s", truncateString(error, m_termWidth/2 - 4).c_str());
        wattroff(m_leftPane, COLOR_PAIR(COLOR_ERROR));
        return;
    }

    // Show buckets
    int height, width;
    getmaxyx(m_leftPane, height, width);
    int contentHeight = height - 2;  // Account for borders

    // Adjust scroll offset if needed
    int numBuckets = buckets.size();
    if (m_selectedIndex >= numBuckets) {
        m_selectedIndex = numBuckets > 0 ? numBuckets - 1 : 0;
    }

    // Ensure selected item is visible
    if (m_selectedIndex < m_scrollOffset) {
        m_scrollOffset = m_selectedIndex;
    }
    if (m_selectedIndex >= m_scrollOffset + contentHeight) {
        m_scrollOffset = m_selectedIndex - contentHeight + 1;
    }

    // Render visible buckets
    for (int i = 0; i < contentHeight && i + m_scrollOffset < numBuckets; ++i) {
        int bucketIdx = i + m_scrollOffset;
        const auto& bucket = buckets[bucketIdx];

        // Highlight selected item
        bool isSelected = (bucketIdx == m_selectedIndex && !m_focusOnRight);
        if (isSelected) {
            wattron(m_leftPane, COLOR_PAIR(COLOR_SELECTED) | A_BOLD);
        } else {
            wattron(m_leftPane, COLOR_PAIR(COLOR_FOLDER));
        }

        std::string display = "> " + bucket.name;
        display = truncateString(display, width - 4);
        mvwprintw(m_leftPane, i + 1, 2, "%s", display.c_str());

        // Clear to end of line
        wclrtoeol(m_leftPane);

        if (isSelected) {
            wattroff(m_leftPane, COLOR_PAIR(COLOR_SELECTED) | A_BOLD);
        } else {
            wattroff(m_leftPane, COLOR_PAIR(COLOR_FOLDER));
        }
    }
}

void BrowserTUI::renderFolderContents()
{
    const FolderNode* node = m_model.getNode(m_model.currentBucket(), m_model.currentPrefix());
    if (!node) return;

    // Show title with current folder
    wattron(m_leftPane, COLOR_PAIR(COLOR_HEADER));
    std::string title = " " + m_model.currentBucket();
    if (!m_model.currentPrefix().empty()) {
        title += "/" + m_model.currentPrefix();
    }
    title += " ";
    title = truncateString(title, m_termWidth/2 - 4);
    mvwprintw(m_leftPane, 0, 2, "%s", title.c_str());
    wattroff(m_leftPane, COLOR_PAIR(COLOR_HEADER));

    // Show error if any
    if (!node->error.empty()) {
        wattron(m_leftPane, COLOR_PAIR(COLOR_ERROR));
        mvwprintw(m_leftPane, 2, 2, "Error: %s", truncateString(node->error, m_termWidth/2 - 4).c_str());
        wattroff(m_leftPane, COLOR_PAIR(COLOR_ERROR));
        return;
    }

    int height, width;
    getmaxyx(m_leftPane, height, width);
    int contentHeight = height - 2;

    // Build display list: [..] parent + objects
    std::vector<std::string> displayItems;
    std::vector<bool> isFolder;
    std::vector<int64_t> sizes;

    // Add parent entry if not at bucket root
    if (!m_model.currentPrefix().empty()) {
        displayItems.push_back("[..]");
        isFolder.push_back(true);
        sizes.push_back(0);
    }

    // Add objects
    for (const auto& obj : node->objects) {
        // Determine display name and type
        std::string displayName = obj.key;

        // Remove current prefix to show relative path
        if (!m_model.currentPrefix().empty() && displayName.find(m_model.currentPrefix()) == 0) {
            displayName = displayName.substr(m_model.currentPrefix().size());
        }

        bool isDir = obj.is_folder;
        displayItems.push_back(isDir ? "> " + displayName : "  " + displayName);
        isFolder.push_back(isDir);
        sizes.push_back(obj.size);
    }

    // Adjust selection
    int numItems = displayItems.size();
    if (m_selectedIndex >= numItems) {
        m_selectedIndex = numItems > 0 ? numItems - 1 : 0;
    }

    // Ensure selected item is visible
    if (m_selectedIndex < m_scrollOffset) {
        m_scrollOffset = m_selectedIndex;
    }
    if (m_selectedIndex >= m_scrollOffset + contentHeight) {
        m_scrollOffset = m_selectedIndex - contentHeight + 1;
    }

    // Render visible items
    for (int i = 0; i < contentHeight && i + m_scrollOffset < numItems; ++i) {
        int itemIdx = i + m_scrollOffset;

        bool isSelected = (itemIdx == m_selectedIndex && !m_focusOnRight);
        if (isSelected) {
            wattron(m_leftPane, COLOR_PAIR(COLOR_SELECTED) | A_BOLD);
        } else if (isFolder[itemIdx]) {
            wattron(m_leftPane, COLOR_PAIR(COLOR_FOLDER));
        } else {
            wattron(m_leftPane, COLOR_PAIR(COLOR_NORMAL));
        }

        // Format: "  name             size"
        std::string name = displayItems[itemIdx];
        std::string size = isFolder[itemIdx] ? "" : formatSize(sizes[itemIdx]);

        int nameWidth = width - 16;  // Leave space for size
        name = truncateString(name, nameWidth);

        mvwprintw(m_leftPane, i + 1, 2, "%-*s %12s", nameWidth, name.c_str(), size.c_str());
        wclrtoeol(m_leftPane);

        if (isSelected) {
            wattroff(m_leftPane, COLOR_PAIR(COLOR_SELECTED) | A_BOLD);
        } else if (isFolder[itemIdx]) {
            wattroff(m_leftPane, COLOR_PAIR(COLOR_FOLDER));
        } else {
            wattroff(m_leftPane, COLOR_PAIR(COLOR_NORMAL));
        }
    }

    // Show "loading more" indicator if truncated
    if (node->is_truncated) {
        mvwprintw(m_leftPane, contentHeight, 2, "[Loading more...]");
    }
}

void BrowserTUI::renderRightPane()
{
    if (!m_rightPane) return;

    // Draw border
    box(m_rightPane, 0, 0);

    // Show title
    wattron(m_rightPane, COLOR_PAIR(COLOR_HEADER));
    mvwprintw(m_rightPane, 0, 2, " Preview ");
    wattroff(m_rightPane, COLOR_PAIR(COLOR_HEADER));

    renderPreview();
}

void BrowserTUI::renderPreview()
{
    int height, width;
    getmaxyx(m_rightPane, height, width);
    int contentHeight = height - 2;
    int contentWidth = width - 4;

    // Show message if no file selected
    if (!m_model.hasSelection()) {
        mvwprintw(m_rightPane, 2, 2, "No file selected");
        return;
    }

    // Show loading indicator
    if (m_model.previewLoading()) {
        mvwprintw(m_rightPane, 2, 2, "Loading...");
        return;
    }

    // Show error if any
    const std::string& error = m_model.previewError();
    if (!error.empty()) {
        wattron(m_rightPane, COLOR_PAIR(COLOR_ERROR));
        mvwprintw(m_rightPane, 2, 2, "Error: %s", truncateString(error, contentWidth).c_str());
        wattroff(m_rightPane, COLOR_PAIR(COLOR_ERROR));
        return;
    }

    // Show "not supported" message
    if (!m_model.previewSupported()) {
        mvwprintw(m_rightPane, 2, 2, "Preview not supported for this file type");

        // Show file info
        std::string fileName = m_model.selectedKey();
        std::string sizeStr = formatSize(m_model.selectedFileSize());
        mvwprintw(m_rightPane, 4, 2, "File: %s", truncateString(fileName, contentWidth).c_str());
        mvwprintw(m_rightPane, 5, 2, "Size: %s", sizeStr.c_str());
        return;
    }

    // Get preview content
    std::string content = m_model.previewContent();

    // Split into lines and render with scrolling
    std::vector<std::string> lines;
    std::istringstream iss(content);
    std::string line;
    while (std::getline(iss, line)) {
        // Word wrap long lines
        while ((int)line.length() > contentWidth) {
            lines.push_back(line.substr(0, contentWidth));
            line = line.substr(contentWidth);
        }
        lines.push_back(line);
    }

    // Adjust preview scroll offset
    int numLines = lines.size();
    if (m_previewScrollOffset >= numLines) {
        m_previewScrollOffset = numLines > contentHeight ? numLines - contentHeight : 0;
    }
    if (m_previewScrollOffset < 0) {
        m_previewScrollOffset = 0;
    }

    // Render visible lines
    for (int i = 0; i < contentHeight && i + m_previewScrollOffset < numLines; ++i) {
        int lineIdx = i + m_previewScrollOffset;
        std::string displayLine = lines[lineIdx];
        if ((int)displayLine.length() > contentWidth) {
            displayLine = displayLine.substr(0, contentWidth);
        }
        mvwprintw(m_rightPane, i + 1, 2, "%s", displayLine.c_str());
    }

    // Show scroll indicator
    if (numLines > contentHeight) {
        int scrollPercent = (m_previewScrollOffset * 100) / (numLines - contentHeight);
        mvwprintw(m_rightPane, height - 1, width - 8, " %3d%% ", scrollPercent);
    }
}

void BrowserTUI::renderStatusBar()
{
    if (!m_statusBar) return;

    wattron(m_statusBar, COLOR_PAIR(COLOR_STATUS));

    std::string status;

    if (m_model.isAtRoot()) {
        // Bucket list view
        int numBuckets = m_model.buckets().size();
        status = formatNumber(numBuckets) + " buckets";
    } else {
        // Folder view
        const FolderNode* node = m_model.getNode(m_model.currentBucket(), m_model.currentPrefix());
        if (node) {
            int numObjects = 0;
            int64_t totalSize = 0;
            for (const auto& obj : node->objects) {
                if (!obj.is_folder) {
                    numObjects++;
                    totalSize += obj.size;
                }
            }
            status = formatNumber(numObjects) + " objects | " + formatSize(totalSize);
        }
    }

    // Add keyboard shortcuts
    status += " | q:quit r:refresh Tab:switch";
    status = truncateString(status, m_termWidth - 2);

    mvwprintw(m_statusBar, 0, 1, "%s", status.c_str());

    wattroff(m_statusBar, COLOR_PAIR(COLOR_STATUS));
}

bool BrowserTUI::handleInput(int ch)
{
    switch (ch) {
        case 'q':
        case 'Q':
            m_shouldQuit = true;
            return true;

        case 'r':
        case 'R':
            handleRefresh();
            return true;

        case '\t':  // Tab key
            m_focusOnRight = !m_focusOnRight;
            return true;

        case KEY_UP:
        case 'k':
            if (m_focusOnRight) {
                m_previewScrollOffset--;
                if (m_previewScrollOffset < 0) m_previewScrollOffset = 0;
            } else {
                moveSelection(-1);
            }
            return true;

        case KEY_DOWN:
        case 'j':
            if (m_focusOnRight) {
                m_previewScrollOffset++;
            } else {
                moveSelection(1);
            }
            return true;

        case KEY_PPAGE:  // Page Up
            if (m_focusOnRight) {
                int height = getmaxy(m_rightPane) - 2;
                m_previewScrollOffset -= height;
                if (m_previewScrollOffset < 0) m_previewScrollOffset = 0;
            } else {
                moveSelection(-10);
            }
            return true;

        case KEY_NPAGE:  // Page Down
            if (m_focusOnRight) {
                int height = getmaxy(m_rightPane) - 2;
                m_previewScrollOffset += height;
            } else {
                moveSelection(10);
            }
            return true;

        case '\n':  // Enter
        case KEY_ENTER:
            handleEnter();
            return true;

        case KEY_BACKSPACE:
        case 127:  // Backspace
        case 'h':
            if (!m_focusOnRight) {
                handleBackspace();
            }
            return true;

        case KEY_LEFT:
            handleBackspace();
            return true;
    }

    return false;
}

void BrowserTUI::moveSelection(int delta)
{
    m_selectedIndex += delta;

    // Get max index
    int maxIndex = 0;
    if (m_model.isAtRoot()) {
        maxIndex = m_model.buckets().size() - 1;
    } else {
        const FolderNode* node = m_model.getNode(m_model.currentBucket(), m_model.currentPrefix());
        if (node) {
            maxIndex = node->objects.size();
            if (!m_model.currentPrefix().empty()) {
                maxIndex++;  // Account for [..] entry
            }
            maxIndex--;
        }
    }

    // Clamp
    if (m_selectedIndex < 0) m_selectedIndex = 0;
    if (m_selectedIndex > maxIndex) m_selectedIndex = maxIndex;
}

void BrowserTUI::handleEnter()
{
    if (m_model.isAtRoot()) {
        // Navigate into selected bucket
        const auto& buckets = m_model.buckets();
        if (m_selectedIndex >= 0 && m_selectedIndex < (int)buckets.size()) {
            m_model.navigateInto(buckets[m_selectedIndex].name, "");
            m_selectedIndex = 0;
            m_scrollOffset = 0;
        }
    } else {
        // Navigate into folder or select file
        const FolderNode* node = m_model.getNode(m_model.currentBucket(), m_model.currentPrefix());
        if (!node) return;

        int itemIdx = m_selectedIndex;

        // Handle [..] parent navigation
        if (!m_model.currentPrefix().empty() && itemIdx == 0) {
            handleBackspace();
            return;
        }

        // Adjust index if [..] entry exists
        if (!m_model.currentPrefix().empty()) {
            itemIdx--;
        }

        if (itemIdx >= 0 && itemIdx < (int)node->objects.size()) {
            const auto& obj = node->objects[itemIdx];

            if (obj.is_folder) {
                // Navigate into folder
                m_model.navigateInto(m_model.currentBucket(), obj.key);
                m_selectedIndex = 0;
                m_scrollOffset = 0;
            } else {
                // Select file for preview
                m_model.selectFile(m_model.currentBucket(), obj.key);
                m_previewScrollOffset = 0;
            }
        }
    }
}

void BrowserTUI::handleBackspace()
{
    if (!m_model.isAtRoot()) {
        m_model.navigateUp();
        m_selectedIndex = 0;
        m_scrollOffset = 0;
    }
}

void BrowserTUI::handleRefresh()
{
    m_model.refresh();
}

// Helper functions

std::string BrowserTUI::formatSize(int64_t bytes)
{
    if (bytes < 0) return "???";
    if (bytes < 1024) return std::to_string(bytes) + " B";
    if (bytes < 1024 * 1024) {
        double kb = bytes / 1024.0;
        std::ostringstream oss;
        oss << std::fixed << std::setprecision(1) << kb << " KB";
        return oss.str();
    }
    if (bytes < 1024LL * 1024 * 1024) {
        double mb = bytes / (1024.0 * 1024.0);
        std::ostringstream oss;
        oss << std::fixed << std::setprecision(1) << mb << " MB";
        return oss.str();
    }
    double gb = bytes / (1024.0 * 1024.0 * 1024.0);
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(1) << gb << " GB";
    return oss.str();
}

std::string BrowserTUI::formatNumber(int64_t number)
{
    if (number < 1000) return std::to_string(number);
    if (number < 1000000) {
        double k = number / 1000.0;
        std::ostringstream oss;
        oss << std::fixed << std::setprecision(1) << k << "K";
        return oss.str();
    }
    double m = number / 1000000.0;
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(1) << m << "M";
    return oss.str();
}

std::string BrowserTUI::truncateString(const std::string& str, int maxLen)
{
    if ((int)str.length() <= maxLen) return str;
    if (maxLen < 3) return "...";
    return str.substr(0, maxLen - 3) + "...";
}

std::string BrowserTUI::buildS3Path(const std::string& bucket, const std::string& prefix)
{
    std::string path = "s3://" + bucket;
    if (!prefix.empty()) {
        path += "/" + prefix;
    }
    return path;
}
