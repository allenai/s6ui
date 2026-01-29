#pragma once

#include <string>
#include <vector>
#include <atomic>
#include <thread>
#include <mutex>
#include <unordered_map>
#include <cstdint>

struct TextPosition {
    uint64_t line = 0;
    uint32_t byteOffset = 0;
    bool operator==(const TextPosition& o) const { return line == o.line && byteOffset == o.byteOffset; }
    bool operator<(const TextPosition& o) const { return line < o.line || (line == o.line && byteOffset < o.byteOffset); }
    bool operator<=(const TextPosition& o) const { return *this == o || *this < o; }
};

class MmapTextViewer {
public:
    MmapTextViewer();
    ~MmapTextViewer();

    MmapTextViewer(const MmapTextViewer&) = delete;
    MmapTextViewer& operator=(const MmapTextViewer&) = delete;

    bool open(const std::string& filePath);
    void close();
    bool isOpen() const;
    uint64_t fileSize() const;
    uint64_t lineCount() const;

    void render(float width, float height);

    void setWordWrap(bool enabled);
    bool wordWrap() const;

    void scrollToTop();
    void scrollToBottom();
    void scrollToLine(uint64_t line);

private:
    // Line data access - returns pointer into mmap and length
    struct LineData {
        const char* ptr;
        uint64_t length;
    };
    LineData getLineData(uint64_t lineIndex) const;

    // Newline indexing (runs on background thread)
    void buildNewlineIndex();

    // Word wrap
    struct WrapInfo {
        uint32_t visualRowCount;
        std::vector<uint32_t> rowStartOffsets; // byte offsets within the line
    };

    struct WrapCacheKey {
        uint64_t line;
        float width;
        bool operator==(const WrapCacheKey& o) const {
            return line == o.line && width == o.width;
        }
    };

    struct WrapCacheKeyHash {
        size_t operator()(const WrapCacheKey& k) const {
            size_t h1 = std::hash<uint64_t>{}(k.line);
            size_t h2 = std::hash<float>{}(k.width);
            return h1 ^ (h2 << 1);
        }
    };

    WrapInfo computeWrapInfo(uint64_t lineIndex, float wrapWidth) const;

    // Scrollbar
    void renderScrollbar(float x, float y, float height, float totalLines);
    float estimateAverageVisualRowsPerLine();

    // Scroll helpers
    void scrollByVisualRows(int64_t rows);

    // File mapping
    int m_fd = -1;
    void* m_mapBase = nullptr;
    uint64_t m_fileSize = 0;
    std::string m_filePath;

    // Newline index
    std::vector<uint64_t> m_lineOffsets;
    std::thread m_indexThread;
    std::atomic<bool> m_indexingDone{false};
    std::atomic<uint64_t> m_indexedBytes{0};
    std::atomic<uint64_t> m_indexedLineCount{0};
    std::mutex m_lineOffsetsMutex;

    // Scroll state
    uint64_t m_anchorLine = 0;
    uint32_t m_anchorSubRow = 0;
    float m_smoothOffsetY = 0.0f;

    // Word wrap
    bool m_wordWrap = false;
    mutable std::unordered_map<WrapCacheKey, WrapInfo, WrapCacheKeyHash> m_wrapCache;
    float m_lastWrapWidth = 0.0f;

    // Selection
    TextPosition hitTest(float mouseX, float mouseY, float startX, float startY, float textX, float lineHeight) const;
    std::string getSelectedText() const;
    void copySelection();

    bool m_selectionActive = false;
    TextPosition m_selectionAnchor;
    TextPosition m_selectionEnd;
    bool m_mouseDown = false;

    // Scrollbar dragging
    bool m_scrollbarDragging = false;
    float m_scrollbarDragStartY = 0.0f;

    // Cached estimate
    float m_avgVisualRows = 1.0f;
    uint64_t m_avgVisualRowsSampleLine = 0;

    static constexpr uint64_t MAX_DISPLAY_LINE_BYTES = 65536;
    static constexpr float LINE_NUMBER_GUTTER_WIDTH = 60.0f;
    static constexpr float SCROLLBAR_WIDTH = 14.0f;
    static constexpr size_t WRAP_CACHE_MAX_SIZE = 4096;
};
