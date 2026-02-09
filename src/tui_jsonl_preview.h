#pragma once

#include "tui_preview_renderer.h"
#include <string>

// JSONL preview renderer for TUI
// Shows line-by-line paging with raw/formatted JSON toggle
// Optional split-view for "text" field extraction
class TUIJsonlRenderer : public TUIPreviewRenderer {
public:
    TUIJsonlRenderer() = default;
    ~TUIJsonlRenderer() override = default;

    bool canHandle(const std::string& key) const override;
    bool render(const TUIPreviewContext& ctx) override;
    bool handleInput(int ch, const TUIPreviewContext& ctx) override;
    void reset() override;

    int scrollOffset() const override { return m_jsonScrollOffset; }
    int totalLines() const override { return 0; }  // Not used for JSONL (line-based, not scroll-based)

    // Check if this file should fall back to text renderer
    bool wantsFallback(const std::string& bucket, const std::string& key) const;

    // Static helper to check file extension
    static bool isJsonlFile(const std::string& key);

private:
    // Validate if a line contains valid JSON
    static bool isValidJsonLine(const std::string& line);

    // Format JSON with pretty-printing
    std::string formatJson(const std::string& rawJson);

    // Extract text field from JSON object
    std::string extractTextField(const std::string& json, std::string& outFieldName);

    // Navigate to a different line
    void navigateLine(int delta, const TUIPreviewContext& ctx);

    // Update formatted cache for current line
    void updateCache(const std::string& rawJson);

    // Render helpers
    void renderNavigationBar(const TUIPreviewContext& ctx, size_t lineCount, bool isStreaming, float progress);
    void renderLineContent(const TUIPreviewContext& ctx, const std::string& lineContent, bool lineComplete);

    // State
    std::string m_currentKey;           // Track current file (bucket/key)
    size_t m_currentLine = 0;           // Current line being viewed
    bool m_rawMode = false;             // Raw JSON vs formatted
    bool m_splitView = true;            // Split view enabled (when text field exists)

    // Caching
    std::string m_formattedCache;       // Formatted JSON for current line
    std::string m_textFieldCache;       // Extracted text field content
    std::string m_textFieldName;        // Name of the extracted field
    size_t m_cachedLineIndex = SIZE_MAX; // Which line is cached

    // Scrolling (for split-view)
    int m_jsonScrollOffset = 0;
    int m_textScrollOffset = 0;

    // Validation and fallback
    bool m_validatedFirstLine = false;
    std::string m_fallbackKey;          // File that should use text renderer
};
