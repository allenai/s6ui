#pragma once

#include "tui_preview_renderer.h"
#include <vector>
#include <string>

// Fallback text preview renderer - maintains current behavior
// Shows content as plain text with word wrapping and scrolling
class TUITextRenderer : public TUIPreviewRenderer {
public:
    TUITextRenderer() = default;
    ~TUITextRenderer() override = default;

    bool canHandle(const std::string& key) const override;
    bool render(const TUIPreviewContext& ctx) override;
    bool handleInput(int ch, const TUIPreviewContext& ctx) override;
    void reset() override;

    int scrollOffset() const override { return m_scrollOffset; }
    int totalLines() const override { return static_cast<int>(m_lines.size()); }

private:
    // Cached lines for current content
    std::vector<std::string> m_lines;
    int m_scrollOffset = 0;
    std::string m_currentKey;  // Track when content changes
};
