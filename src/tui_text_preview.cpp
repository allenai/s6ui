#include "tui_text_preview.h"
#include "streaming_preview.h"
#include <sstream>

bool TUITextRenderer::canHandle(const std::string& key) const
{
    // This is the fallback renderer - it can handle any file type
    return true;
}

bool TUITextRenderer::render(const TUIPreviewContext& ctx)
{
    // Check if file changed - rebuild line cache
    std::string fullKey = ctx.bucket + "/" + ctx.key;
    if (m_currentKey != fullKey) {
        m_currentKey = fullKey;
        m_scrollOffset = 0;
        m_lines.clear();

        // Get content from streaming preview if available
        std::string content;
        if (ctx.streamingPreview) {
            content = ctx.streamingPreview->getAllContent();
        }

        // Split into lines and apply word wrapping
        std::istringstream iss(content);
        std::string line;
        while (std::getline(iss, line)) {
            // Word wrap long lines
            while (static_cast<int>(line.length()) > ctx.availWidth) {
                m_lines.push_back(line.substr(0, ctx.availWidth));
                line = line.substr(ctx.availWidth);
            }
            m_lines.push_back(line);
        }
    }

    // Clamp scroll offset
    int numLines = static_cast<int>(m_lines.size());
    if (m_scrollOffset >= numLines) {
        m_scrollOffset = numLines > ctx.availHeight ? numLines - ctx.availHeight : 0;
    }
    if (m_scrollOffset < 0) {
        m_scrollOffset = 0;
    }

    // Render visible lines
    for (int i = 0; i < ctx.availHeight && i + m_scrollOffset < numLines; ++i) {
        int lineIdx = i + m_scrollOffset;
        std::string displayLine = m_lines[lineIdx];
        if (static_cast<int>(displayLine.length()) > ctx.availWidth) {
            displayLine = displayLine.substr(0, ctx.availWidth);
        }
        mvwprintw(ctx.window, i + 1, 2, "%s", displayLine.c_str());
    }

    return true;
}

bool TUITextRenderer::handleInput(int ch, const TUIPreviewContext& ctx)
{
    int numLines = static_cast<int>(m_lines.size());
    bool handled = false;

    switch (ch) {
        case KEY_UP:
        case 'k':
            if (m_scrollOffset > 0) {
                m_scrollOffset--;
                handled = true;
            }
            break;

        case KEY_DOWN:
        case 'j':
            if (m_scrollOffset < numLines - ctx.availHeight) {
                m_scrollOffset++;
                handled = true;
            }
            break;

        case KEY_PPAGE:  // Page Up
            m_scrollOffset -= ctx.availHeight;
            if (m_scrollOffset < 0) m_scrollOffset = 0;
            handled = true;
            break;

        case KEY_NPAGE:  // Page Down
            m_scrollOffset += ctx.availHeight;
            if (m_scrollOffset > numLines - ctx.availHeight) {
                m_scrollOffset = numLines - ctx.availHeight;
            }
            if (m_scrollOffset < 0) m_scrollOffset = 0;
            handled = true;
            break;

        case KEY_HOME:
        case 'g':
            m_scrollOffset = 0;
            handled = true;
            break;

        case KEY_END:
        case 'G':
            m_scrollOffset = numLines - ctx.availHeight;
            if (m_scrollOffset < 0) m_scrollOffset = 0;
            handled = true;
            break;
    }

    return handled;
}

void TUITextRenderer::reset()
{
    m_lines.clear();
    m_scrollOffset = 0;
    m_currentKey.clear();
}
