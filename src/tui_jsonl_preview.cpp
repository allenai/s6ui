#include "tui_jsonl_preview.h"
#include "streaming_preview.h"
#include "nlohmann/json.hpp"
#include <cctype>
#include <sstream>

bool TUIJsonlRenderer::isJsonlFile(const std::string& key)
{
    // Check for .jsonl, .ndjson, or .json extension (also handles compressed variants)
    size_t dotPos = key.rfind('.');
    if (dotPos == std::string::npos) return false;

    std::string ext = key.substr(dotPos);
    for (char& c : ext) {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }

    // If it's a compressed file (.gz, .zst, .zstd), check the inner extension
    if ((ext == ".gz" || ext == ".zst" || ext == ".zstd") && dotPos > 0) {
        std::string withoutCompression = key.substr(0, dotPos);
        size_t innerDotPos = withoutCompression.rfind('.');
        if (innerDotPos == std::string::npos) return false;
        ext = withoutCompression.substr(innerDotPos);
        for (char& c : ext) {
            c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        }
    }

    return ext == ".jsonl" || ext == ".ndjson" || ext == ".json";
}

bool TUIJsonlRenderer::canHandle(const std::string& key) const
{
    return isJsonlFile(key);
}

bool TUIJsonlRenderer::render(const TUIPreviewContext& ctx)
{
    auto* sp = ctx.streamingPreview;
    if (!sp) {
        mvwprintw(ctx.window, 2, 2, "Loading...");
        return true;
    }

    // Check if file changed - reset state
    std::string fullKey = ctx.bucket + "/" + ctx.key;
    bool fileChanged = (m_currentKey != fullKey);
    if (fileChanged) {
        m_currentKey = fullKey;
        m_currentLine = 0;
        m_cachedLineIndex = SIZE_MAX;
        m_formattedCache.clear();
        m_textFieldCache.clear();
        m_textFieldName.clear();
        m_validatedFirstLine = false;
        m_jsonScrollOffset = 0;
        m_textScrollOffset = 0;
    }

    // Validate first line once it's complete - if not valid JSON, trigger fallback
    if (!m_validatedFirstLine && sp->lineCount() > 0 && sp->isLineComplete(0)) {
        m_validatedFirstLine = true;
        std::string firstLine = sp->getLine(0);
        if (!isValidJsonLine(firstLine)) {
            // First line isn't valid JSON - mark this file for fallback
            m_fallbackKey = fullKey;
            mvwprintw(ctx.window, 2, 2, "Not valid JSONL, switching to text view...");
            return true;
        }
    }

    // If this file was marked for fallback, don't render
    if (m_fallbackKey == fullKey) {
        return true;
    }

    // Get line count and calculate streaming progress
    size_t lineCount = sp->lineCount();
    bool isStreaming = !sp->isComplete();
    float progress = 0.0f;
    if (isStreaming && sp->totalSourceBytes() > 0) {
        progress = static_cast<float>(sp->bytesDownloaded()) / static_cast<float>(sp->totalSourceBytes());
    }

    // Clamp current line to valid range
    if (m_currentLine >= lineCount && lineCount > 0) {
        m_currentLine = lineCount - 1;
    }

    // Render navigation bar (line 1-2 of window)
    renderNavigationBar(ctx, lineCount, isStreaming, progress);

    // Get current line content (starting from line 3)
    if (lineCount > 0) {
        bool lineComplete = sp->isLineComplete(m_currentLine);
        std::string lineContent = sp->getLine(m_currentLine);
        renderLineContent(ctx, lineContent, lineComplete, fileChanged);
    } else {
        mvwprintw(ctx.window, 3, 2, "No lines loaded yet...");
    }

    return true;
}

void TUIJsonlRenderer::renderNavigationBar(const TUIPreviewContext& ctx, size_t lineCount,
                                            bool isStreaming, float progress)
{
    // Line 1: Navigation controls
    std::string navStr = "< > Line ";
    navStr += std::to_string(m_currentLine + 1);
    navStr += " / ";
    navStr += std::to_string(lineCount);

    // Add streaming progress if downloading
    if (isStreaming) {
        navStr += " (";
        navStr += std::to_string(static_cast<int>(progress * 100));
        navStr += "%)";
    }

    // Add mode indicator
    if (m_rawMode) {
        navStr += " [RAW]";
    } else {
        navStr += " [FORMATTED]";
    }

    // Add split-view indicator if text field exists
    if (!m_textFieldCache.empty()) {
        navStr += m_splitView ? " [SPLIT]" : " [JSON]";
    }

    mvwprintw(ctx.window, 1, 2, "%s", navStr.c_str());

    // Line 2: Help text
    mvwprintw(ctx.window, 2, 2, "h/l or Left/Right: navigate | Space: toggle raw | s: toggle split");
}

void TUIJsonlRenderer::renderLineContent(const TUIPreviewContext& ctx, const std::string& lineContent,
                                          bool lineComplete, bool fileChanged)
{
    if (lineContent.empty()) {
        mvwprintw(ctx.window, 4, 2, "(empty line)");
        return;
    }

    // Handle incomplete lines
    if (!lineComplete) {
        // Line is still being downloaded - invalidate cache
        if (m_cachedLineIndex == m_currentLine) {
            m_cachedLineIndex = SIZE_MAX;
        }

        std::string msg = "Line incomplete (";
        msg += std::to_string(lineContent.size());
        msg += " bytes loaded)...";
        mvwprintw(ctx.window, 4, 2, "%s", msg.c_str());

        // Show partial content starting at line 5
        int startLine = 5;
        int maxLines = ctx.availHeight - startLine;
        std::istringstream iss(lineContent);
        std::string line;
        int lineNum = 0;
        while (std::getline(iss, line) && lineNum < maxLines) {
            if (static_cast<int>(line.length()) > ctx.availWidth) {
                line = line.substr(0, ctx.availWidth);
            }
            mvwprintw(ctx.window, startLine + lineNum, 2, "%s", line.c_str());
            lineNum++;
        }
        return;
    }

    // Handle complete lines
    if (m_rawMode) {
        // Raw mode - show unformatted JSON
        int startLine = 4;
        int maxLines = ctx.availHeight - startLine;
        std::istringstream iss(lineContent);
        std::string line;
        int lineNum = 0;
        while (std::getline(iss, line) && lineNum < maxLines) {
            if (static_cast<int>(line.length()) > ctx.availWidth) {
                line = line.substr(0, ctx.availWidth);
            }
            mvwprintw(ctx.window, startLine + lineNum, 2, "%s", line.c_str());
            lineNum++;
        }
    } else {
        // Formatted mode - update cache if needed
        bool lineChanged = (m_cachedLineIndex != m_currentLine);
        if (lineChanged) {
            updateCache(lineContent);
            m_cachedLineIndex = m_currentLine;
        }

        int startLine = 4;
        int availLines = ctx.availHeight - startLine;

        // Check if we have text field for split-view
        if (!m_textFieldCache.empty() && m_splitView) {
            // Split view: top half JSON, bottom half text
            int jsonHeight = availLines * 4 / 10;  // 40%
            int textHeight = availLines - jsonHeight - 1;  // 60% minus separator

            // Render JSON section
            std::istringstream jsonIss(m_formattedCache);
            std::string line;
            int lineNum = 0;
            int jsonLine = 0;

            // Skip lines based on scroll offset
            while (std::getline(jsonIss, line) && jsonLine < m_jsonScrollOffset) {
                jsonLine++;
            }

            // Render visible JSON lines
            while (std::getline(jsonIss, line) && lineNum < jsonHeight) {
                if (static_cast<int>(line.length()) > ctx.availWidth) {
                    line = line.substr(0, ctx.availWidth);
                }
                mvwprintw(ctx.window, startLine + lineNum, 2, "%s", line.c_str());
                lineNum++;
            }

            // Draw separator
            int sepLine = startLine + jsonHeight;
            for (int i = 0; i < ctx.availWidth; ++i) {
                mvwaddch(ctx.window, sepLine, 2 + i, ACS_HLINE);
            }

            // Render text section with word wrap
            std::istringstream textIss(m_textFieldCache);
            lineNum = 0;
            int textLine = 0;

            // Skip lines based on scroll offset
            while (std::getline(textIss, line) && textLine < m_textScrollOffset) {
                textLine++;
            }

            // Render visible text lines with word wrap
            while (std::getline(textIss, line) && lineNum < textHeight) {
                // Simple word wrap: break at width
                size_t pos = 0;
                while (pos < line.length() && lineNum < textHeight) {
                    std::string segment = line.substr(pos, ctx.availWidth);
                    mvwprintw(ctx.window, sepLine + 1 + lineNum, 2, "%s", segment.c_str());
                    pos += ctx.availWidth;
                    lineNum++;
                }
            }
        } else {
            // No text field or split-view disabled - show JSON only
            std::istringstream iss(m_formattedCache);
            std::string line;
            int lineNum = 0;
            int scrolledLines = 0;

            // Skip lines based on scroll offset
            while (std::getline(iss, line) && scrolledLines < m_jsonScrollOffset) {
                scrolledLines++;
            }

            // Render visible lines
            while (std::getline(iss, line) && lineNum < availLines) {
                if (static_cast<int>(line.length()) > ctx.availWidth) {
                    line = line.substr(0, ctx.availWidth);
                }
                mvwprintw(ctx.window, startLine + lineNum, 2, "%s", line.c_str());
                lineNum++;
            }
        }
    }
}

bool TUIJsonlRenderer::handleInput(int ch, const TUIPreviewContext& ctx)
{
    bool handled = false;

    switch (ch) {
        case KEY_LEFT:
        case 'h':
            // Navigate to previous line
            navigateLine(-1, ctx);
            handled = true;
            break;

        case KEY_RIGHT:
        case 'l':
            // Navigate to next line
            navigateLine(1, ctx);
            handled = true;
            break;

        case ' ':
            // Toggle raw/formatted mode
            m_rawMode = !m_rawMode;
            m_cachedLineIndex = SIZE_MAX;  // Force cache refresh
            handled = true;
            break;

        case 's':
        case 'S':
            // Toggle split-view (only if text field exists)
            if (!m_textFieldCache.empty()) {
                m_splitView = !m_splitView;
            }
            handled = true;
            break;

        case KEY_UP:
        case 'k':
            // Scroll JSON or text content up
            if (m_jsonScrollOffset > 0) {
                m_jsonScrollOffset--;
            }
            handled = true;
            break;

        case KEY_DOWN:
        case 'j':
            // Scroll JSON or text content down
            m_jsonScrollOffset++;
            handled = true;
            break;

        case KEY_PPAGE:  // Page Up
            m_jsonScrollOffset -= 10;
            if (m_jsonScrollOffset < 0) m_jsonScrollOffset = 0;
            handled = true;
            break;

        case KEY_NPAGE:  // Page Down
            m_jsonScrollOffset += 10;
            handled = true;
            break;
    }

    return handled;
}

void TUIJsonlRenderer::reset()
{
    m_currentKey.clear();
    m_currentLine = 0;
    m_rawMode = false;
    m_splitView = true;
    m_formattedCache.clear();
    m_textFieldCache.clear();
    m_textFieldName.clear();
    m_cachedLineIndex = SIZE_MAX;
    m_jsonScrollOffset = 0;
    m_textScrollOffset = 0;
    m_validatedFirstLine = false;
    // Don't clear m_fallbackKey - it needs to persist
}

bool TUIJsonlRenderer::wantsFallback(const std::string& bucket, const std::string& key) const
{
    std::string fullKey = bucket + "/" + key;
    return fullKey == m_fallbackKey;
}

bool TUIJsonlRenderer::isValidJsonLine(const std::string& line)
{
    if (line.empty()) return false;

    // Quick check: valid JSON objects/arrays start with { or [
    size_t start = 0;
    while (start < line.size() && std::isspace(static_cast<unsigned char>(line[start]))) {
        ++start;
    }
    if (start >= line.size()) return false;

    char firstChar = line[start];
    if (firstChar != '{' && firstChar != '[') {
        return false;
    }

    // Try to parse as JSON
    try {
        (void)nlohmann::json::parse(line);
        return true;
    } catch (...) {
        return false;
    }
}

void TUIJsonlRenderer::navigateLine(int delta, const TUIPreviewContext& ctx)
{
    auto* sp = ctx.streamingPreview;
    if (!sp) return;

    size_t lineCount = sp->lineCount();
    if (lineCount == 0) return;

    if (delta < 0) {
        size_t absDelta = static_cast<size_t>(-delta);
        if (m_currentLine >= absDelta) {
            m_currentLine -= absDelta;
        } else {
            m_currentLine = 0;
        }
    } else if (delta > 0) {
        size_t newLine = m_currentLine + static_cast<size_t>(delta);
        if (newLine < lineCount) {
            m_currentLine = newLine;
        } else {
            m_currentLine = lineCount - 1;
        }
    }

    // Reset scroll offsets when changing lines
    m_jsonScrollOffset = 0;
    m_textScrollOffset = 0;
}

void TUIJsonlRenderer::updateCache(const std::string& rawJson)
{
    try {
        auto parsed = nlohmann::json::parse(rawJson);
        m_formattedCache = parsed.dump(2);  // Pretty-print with 2-space indent
        m_textFieldCache = extractTextField(rawJson, m_textFieldName);
    } catch (const nlohmann::json::parse_error& e) {
        // Not valid JSON - return original with error message
        m_formattedCache = std::string("(Invalid JSON: ") + e.what() + ")\n\n" + rawJson;
        m_textFieldCache.clear();
        m_textFieldName.clear();
    }
}

std::string TUIJsonlRenderer::extractTextField(const std::string& json, std::string& outFieldName)
{
    outFieldName.clear();

    try {
        auto parsed = nlohmann::json::parse(json);
        if (!parsed.is_object()) {
            return "";
        }

        // Only look for "text" field
        if (parsed.contains("text") && parsed["text"].is_string()) {
            outFieldName = "text";
            return parsed["text"].get<std::string>();
        }

    } catch (...) {
        // Ignore parse errors
    }

    return "";
}

std::string TUIJsonlRenderer::formatJson(const std::string& rawJson)
{
    try {
        auto parsed = nlohmann::json::parse(rawJson);
        return parsed.dump(2);  // Pretty-print with 2-space indent
    } catch (...) {
        return rawJson;  // Return original if parsing fails
    }
}
