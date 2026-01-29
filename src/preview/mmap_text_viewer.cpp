#include "mmap_text_viewer.h"
#include "streaming_preview.h"
#include "imgui/imgui.h"

#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <algorithm>
#include <cmath>

MmapTextViewer::MmapTextViewer() = default;

MmapTextViewer::~MmapTextViewer() {
    close();
}

void MmapTextViewer::open(StreamingFilePreview* source) {
    close();

    if (!source)
        return;

    m_source = source;
    const std::string& path = source->tempFilePath();
    if (path.empty())
        return;

    m_fd = ::open(path.c_str(), O_RDONLY);
    if (m_fd < 0)
        return;

    uint64_t size = source->bytesWritten();
    m_fileSize = size;

    if (m_fileSize == 0) {
        // Valid but empty so far - will get data on refresh
        m_lineOffsets.push_back(0);
        return;
    }

    m_mapBase = mmap(nullptr, m_fileSize, PROT_READ, MAP_PRIVATE, m_fd, 0);
    if (m_mapBase == MAP_FAILED) {
        m_mapBase = nullptr;
        ::close(m_fd);
        m_fd = -1;
        return;
    }

    madvise(m_mapBase, m_fileSize, MADV_RANDOM);

    // Index newlines synchronously
    m_lineOffsets.push_back(0);
    indexNewlinesFrom(0);
}

void MmapTextViewer::refresh() {
    if (!m_source || m_fd < 0)
        return;

    uint64_t newSize = m_source->bytesWritten();
    if (newSize <= m_fileSize)
        return;

    // Unmap old mapping
    if (m_mapBase && m_mapBase != MAP_FAILED) {
        munmap(m_mapBase, m_fileSize);
        m_mapBase = nullptr;
    }

    uint64_t oldSize = m_fileSize;
    m_fileSize = newSize;

    m_mapBase = mmap(nullptr, m_fileSize, PROT_READ, MAP_PRIVATE, m_fd, 0);
    if (m_mapBase == MAP_FAILED) {
        m_mapBase = nullptr;
        return;
    }

    madvise(m_mapBase, m_fileSize, MADV_RANDOM);

    // If this is the first data (was empty before), add initial line offset
    if (oldSize == 0 && m_lineOffsets.empty()) {
        m_lineOffsets.push_back(0);
    }

    // Index newlines from where we left off
    indexNewlinesFrom(oldSize);

    // Invalidate wrap cache for the last line (it may have grown)
    if (!m_wrapCache.empty() && !m_lineOffsets.empty()) {
        uint64_t lastLine = m_lineOffsets.size() - 1;
        // Remove any cached wrap info for the last line at any width
        auto it = m_wrapCache.begin();
        while (it != m_wrapCache.end()) {
            if (it->first.line == lastLine) {
                it = m_wrapCache.erase(it);
            } else {
                ++it;
            }
        }
    }
}

void MmapTextViewer::close() {
    if (m_mapBase && m_mapBase != MAP_FAILED) {
        munmap(m_mapBase, m_fileSize);
        m_mapBase = nullptr;
    }
    if (m_fd >= 0) {
        ::close(m_fd);
        m_fd = -1;
    }

    m_source = nullptr;
    m_fileSize = 0;
    m_lineOffsets.clear();
    m_indexedBytes = 0;
    m_anchorLine = 0;
    m_anchorSubRow = 0;
    m_smoothOffsetY = 0.0f;
    m_wrapCache.clear();
    m_selectionActive = false;
    m_mouseDown = false;
    m_scrollbarDragging = false;
    m_avgVisualRows = 1.0f;
    m_avgVisualRowsSampleLine = 0;
}

bool MmapTextViewer::isOpen() const {
    return m_fd >= 0;
}

uint64_t MmapTextViewer::fileSize() const {
    return m_fileSize;
}

uint64_t MmapTextViewer::lineCount() const {
    return m_lineOffsets.size();
}

void MmapTextViewer::setWordWrap(bool enabled) {
    if (m_wordWrap != enabled) {
        m_wordWrap = enabled;
        m_wrapCache.clear();
        m_anchorSubRow = 0;
    }
}

bool MmapTextViewer::wordWrap() const {
    return m_wordWrap;
}

void MmapTextViewer::scrollToTop() {
    m_anchorLine = 0;
    m_anchorSubRow = 0;
    m_smoothOffsetY = 0.0f;
}

void MmapTextViewer::scrollToBottom() {
    uint64_t lc = lineCount();
    if (lc == 0) return;
    m_anchorLine = (lc > 100) ? lc - 100 : 0;
    m_anchorSubRow = 0;
    m_smoothOffsetY = 0.0f;
}

void MmapTextViewer::scrollToLine(uint64_t line) {
    uint64_t lc = lineCount();
    if (line >= lc && lc > 0) line = lc - 1;
    m_anchorLine = line;
    m_anchorSubRow = 0;
    m_smoothOffsetY = 0.0f;
}

void MmapTextViewer::indexNewlinesFrom(uint64_t fromByte) {
    if (!m_mapBase || m_fileSize == 0)
        return;

    const char* base = static_cast<const char*>(m_mapBase);
    uint64_t pos = fromByte;

    while (pos < m_fileSize) {
        const void* found = memchr(base + pos, '\n', m_fileSize - pos);
        if (!found)
            break;

        uint64_t nlPos = static_cast<uint64_t>(static_cast<const char*>(found) - base);
        uint64_t nextLineStart = nlPos + 1;

        if (nextLineStart < m_fileSize) {
            m_lineOffsets.push_back(nextLineStart);
        }

        pos = nextLineStart;
    }

    m_indexedBytes = m_fileSize;
}

MmapTextViewer::LineData MmapTextViewer::getLineData(uint64_t lineIndex) const {
    const char* base = static_cast<const char*>(m_mapBase);
    if (!base) return {nullptr, 0};

    uint64_t lc = m_lineOffsets.size();
    if (lineIndex >= lc)
        return {nullptr, 0};

    uint64_t start = m_lineOffsets[lineIndex];

    uint64_t end;
    if (lineIndex + 1 < lc) {
        end = m_lineOffsets[lineIndex + 1];
    } else {
        end = m_fileSize;
    }

    // Strip trailing \n and \r
    while (end > start && (base[end - 1] == '\n' || base[end - 1] == '\r'))
        --end;

    return {base + start, end - start};
}

MmapTextViewer::WrapInfo MmapTextViewer::computeWrapInfo(uint64_t lineIndex, float wrapWidth) const {
    WrapInfo info;
    info.visualRowCount = 1;
    info.rowStartOffsets.push_back(0);

    LineData ld = getLineData(lineIndex);
    if (!ld.ptr || ld.length == 0)
        return info;

    ImFontBaked* font = ImGui::GetFontBaked();
    float x = 0.0f;
    uint32_t lastBreakOffset = 0;
    uint32_t lineLen = static_cast<uint32_t>(std::min(ld.length, static_cast<uint64_t>(MAX_DISPLAY_LINE_BYTES)));

    uint32_t i = 0;
    while (i < lineLen) {
        // Decode UTF-8 character
        unsigned int c = 0;
        int charBytes = 1;
        unsigned char ch = static_cast<unsigned char>(ld.ptr[i]);
        if (ch < 0x80) {
            c = ch;
        } else if (ch < 0xE0 && i + 1 < lineLen) {
            c = ch;
            charBytes = 2;
        } else if (ch < 0xF0 && i + 2 < lineLen) {
            c = ch;
            charBytes = 3;
        } else if (i + 3 < lineLen) {
            c = ch;
            charBytes = 4;
        }

        float charWidth = font->GetCharAdvance(static_cast<ImWchar>(c));

        if (x + charWidth > wrapWidth && x > 0.0f) {
            uint32_t breakAt = i;

            if (lastBreakOffset > info.rowStartOffsets.back()) {
                breakAt = lastBreakOffset;
            }

            info.rowStartOffsets.push_back(breakAt);
            info.visualRowCount++;
            x = 0.0f;

            if (breakAt < i) {
                i = breakAt;
                continue;
            }
        }

        if (c == ' ' || c == '\t' || c == '-' || c == '/' || c == '\\' || c == ',' || c == ';') {
            lastBreakOffset = i + static_cast<uint32_t>(charBytes);
        }

        x += charWidth;
        i += static_cast<uint32_t>(charBytes);
    }

    return info;
}

float MmapTextViewer::estimateAverageVisualRowsPerLine() {
    if (!m_wordWrap)
        return 1.0f;

    uint64_t lc = lineCount();
    if (lc == 0)
        return 1.0f;

    uint64_t sampleCount = std::min(lc, static_cast<uint64_t>(1000));
    float totalRows = 0.0f;

    for (uint64_t i = 0; i < sampleCount; ++i) {
        uint64_t idx = (i * lc) / sampleCount;
        WrapInfo wi = computeWrapInfo(idx, m_lastWrapWidth);
        totalRows += wi.visualRowCount;
    }

    m_avgVisualRows = totalRows / static_cast<float>(sampleCount);
    m_avgVisualRowsSampleLine = lc;
    return m_avgVisualRows;
}

void MmapTextViewer::scrollByVisualRows(int64_t rows) {
    uint64_t lc = lineCount();
    if (lc == 0) return;

    if (rows > 0) {
        for (int64_t r = 0; r < rows; ++r) {
            if (m_wordWrap) {
                WrapInfo wi = computeWrapInfo(m_anchorLine, m_lastWrapWidth);
                if (m_anchorSubRow + 1 < wi.visualRowCount) {
                    m_anchorSubRow++;
                } else {
                    if (m_anchorLine + 1 < lc) {
                        m_anchorLine++;
                        m_anchorSubRow = 0;
                    }
                }
            } else {
                if (m_anchorLine + 1 < lc) {
                    m_anchorLine++;
                }
            }
        }
    } else {
        int64_t remaining = -rows;
        for (int64_t r = 0; r < remaining; ++r) {
            if (m_wordWrap) {
                if (m_anchorSubRow > 0) {
                    m_anchorSubRow--;
                } else if (m_anchorLine > 0) {
                    m_anchorLine--;
                    WrapInfo wi = computeWrapInfo(m_anchorLine, m_lastWrapWidth);
                    m_anchorSubRow = wi.visualRowCount - 1;
                }
            } else {
                if (m_anchorLine > 0) {
                    m_anchorLine--;
                }
            }
        }
    }
}

void MmapTextViewer::renderScrollbar(float x, float y, float height, float totalLines) {
    ImDrawList* dl = ImGui::GetWindowDrawList();
    float avg = m_wordWrap ? m_avgVisualRows : 1.0f;
    float totalVisualRows = totalLines * avg;
    float lineHeight = ImGui::GetTextLineHeightWithSpacing();
    float viewportRows = height / lineHeight;

    if (totalVisualRows <= viewportRows) {
        dl->AddRectFilled(ImVec2(x, y), ImVec2(x + SCROLLBAR_WIDTH, y + height),
                          IM_COL32(30, 30, 30, 255));
        return;
    }

    dl->AddRectFilled(ImVec2(x, y), ImVec2(x + SCROLLBAR_WIDTH, y + height),
                      IM_COL32(30, 30, 30, 255));

    float thumbRatio = viewportRows / totalVisualRows;
    float thumbH = std::max(20.0f, height * thumbRatio);
    float currentVisualRow = static_cast<float>(m_anchorLine) * avg + m_anchorSubRow;
    float scrollFraction = currentVisualRow / (totalVisualRows - viewportRows);
    scrollFraction = std::clamp(scrollFraction, 0.0f, 1.0f);
    float thumbY = y + scrollFraction * (height - thumbH);

    ImGuiIO& io = ImGui::GetIO();
    ImVec2 mousePos = io.MousePos;
    bool mouseInScrollbar = mousePos.x >= x && mousePos.x <= x + SCROLLBAR_WIDTH &&
                            mousePos.y >= y && mousePos.y <= y + height;

    if (mouseInScrollbar && ImGui::IsMouseClicked(0)) {
        if (mousePos.y >= thumbY && mousePos.y <= thumbY + thumbH) {
            m_scrollbarDragging = true;
            m_scrollbarDragStartY = mousePos.y - thumbY;
        } else {
            float clickFraction = (mousePos.y - y) / height;
            uint64_t targetLine = static_cast<uint64_t>(clickFraction * totalLines);
            scrollToLine(targetLine);
            m_scrollbarDragging = true;
            m_scrollbarDragStartY = thumbH * 0.5f;
        }
    }

    if (m_scrollbarDragging) {
        if (ImGui::IsMouseDown(0)) {
            float newThumbY = mousePos.y - m_scrollbarDragStartY;
            float newFraction = (newThumbY - y) / (height - thumbH);
            newFraction = std::clamp(newFraction, 0.0f, 1.0f);
            uint64_t targetLine = static_cast<uint64_t>(newFraction * (totalLines > 0 ? totalLines - 1 : 0));
            scrollToLine(targetLine);
        } else {
            m_scrollbarDragging = false;
        }
    }

    ImU32 thumbColor = m_scrollbarDragging ? IM_COL32(180, 180, 180, 255) :
                       mouseInScrollbar ? IM_COL32(140, 140, 140, 255) :
                       IM_COL32(100, 100, 100, 255);
    dl->AddRectFilled(ImVec2(x + 2, thumbY), ImVec2(x + SCROLLBAR_WIDTH - 2, thumbY + thumbH),
                      thumbColor, 4.0f);
}

TextPosition MmapTextViewer::hitTest(float mouseX, float mouseY, float /*startX*/, float startY, float textX, float lineHeight) const {
    TextPosition result;
    uint64_t lc = lineCount();
    if (lc == 0) return result;

    float relY = mouseY - startY;
    if (relY < 0.0f) relY = 0.0f;
    int visualRow = static_cast<int>(relY / lineHeight);

    uint64_t curLine = m_anchorLine;
    uint32_t curSubRow = m_anchorSubRow;
    int rowsToSkip = visualRow;

    float textAreaWidth = m_lastWrapWidth;

    while (rowsToSkip > 0 && curLine < lc) {
        if (m_wordWrap && textAreaWidth > 0.0f) {
            WrapCacheKey key{curLine, textAreaWidth};
            auto cacheIt = m_wrapCache.find(key);
            uint32_t totalRows = 1;
            if (cacheIt != m_wrapCache.end()) {
                totalRows = cacheIt->second.visualRowCount;
            } else {
                WrapInfo wi = computeWrapInfo(curLine, textAreaWidth);
                totalRows = wi.visualRowCount;
            }
            uint32_t remainingInLine = totalRows - curSubRow;
            if (static_cast<uint32_t>(rowsToSkip) < remainingInLine) {
                curSubRow += rowsToSkip;
                rowsToSkip = 0;
            } else {
                rowsToSkip -= remainingInLine;
                curLine++;
                curSubRow = 0;
            }
        } else {
            curLine++;
            rowsToSkip--;
        }
    }

    if (curLine >= lc) {
        curLine = lc - 1;
        curSubRow = 0;
    }

    result.line = curLine;

    LineData ld = getLineData(curLine);
    if (!ld.ptr || ld.length == 0) {
        result.byteOffset = 0;
        return result;
    }

    uint32_t lineLen = static_cast<uint32_t>(std::min(ld.length, static_cast<uint64_t>(MAX_DISPLAY_LINE_BYTES)));

    uint32_t rowStart = 0;
    uint32_t rowEnd = lineLen;

    if (m_wordWrap && textAreaWidth > 0.0f) {
        WrapCacheKey key{curLine, textAreaWidth};
        auto cacheIt = m_wrapCache.find(key);
        WrapInfo wi;
        if (cacheIt != m_wrapCache.end()) {
            wi = cacheIt->second;
        } else {
            wi = computeWrapInfo(curLine, textAreaWidth);
        }
        if (curSubRow < wi.visualRowCount) {
            rowStart = wi.rowStartOffsets[curSubRow];
            rowEnd = (curSubRow + 1 < wi.visualRowCount) ? wi.rowStartOffsets[curSubRow + 1] : lineLen;
        }
    }

    ImFontBaked* font = ImGui::GetFontBaked();
    float x = 0.0f;
    float targetX = mouseX - textX;
    if (targetX < 0.0f) {
        result.byteOffset = rowStart;
        return result;
    }

    uint32_t i = rowStart;
    while (i < rowEnd) {
        unsigned char ch = static_cast<unsigned char>(ld.ptr[i]);
        int charBytes = 1;
        if (ch >= 0xF0 && i + 3 < rowEnd) charBytes = 4;
        else if (ch >= 0xE0 && i + 2 < rowEnd) charBytes = 3;
        else if (ch >= 0xC0 && i + 1 < rowEnd) charBytes = 2;

        float charWidth = font->GetCharAdvance(static_cast<ImWchar>(ch));
        if (x + charWidth * 0.5f > targetX) {
            result.byteOffset = i;
            return result;
        }
        x += charWidth;
        i += charBytes;
    }

    result.byteOffset = rowEnd;
    return result;
}

std::string MmapTextViewer::getSelectedText() const {
    if (!m_selectionActive) return "";

    TextPosition start = m_selectionAnchor;
    TextPosition end = m_selectionEnd;
    if (end < start) std::swap(start, end);

    std::string result;

    if (start.line == end.line) {
        LineData ld = getLineData(start.line);
        if (ld.ptr && start.byteOffset < ld.length) {
            uint32_t s = start.byteOffset;
            uint32_t e = std::min(static_cast<uint32_t>(end.byteOffset), static_cast<uint32_t>(ld.length));
            if (e > s) result.append(ld.ptr + s, e - s);
        }
        return result;
    }

    {
        LineData ld = getLineData(start.line);
        if (ld.ptr && start.byteOffset < ld.length) {
            result.append(ld.ptr + start.byteOffset, ld.length - start.byteOffset);
        }
        result += '\n';
    }

    for (uint64_t line = start.line + 1; line < end.line; ++line) {
        LineData ld = getLineData(line);
        if (ld.ptr && ld.length > 0) {
            result.append(ld.ptr, ld.length);
        }
        result += '\n';
    }

    {
        LineData ld = getLineData(end.line);
        if (ld.ptr) {
            uint32_t e = std::min(static_cast<uint32_t>(end.byteOffset), static_cast<uint32_t>(ld.length));
            if (e > 0) result.append(ld.ptr, e);
        }
    }

    return result;
}

void MmapTextViewer::copySelection() {
    std::string text = getSelectedText();
    if (!text.empty()) {
        ImGui::SetClipboardText(text.c_str());
    }
}

void MmapTextViewer::render(float width, float height) {
    // Push a unique ID scope so multiple MmapTextViewer instances
    // can render in the same frame without ImGui ID conflicts
    ImGui::PushID(this);

    if (!isOpen()) {
        ImGui::TextColored(ImVec4(0.5f, 0.5f, 0.5f, 1.0f), "(no file open)");
        ImGui::PopID();
        return;
    }

    if (m_fileSize == 0) {
        ImGui::TextColored(ImVec4(0.5f, 0.5f, 0.5f, 1.0f), "(empty file)");
        ImGui::PopID();
        return;
    }

    uint64_t lc = lineCount();
    if (lc == 0) { ImGui::PopID(); return; }

    // Clamp anchor
    if (m_anchorLine >= lc)
        m_anchorLine = lc - 1;

    float lineHeight = ImGui::GetTextLineHeightWithSpacing();
    float textAreaWidth = width - LINE_NUMBER_GUTTER_WIDTH - SCROLLBAR_WIDTH;
    m_lastWrapWidth = textAreaWidth;

    // Clear wrap cache if width changed significantly
    if (!m_wrapCache.empty()) {
        auto it = m_wrapCache.begin();
        if (std::abs(it->first.width - textAreaWidth) > 1.0f) {
            m_wrapCache.clear();
        }
    }

    // Handle input
    ImGuiIO& io = ImGui::GetIO();
    ImVec2 windowPos = ImGui::GetCursorScreenPos();
    ImVec2 mousePos = io.MousePos;

    ImGui::SetNextItemAllowOverlap();
    ImGui::InvisibleButton("##viewer_input", ImVec2(width, height));
    bool viewerFocused = ImGui::IsItemFocused();

    bool mouseInArea = mousePos.x >= windowPos.x && mousePos.x < windowPos.x + width &&
                       mousePos.y >= windowPos.y && mousePos.y < windowPos.y + height;
    bool mouseInTextArea = mousePos.x >= windowPos.x && mousePos.x < windowPos.x + width - SCROLLBAR_WIDTH &&
                           mousePos.y >= windowPos.y && mousePos.y < windowPos.y + height;

    bool popupOpen = ImGui::IsPopupOpen("##textviewer_ctx");

    if ((mouseInArea || m_scrollbarDragging) && !popupOpen) {
        if (io.MouseWheel != 0.0f) {
            int rows = static_cast<int>(-io.MouseWheel * 3.0f);
            scrollByVisualRows(rows);
        }
    }

    if (viewerFocused) {
        if (ImGui::IsKeyPressed(ImGuiKey_DownArrow))
            scrollByVisualRows(1);
        if (ImGui::IsKeyPressed(ImGuiKey_UpArrow))
            scrollByVisualRows(-1);

        int visibleRows = static_cast<int>(height / lineHeight);
        if (ImGui::IsKeyPressed(ImGuiKey_PageDown))
            scrollByVisualRows(visibleRows);
        if (ImGui::IsKeyPressed(ImGuiKey_PageUp))
            scrollByVisualRows(-visibleRows);

        if (ImGui::IsKeyPressed(ImGuiKey_Home))
            scrollToTop();
        if (ImGui::IsKeyPressed(ImGuiKey_End))
            scrollToBottom();

        if (m_selectionActive && ImGui::IsKeyPressed(ImGuiKey_C) && (io.KeySuper || io.KeyCtrl)) {
            copySelection();
        }
    }

    if (mouseInTextArea && ImGui::IsMouseClicked(ImGuiMouseButton_Right)) {
        ImGui::OpenPopup("##textviewer_ctx");
    }
    if (ImGui::BeginPopup("##textviewer_ctx")) {
        bool hasSel = m_selectionActive;
        if (ImGui::MenuItem("Copy", "Cmd+C", false, hasSel)) {
            copySelection();
        }
        ImGui::EndPopup();
    }

    // Render content
    ImDrawList* dl = ImGui::GetWindowDrawList();
    float startX = windowPos.x;
    float startY = windowPos.y;
    float textX = startX + LINE_NUMBER_GUTTER_WIDTH + 4.0f;

    // Mouse click/drag handling for text selection
    if (mouseInTextArea && ImGui::IsMouseClicked(0) && !m_scrollbarDragging && !popupOpen) {
        TextPosition pos = hitTest(mousePos.x, mousePos.y, startX, startY, textX, lineHeight);
        m_selectionAnchor = pos;
        m_selectionEnd = pos;
        m_mouseDown = true;
        m_selectionActive = false;
    }

    if (m_mouseDown) {
        if (ImGui::IsMouseDown(0)) {
            TextPosition pos = hitTest(mousePos.x, mousePos.y, startX, startY, textX, lineHeight);
            m_selectionEnd = pos;
            if (!(m_selectionAnchor == m_selectionEnd)) {
                m_selectionActive = true;
            }
        } else {
            m_mouseDown = false;
        }
    }

    // Normalize selection for rendering
    TextPosition selStart, selEnd;
    if (m_selectionActive) {
        selStart = m_selectionAnchor;
        selEnd = m_selectionEnd;
        if (selEnd < selStart) std::swap(selStart, selEnd);
    }

    // Background
    dl->AddRectFilled(ImVec2(startX, startY),
                      ImVec2(startX + width, startY + height),
                      IM_COL32(20, 20, 20, 255));

    // Gutter background
    dl->AddRectFilled(ImVec2(startX, startY),
                      ImVec2(startX + LINE_NUMBER_GUTTER_WIDTH, startY + height),
                      IM_COL32(30, 30, 30, 255));

    // Helper lambda to compute pixel X offset for a byte offset within a line's text
    ImFontBaked* font = ImGui::GetFontBaked();
    auto computeXForOffset = [&](const char* ptr, uint32_t from, uint32_t to) -> float {
        float x = 0.0f;
        for (uint32_t i = from; i < to; ++i) {
            unsigned char ch = static_cast<unsigned char>(ptr[i]);
            x += font->GetCharAdvance(static_cast<ImWchar>(ch));
        }
        return x;
    };

    ImU32 selColor = IM_COL32(60, 100, 180, 128);

    // Clip text rendering to exclude scrollbar area
    dl->PushClipRect(ImVec2(startX, startY),
                     ImVec2(startX + width - SCROLLBAR_WIDTH, startY + height), true);

    // Render lines
    float cursorY = startY;
    uint64_t currentLine = m_anchorLine;
    uint32_t currentSubRow = m_anchorSubRow;
    ImU32 textColor = IM_COL32(220, 220, 220, 255);
    ImU32 gutterColor = IM_COL32(120, 120, 120, 255);

    char lineNumBuf[24];

    while (cursorY < startY + height && currentLine < lc) {
        LineData ld = getLineData(currentLine);

        if (m_wordWrap && textAreaWidth > 0.0f) {
            WrapCacheKey key{currentLine, textAreaWidth};
            auto cacheIt = m_wrapCache.find(key);
            WrapInfo wi;
            if (cacheIt != m_wrapCache.end()) {
                wi = cacheIt->second;
            } else {
                wi = computeWrapInfo(currentLine, textAreaWidth);
                if (m_wrapCache.size() >= WRAP_CACHE_MAX_SIZE) {
                    m_wrapCache.clear();
                }
                m_wrapCache[key] = wi;
            }

            for (uint32_t row = currentSubRow; row < wi.visualRowCount && cursorY < startY + height; ++row) {
                if (row == 0) {
                    snprintf(lineNumBuf, sizeof(lineNumBuf), "%llu", static_cast<unsigned long long>(currentLine + 1));
                    float numWidth = ImGui::CalcTextSize(lineNumBuf).x;
                    dl->AddText(ImVec2(startX + LINE_NUMBER_GUTTER_WIDTH - numWidth - 8.0f, cursorY),
                                gutterColor, lineNumBuf);
                }

                uint32_t rowStart = wi.rowStartOffsets[row];
                uint32_t rowEnd = (row + 1 < wi.visualRowCount) ? wi.rowStartOffsets[row + 1]
                                                                 : static_cast<uint32_t>(std::min(ld.length, static_cast<uint64_t>(MAX_DISPLAY_LINE_BYTES)));

                // Draw selection highlight
                if (m_selectionActive && ld.ptr) {
                    TextPosition rowBegin{currentLine, rowStart};
                    TextPosition rowEndPos{currentLine, rowEnd};
                    if (!(selEnd < rowBegin || rowEndPos < selStart)) {
                        uint32_t hlStart = (selStart.line == currentLine && selStart.byteOffset > rowStart) ? selStart.byteOffset : rowStart;
                        uint32_t hlEnd = (selEnd.line == currentLine && selEnd.byteOffset < rowEnd) ? selEnd.byteOffset : rowEnd;
                        if (hlEnd > hlStart) {
                            float x0 = textX + computeXForOffset(ld.ptr, rowStart, hlStart);
                            float x1 = textX + computeXForOffset(ld.ptr, rowStart, hlEnd);
                            dl->AddRectFilled(ImVec2(x0, cursorY), ImVec2(x1, cursorY + lineHeight), selColor);
                        }
                    }
                }

                if (ld.ptr && rowEnd > rowStart) {
                    dl->AddText(ImVec2(textX, cursorY), textColor,
                                ld.ptr + rowStart, ld.ptr + rowEnd);
                }

                cursorY += lineHeight;
            }
            currentSubRow = 0;
        } else {
            // No word wrap
            snprintf(lineNumBuf, sizeof(lineNumBuf), "%llu", static_cast<unsigned long long>(currentLine + 1));
            float numWidth = ImGui::CalcTextSize(lineNumBuf).x;
            dl->AddText(ImVec2(startX + LINE_NUMBER_GUTTER_WIDTH - numWidth - 8.0f, cursorY),
                        gutterColor, lineNumBuf);

            uint64_t displayLen = std::min(ld.length, MAX_DISPLAY_LINE_BYTES);

            // Draw selection highlight
            if (m_selectionActive && ld.ptr) {
                TextPosition rowBegin{currentLine, 0};
                TextPosition rowEndPos{currentLine, static_cast<uint32_t>(displayLen)};
                if (!(selEnd < rowBegin || rowEndPos < selStart)) {
                    uint32_t hlStart = (selStart.line == currentLine) ? selStart.byteOffset : 0;
                    uint32_t hlEnd = (selEnd.line == currentLine) ? selEnd.byteOffset : static_cast<uint32_t>(displayLen);
                    if (hlEnd > hlStart && hlStart < displayLen) {
                        if (hlEnd > displayLen) hlEnd = static_cast<uint32_t>(displayLen);
                        float x0 = textX + computeXForOffset(ld.ptr, 0, hlStart);
                        float x1 = textX + computeXForOffset(ld.ptr, 0, hlEnd);
                        dl->AddRectFilled(ImVec2(x0, cursorY), ImVec2(x1, cursorY + lineHeight), selColor);
                    }
                }
            }

            if (ld.ptr && ld.length > 0) {
                dl->AddText(ImVec2(textX, cursorY), textColor,
                            ld.ptr, ld.ptr + displayLen);
            }

            cursorY += lineHeight;
        }

        currentLine++;
    }

    dl->PopClipRect();

    // Scrollbar
    if (m_wordWrap && m_avgVisualRowsSampleLine != lc) {
        estimateAverageVisualRowsPerLine();
    }
    renderScrollbar(startX + width - SCROLLBAR_WIDTH, startY, height, static_cast<float>(lc));

    ImGui::PopID();
}
