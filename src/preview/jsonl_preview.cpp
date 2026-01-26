#include "jsonl_preview.h"
#include "browser_model.h"
#include "streaming_preview.h"
#include "imgui/imgui.h"
#include "nlohmann/json.hpp"
#include <cctype>

bool JsonlPreviewRenderer::isJsonlFile(const std::string& key) {
    // Check for .jsonl, .ndjson, or .json extension (also handles compressed variants)
    // Note: .json files are included because they might be newline-delimited JSON
    // The renderer will validate the first line and fall back if it's not valid JSONL
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

bool JsonlPreviewRenderer::canHandle(const std::string& key) const {
    // Only handle JSONL files that have streaming preview available
    // The streaming preview check will be done at render time
    return isJsonlFile(key);
}

void JsonlPreviewRenderer::render(const PreviewContext& ctx) {
    auto* sp = ctx.streamingPreview;
    if (!sp) {
        // No streaming preview - shouldn't happen if canHandle was called correctly
        // but fall through gracefully
        ImGui::Text("Preview: %s", ctx.filename.c_str());
        ImGui::Separator();
        ImGui::TextColored(ImVec4(0.5f, 0.5f, 0.5f, 1.0f), "Loading...");
        return;
    }

    // Check if file changed - reset state
    std::string fullKey = ctx.bucket + "/" + ctx.key;
    bool fileChanged = (m_currentKey != fullKey);
    if (fileChanged) {
        m_currentKey = fullKey;
        m_currentLine = 0;
        m_formattedLineIndex = SIZE_MAX;
        m_formattedCache.clear();
        m_textFieldCache.clear();
        m_textFieldName.clear();
        m_validatedFirstLine = false;
        m_scrollGeneration++;  // Reset scroll by changing widget IDs
        // Don't clear m_fallbackKey here - it's used by wantsFallback() which is called before render()
    }

    // Validate first line once it's complete - if not valid JSON, trigger fallback
    if (!m_validatedFirstLine && sp->lineCount() > 0 && sp->isLineComplete(0)) {
        m_validatedFirstLine = true;
        std::string firstLine = sp->getLine(0);
        if (!isValidJsonLine(firstLine)) {
            // First line isn't valid JSON - mark this file for fallback
            m_fallbackKey = fullKey;
            // Return early - UI will switch to text renderer on next frame
            ImGui::TextColored(ImVec4(0.5f, 0.5f, 0.5f, 1.0f), "Not valid JSONL, switching to text view...");
            return;
        }
    }

    // If this file was marked for fallback, don't render (UI should have caught this)
    if (m_fallbackKey == fullKey) {
        return;
    }

    // Header with filename and progress
    ImGui::Text("Preview: %s", ctx.filename.c_str());
    if (!sp->isComplete()) {
        size_t totalBytes = sp->totalSourceBytes();
        size_t downloadedBytes = sp->bytesDownloaded();
        float progress = totalBytes > 0 ? static_cast<float>(downloadedBytes) / static_cast<float>(totalBytes) : 0.0f;
        ImGui::SameLine();
        ImGui::TextColored(ImVec4(0.5f, 0.5f, 1.0f, 1.0f), " (%.0f%%)", progress * 100.0f);
    }
    ImGui::Separator();

    // Navigation bar
    size_t lineCount = sp->lineCount();

    // Clamp current line to valid range
    if (m_currentLine >= lineCount && lineCount > 0) {
        m_currentLine = lineCount - 1;
    }

    // Navigation controls
    ImGui::BeginGroup();

    // Left arrow button
    if (ImGui::Button("<") || (ImGui::IsKeyPressed(ImGuiKey_LeftArrow) && !ImGui::GetIO().WantTextInput)) {
        navigateLine(-1, ctx);
    }
    ImGui::SameLine();

    // Line counter
    ImGui::Text("Line %zu / %zu", m_currentLine + 1, lineCount);
    ImGui::SameLine();

    // Right arrow button
    if (ImGui::Button(">") || (ImGui::IsKeyPressed(ImGuiKey_RightArrow) && !ImGui::GetIO().WantTextInput)) {
        navigateLine(1, ctx);
    }
    ImGui::SameLine();

    // Toggle raw/formatted
    if (ImGui::Checkbox("Raw", &m_rawMode)) {
        m_formattedLineIndex = SIZE_MAX;  // Force refresh
    }

    ImGui::EndGroup();
    ImGui::Separator();

    // Get current line content
    if (lineCount > 0) {
        bool lineComplete = sp->isLineComplete(m_currentLine);
        std::string lineContent = sp->getLine(m_currentLine);

        if (lineContent.empty()) {
            ImGui::TextColored(ImVec4(0.5f, 0.5f, 0.5f, 1.0f), "(empty line)");
        } else if (!lineComplete) {
            // Line is still being downloaded - invalidate cache so it re-formats when complete
            if (m_formattedLineIndex == m_currentLine) {
                m_formattedLineIndex = SIZE_MAX;
            }

            // Show partial content with indicator
            ImGui::TextColored(ImVec4(0.5f, 0.5f, 1.0f, 1.0f),
                "Line incomplete (%zu bytes loaded so far)...", lineContent.size());
            ImGui::Spacing();

            // Show partial content in raw form (don't try to parse incomplete JSON)
            ImVec2 availSize = ImGui::GetContentRegionAvail();
            // Show first/last part of partial content
            std::string displayContent;
            if (lineContent.size() > 1000) {
                displayContent = lineContent.substr(0, 500) + "\n...\n" +
                                 lineContent.substr(lineContent.size() - 500);
            } else {
                displayContent = lineContent;
            }
            // Style to match app background
            ImGui::PushStyleColor(ImGuiCol_FrameBg, ImGui::GetStyleColorVec4(ImGuiCol_WindowBg));
            ImGui::InputTextMultiline("##PartialContent",
                const_cast<char*>(displayContent.c_str()), displayContent.size() + 1,
                availSize, ImGuiInputTextFlags_ReadOnly);
            ImGui::PopStyleColor();
        } else if (m_rawMode) {
            // Raw mode - show unformatted line in scrollable region
            ImVec2 availSize = ImGui::GetContentRegionAvail();
            // Use scroll generation in ID to reset scroll when file/line changes
            char widgetId[64];
            snprintf(widgetId, sizeof(widgetId), "##RawContent_%d_%zu", m_scrollGeneration, m_currentLine);

            // Style to match app background
            ImGui::PushStyleColor(ImGuiCol_FrameBg, ImGui::GetStyleColorVec4(ImGuiCol_WindowBg));
            ImGui::InputTextMultiline(widgetId,
                const_cast<char*>(lineContent.c_str()), lineContent.size() + 1,
                availSize, ImGuiInputTextFlags_ReadOnly);
            ImGui::PopStyleColor();
        } else {
            // Formatted mode - show pretty-printed JSON (and extract text field)
            if (m_formattedLineIndex != m_currentLine) {
                updateCache(lineContent);
                m_formattedLineIndex = m_currentLine;
                m_scrollGeneration++;  // Reset scroll when line changes
            }

            ImVec2 availSize = ImGui::GetContentRegionAvail();

            // Use scroll generation in IDs to reset scroll when content changes
            char jsonWidgetId[64];
            snprintf(jsonWidgetId, sizeof(jsonWidgetId), "##FormattedContent_%d_%zu", m_scrollGeneration, m_currentLine);

            // Style to match app background
            ImGui::PushStyleColor(ImGuiCol_FrameBg, ImGui::GetStyleColorVec4(ImGuiCol_WindowBg));

            // If we have a text field, split the view horizontally
            if (!m_textFieldCache.empty()) {
                float halfHeight = availSize.y * 0.5f - 4;

                // Top pane: formatted JSON
                ImGui::InputTextMultiline(jsonWidgetId,
                    const_cast<char*>(m_formattedCache.c_str()), m_formattedCache.size() + 1,
                    ImVec2(availSize.x, halfHeight), ImGuiInputTextFlags_ReadOnly);

                // Bottom pane: text field
                char textWidgetId[64];
                snprintf(textWidgetId, sizeof(textWidgetId), "##TextFieldContent_%d_%zu", m_scrollGeneration, m_currentLine);
                ImGui::InputTextMultiline(textWidgetId,
                    const_cast<char*>(m_textFieldCache.c_str()), m_textFieldCache.size() + 1,
                    ImVec2(availSize.x, halfHeight), ImGuiInputTextFlags_ReadOnly);
            } else {
                // No text field - show JSON only
                ImGui::InputTextMultiline(jsonWidgetId,
                    const_cast<char*>(m_formattedCache.c_str()), m_formattedCache.size() + 1,
                    availSize, ImGuiInputTextFlags_ReadOnly);
            }

            ImGui::PopStyleColor();
        }
    } else {
        ImGui::TextColored(ImVec4(0.5f, 0.5f, 0.5f, 1.0f), "No lines loaded yet...");
    }
}

void JsonlPreviewRenderer::reset() {
    m_currentKey.clear();
    m_currentLine = 0;
    m_rawMode = false;
    m_formattedCache.clear();
    m_textFieldCache.clear();
    m_textFieldName.clear();
    m_formattedLineIndex = SIZE_MAX;
    // Don't clear m_fallbackKey - it needs to persist so wantsFallback() works
    // across renderer switches. It will naturally expire when a different file is selected.
    m_validatedFirstLine = false;
    m_scrollGeneration++;
}

bool JsonlPreviewRenderer::wantsFallback(const std::string& bucket, const std::string& key) const {
    std::string fullKey = bucket + "/" + key;
    return fullKey == m_fallbackKey;
}

bool JsonlPreviewRenderer::isValidJsonLine(const std::string& line) {
    if (line.empty()) return false;

    // Quick check: valid JSON objects/arrays start with { or [
    // Skip leading whitespace
    size_t start = 0;
    while (start < line.size() && std::isspace(static_cast<unsigned char>(line[start]))) {
        ++start;
    }
    if (start >= line.size()) return false;

    char firstChar = line[start];
    if (firstChar != '{' && firstChar != '[') {
        return false;  // JSONL lines should be objects or arrays
    }

    // Try to parse as JSON
    try {
        (void)nlohmann::json::parse(line);
        return true;
    } catch (...) {
        return false;
    }
}

void JsonlPreviewRenderer::navigateLine(int delta, const PreviewContext& ctx) {
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
}

void JsonlPreviewRenderer::updateCache(const std::string& rawJson) {
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

std::string JsonlPreviewRenderer::extractTextField(const std::string& json, std::string& outFieldName) {
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
