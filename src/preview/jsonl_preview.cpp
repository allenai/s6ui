#include "jsonl_preview.h"
#include "browser_model.h"
#include "streaming_preview.h"
#include "imgui/imgui.h"
#include "nlohmann/json.hpp"
#include <cctype>

bool JsonlPreviewRenderer::isJsonlFile(const std::string& key) {
    // Check for .jsonl or .ndjson extension (also handles compressed variants)
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

    return ext == ".json" || ext == ".jsonl" || ext == ".ndjson";
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
            ImGui::BeginChild("PartialContent", availSize, false,
                ImGuiWindowFlags_HorizontalScrollbar);
            if (fileChanged) ImGui::SetScrollY(0);
            // Show first/last part of partial content
            if (lineContent.size() > 1000) {
                std::string preview = lineContent.substr(0, 500) + "\n...\n" +
                                      lineContent.substr(lineContent.size() - 500);
                ImGui::TextUnformatted(preview.c_str());
            } else {
                ImGui::TextUnformatted(lineContent.c_str());
            }
            ImGui::EndChild();
        } else if (m_rawMode) {
            // Raw mode - show unformatted line in scrollable region
            ImVec2 availSize = ImGui::GetContentRegionAvail();
            ImGui::BeginChild("RawContent", availSize, false,
                ImGuiWindowFlags_HorizontalScrollbar);
            if (fileChanged) ImGui::SetScrollY(0);
            ImGui::TextUnformatted(lineContent.c_str());
            ImGui::EndChild();
        } else {
            // Formatted mode - show pretty-printed JSON (and extract text field)
            if (m_formattedLineIndex != m_currentLine) {
                updateCache(lineContent);
                m_formattedLineIndex = m_currentLine;
            }

            ImVec2 availSize = ImGui::GetContentRegionAvail();

            // If we have a text field, split the view horizontally
            if (!m_textFieldCache.empty()) {
                float halfHeight = availSize.y * 0.5f - 4;

                // Top pane: formatted JSON
                ImGui::BeginChild("FormattedContent", ImVec2(availSize.x, halfHeight), true,
                    ImGuiWindowFlags_HorizontalScrollbar);
                if (fileChanged) ImGui::SetScrollY(0);
                ImGui::TextUnformatted(m_formattedCache.c_str());
                ImGui::EndChild();

                // Bottom pane: text field with word wrap
                ImGui::BeginChild("TextFieldContent", ImVec2(availSize.x, halfHeight), true);
                if (fileChanged) ImGui::SetScrollY(0);
                // Render text with word wrap
                ImGui::PushTextWrapPos(ImGui::GetContentRegionAvail().x);
                ImGui::TextUnformatted(m_textFieldCache.c_str());
                ImGui::PopTextWrapPos();
                ImGui::EndChild();
            } else {
                // No text field - show JSON only
                ImGui::BeginChild("FormattedContent", availSize, false,
                    ImGuiWindowFlags_HorizontalScrollbar);
                if (fileChanged) ImGui::SetScrollY(0);
                ImGui::TextUnformatted(m_formattedCache.c_str());
                ImGui::EndChild();
            }
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
