#include "jsonl_preview.h"
#include "streaming_preview.h"
#include "imgui/imgui.h"
#include "nlohmann/json.hpp"
#include <cctype>

bool JsonlPreviewRenderer::isJsonlFile(const std::string& key) {
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
    return isJsonlFile(key);
}

void JsonlPreviewRenderer::render(const PreviewContext& ctx) {
    auto* sp = ctx.streamingPreview.get();
    if (!sp) {
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
        // Close all viewers BEFORE clearing strings they may reference
        m_rawViewer.close();
        m_jsonViewer.close();
        m_textViewer.close();
        m_jsonSP.reset();
        m_textSP.reset();
        m_incompleteSP.reset();
        // Now safe to clear strings
        m_formattedCache.clear();
        m_textFieldCache.clear();
        m_textFieldName.clear();
        m_validatedFirstLine = false;
        m_fallbackKey.clear();
        m_rawViewerKey.clear();
        m_jsonViewerLine = SIZE_MAX;
        m_textViewerLine = SIZE_MAX;
        m_rawViewerLine = SIZE_MAX;
    }

    // Validate first line once it's complete - if not valid JSON, trigger fallback
    if (!m_validatedFirstLine && sp->lineCount() > 0 && sp->isLineComplete(0)) {
        m_validatedFirstLine = true;
        std::string firstLine = sp->getLine(0);
        if (!isValidJsonLine(firstLine)) {
            m_fallbackKey = fullKey;
            ImGui::TextColored(ImVec4(0.5f, 0.5f, 0.5f, 1.0f), "Not valid JSONL, switching to text view...");
            return;
        }
    }

    // If this file was marked for fallback, don't render
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

    if (ImGui::Button("<") || (ImGui::IsKeyPressed(ImGuiKey_LeftArrow) && !ImGui::GetIO().WantTextInput)) {
        navigateLine(-1, ctx);
    }
    ImGui::SameLine();

    ImGui::Text("Line %zu / %zu", m_currentLine + 1, lineCount);
    ImGui::SameLine();

    if (ImGui::Button(">") || (ImGui::IsKeyPressed(ImGuiKey_RightArrow) && !ImGui::GetIO().WantTextInput)) {
        navigateLine(1, ctx);
    }
    ImGui::SameLine();

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
            // Line is still being downloaded - invalidate cache
            if (m_formattedLineIndex == m_currentLine) {
                m_formattedLineIndex = SIZE_MAX;
            }

            ImGui::TextColored(ImVec4(0.5f, 0.5f, 1.0f, 1.0f),
                "Line incomplete (%zu bytes loaded so far)...", lineContent.size());
            ImGui::Spacing();

            // Show partial content with MmapTextViewer
            std::string displayContent;
            if (lineContent.size() > 1000) {
                displayContent = lineContent.substr(0, 500) + "\n...\n" +
                                 lineContent.substr(lineContent.size() - 500);
            } else {
                displayContent = lineContent;
            }

            m_incompleteSP = std::make_shared<StreamingFilePreview>("", "", displayContent, displayContent.size());
            MmapTextViewer incompleteViewer;
            incompleteViewer.open(m_incompleteSP);

            ImVec2 availSize = ImGui::GetContentRegionAvail();
            if (availSize.y > 0.0f) {
                incompleteViewer.render(availSize.x, availSize.y);
            }
        } else if (m_rawMode) {
            // Clean up incomplete state if we transitioned to complete
            m_incompleteSP.reset();

            // Raw mode - open the main streaming temp file and scroll to current line
            if (m_rawViewerKey != fullKey) {
                m_rawViewer.close();
                m_rawViewer.open(ctx.streamingPreview);
                m_rawViewerKey = fullKey;
            }

            // Refresh mapping if source has new data (no-op when size unchanged)
            m_rawViewer.refresh();

            // Scroll to current line if it changed
            if (m_rawViewerLine != m_currentLine) {
                m_rawViewer.scrollToLine(static_cast<uint64_t>(m_currentLine));
                m_rawViewerLine = m_currentLine;
            }

            ImVec2 availSize = ImGui::GetContentRegionAvail();
            if (availSize.y > 0.0f) {
                m_rawViewer.render(availSize.x, availSize.y);
            }
        } else {
            // Clean up incomplete state if we transitioned to complete
            m_incompleteSP.reset();

            // Formatted mode - show pretty-printed JSON (and extract text field)
            if (m_formattedLineIndex != m_currentLine) {
                updateCache(lineContent);
                m_formattedLineIndex = m_currentLine;

                // Create StreamingFilePreview for formatted JSON
                m_jsonSP = std::make_shared<StreamingFilePreview>("", "", m_formattedCache, m_formattedCache.size());
                m_jsonViewer.close();
                m_jsonViewer.open(m_jsonSP);
                m_jsonViewerLine = m_currentLine;

                // Create StreamingFilePreview for text field if present
                if (!m_textFieldCache.empty()) {
                    m_textSP = std::make_shared<StreamingFilePreview>("", "", m_textFieldCache, m_textFieldCache.size());
                    m_textViewer.close();
                    m_textViewer.open(m_textSP);
                    m_textViewer.setWordWrap(true);
                    m_textViewerLine = m_currentLine;
                } else {
                    m_textViewer.close();
                    m_textSP.reset();
                    m_textViewerLine = SIZE_MAX;
                }
            }

            ImVec2 availSize = ImGui::GetContentRegionAvail();

            if (!m_textFieldCache.empty()) {
                float halfHeight = availSize.y * 0.5f - 4;

                // Top pane: formatted JSON
                if (halfHeight > 0.0f) {
                    m_jsonViewer.render(availSize.x, halfHeight);
                }

                ImGui::Spacing();

                // Bottom pane: text field
                if (halfHeight > 0.0f) {
                    m_textViewer.render(availSize.x, halfHeight);
                }
            } else {
                // No text field - show JSON only
                if (availSize.y > 0.0f) {
                    m_jsonViewer.render(availSize.x, availSize.y);
                }
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
    m_validatedFirstLine = false;

    m_rawViewer.close();
    m_jsonViewer.close();
    m_textViewer.close();
    m_jsonSP.reset();
    m_textSP.reset();
    m_incompleteSP.reset();
    m_rawViewerKey.clear();
    m_jsonViewerLine = SIZE_MAX;
    m_textViewerLine = SIZE_MAX;
    m_rawViewerLine = SIZE_MAX;
}

bool JsonlPreviewRenderer::wantsFallback(const std::string& bucket, const std::string& key) const {
    std::string fullKey = bucket + "/" + key;
    return fullKey == m_fallbackKey;
}

bool JsonlPreviewRenderer::isValidJsonLine(const std::string& line) {
    if (line.empty()) return false;

    size_t start = 0;
    while (start < line.size() && std::isspace(static_cast<unsigned char>(line[start]))) {
        ++start;
    }
    if (start >= line.size()) return false;

    char firstChar = line[start];
    if (firstChar != '{' && firstChar != '[') {
        return false;
    }

    try {
        (void)nlohmann::json::parse(line);
        return true;
    } catch (...) {
        return false;
    }
}

void JsonlPreviewRenderer::navigateLine(int delta, const PreviewContext& ctx) {
    auto* sp = ctx.streamingPreview.get();
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
        m_formattedCache = parsed.dump(2);
        m_textFieldCache = extractTextField(rawJson, m_textFieldName);
    } catch (const nlohmann::json::parse_error& e) {
        m_formattedCache = std::string("(Invalid JSON: ") + e.what() + ")\n\n" + rawJson;
        m_textFieldCache.clear();
        m_textFieldName.clear();
    } catch (const std::exception& e) {
        m_formattedCache = std::string("(Error: ") + e.what() + ")";
        m_textFieldCache.clear();
        m_textFieldName.clear();
    } catch (...) {
        m_formattedCache = "(Error formatting JSON)";
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

        if (parsed.contains("text") && parsed["text"].is_string()) {
            outFieldName = "text";
            return parsed["text"].get<std::string>();
        }

    } catch (...) {
        // Ignore parse errors
    }

    return "";
}
