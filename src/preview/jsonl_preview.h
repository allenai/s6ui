#pragma once

#include "preview_renderer.h"
#include <string>
#include <climits>
#include <cstdint>

class JsonlPreviewRenderer : public IPreviewRenderer {
public:
    JsonlPreviewRenderer() = default;

    bool canHandle(const std::string& key) const override;
    void render(const PreviewContext& ctx) override;
    void reset() override;
    bool wantsFallback(const std::string& bucket, const std::string& key) const override;

    // Check if a file could be a JSONL file based on extension (used externally for routing)
    static bool isJsonlFile(const std::string& key);

private:
    void navigateLine(int delta, const PreviewContext& ctx);
    void updateCache(const std::string& rawJson);
    static std::string extractTextField(const std::string& json, std::string& outFieldName);

    // Try to parse a line as JSON, returns true if valid
    static bool isValidJsonLine(const std::string& line);

    std::string m_currentKey;        // bucket/key of loaded file
    size_t m_currentLine = 0;
    bool m_rawMode = false;
    std::string m_formattedCache;    // Pretty-printed JSON
    std::string m_textFieldCache;    // Extracted text field content
    std::string m_textFieldName;     // Name of the extracted text field
    size_t m_formattedLineIndex = SIZE_MAX;

    // Fallback tracking - stores bucket/key of files that failed JSON parsing
    std::string m_fallbackKey;
    bool m_validatedFirstLine = false;  // True once we've checked the first line
};
