#pragma once

#include "preview_renderer.h"
#include <string>
#include <climits>

class JsonlPreviewRenderer : public IPreviewRenderer {
public:
    JsonlPreviewRenderer() = default;

    bool canHandle(const std::string& key) const override;
    void render(const PreviewContext& ctx) override;
    void reset() override;

    // Check if a file is a JSONL file (used externally for routing)
    static bool isJsonlFile(const std::string& key);

private:
    void navigateLine(int delta, const PreviewContext& ctx);
    static std::string formatJson(const std::string& json);

    std::string m_currentKey;        // bucket/key of loaded file
    size_t m_currentLine = 0;
    bool m_rawMode = false;
    std::string m_formattedCache;
    size_t m_formattedLineIndex = SIZE_MAX;
};
