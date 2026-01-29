#pragma once

#include "preview_renderer.h"
#include "mmap_text_viewer.h"
#include <string>

class TextPreviewRenderer : public IPreviewRenderer {
public:
    bool canHandle(const std::string& key) const override;
    void render(const PreviewContext& ctx) override;
    void reset() override;

private:
    MmapTextViewer m_viewer;
    std::string m_currentKey;
};
