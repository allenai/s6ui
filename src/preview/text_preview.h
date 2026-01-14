#pragma once

#include "preview_renderer.h"
#include "imguicolortextedit/TextEditor.h"
#include <string>

class TextPreviewRenderer : public IPreviewRenderer {
public:
    TextPreviewRenderer();

    bool canHandle(const std::string& key) const override;
    void render(const PreviewContext& ctx) override;
    void reset() override;

private:
    void updateEditorLanguage(const std::string& filename);

    TextEditor m_editor;
    std::string m_currentKey;  // bucket/key of loaded file
};
