#include "text_preview.h"
#include "browser_model.h"
#include "streaming_preview.h"
#include "imgui/imgui.h"
#include <cctype>

TextPreviewRenderer::TextPreviewRenderer() {
    // Configure the text editor for read-only preview
    m_editor.SetReadOnly(true);
    m_editor.SetPalette(TextEditor::GetDarkPalette());
    m_editor.SetShowWhitespaces(false);
}

bool TextPreviewRenderer::canHandle(const std::string& /*key*/) const {
    // TextPreviewRenderer is the fallback - it handles everything
    // that isn't handled by more specialized renderers
    return true;
}

void TextPreviewRenderer::render(const PreviewContext& ctx) {
    ImGui::Text("Preview: %s", ctx.filename.c_str());

    // Show streaming progress if active
    if (ctx.streamingPreview) {
        float progress = static_cast<float>(ctx.streamingPreview->bytesDownloaded()) /
                         static_cast<float>(ctx.streamingPreview->totalSourceBytes());
        ImGui::SameLine();
        ImGui::TextColored(ImVec4(0.5f, 0.5f, 1.0f, 1.0f), " (%.0f%%)", progress * 100.0f);
    }

    ImGui::Separator();

    const std::string& content = ctx.model.previewContent();
    if (content.empty()) {
        m_currentKey.clear();
        ImGui::TextColored(ImVec4(0.5f, 0.5f, 0.5f, 1.0f), "(empty file)");
    } else {
        // Update editor content if file changed
        std::string fullKey = ctx.bucket + "/" + ctx.key;
        if (m_currentKey != fullKey) {
            m_currentKey = fullKey;
            m_editor.SetText(content);
            updateEditorLanguage(ctx.filename);
            // Reset cursor and selection for new file
            m_editor.SetCursorPosition(TextEditor::Coordinates(0, 0));
            m_editor.SetSelection(TextEditor::Coordinates(0, 0), TextEditor::Coordinates(0, 0));
            // Note: SetText() internally sets mScrollToTop = true, so scroll resets automatically
        }

        // Render the editor (read-only, with syntax highlighting)
        m_editor.Render("##preview", ImVec2(ctx.width, ctx.height - ImGui::GetCursorPosY()), false);
    }
}

void TextPreviewRenderer::reset() {
    m_currentKey.clear();
}

void TextPreviewRenderer::updateEditorLanguage(const std::string& filename) {
    // Find the extension
    size_t dotPos = filename.rfind('.');
    if (dotPos == std::string::npos) {
        // No extension - disable colorization to avoid regex issues
        m_editor.SetColorizerEnable(false);
        return;
    }

    std::string ext = filename.substr(dotPos);
    // Convert to lowercase for comparison
    for (auto& c : ext) {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }

    // Set language based on extension
    // For files with proper language definitions, enable colorization
    if (ext == ".cpp" || ext == ".cxx" || ext == ".cc" || ext == ".hpp" || ext == ".hxx" || ext == ".h") {
        m_editor.SetColorizerEnable(true);
        m_editor.SetLanguageDefinition(TextEditor::LanguageDefinition::CPlusPlus());
    } else if (ext == ".c") {
        m_editor.SetColorizerEnable(true);
        m_editor.SetLanguageDefinition(TextEditor::LanguageDefinition::C());
    } else if (ext == ".lua") {
        m_editor.SetColorizerEnable(true);
        m_editor.SetLanguageDefinition(TextEditor::LanguageDefinition::Lua());
    } else if (ext == ".sql") {
        m_editor.SetColorizerEnable(true);
        m_editor.SetLanguageDefinition(TextEditor::LanguageDefinition::SQL());
    } else if (ext == ".hlsl" || ext == ".fx") {
        m_editor.SetColorizerEnable(true);
        m_editor.SetLanguageDefinition(TextEditor::LanguageDefinition::HLSL());
    } else if (ext == ".glsl" || ext == ".vert" || ext == ".frag" || ext == ".geom") {
        m_editor.SetColorizerEnable(true);
        m_editor.SetLanguageDefinition(TextEditor::LanguageDefinition::GLSL());
    } else if (ext == ".as") {
        m_editor.SetColorizerEnable(true);
        m_editor.SetLanguageDefinition(TextEditor::LanguageDefinition::AngelScript());
    } else {
        // For .txt, .md, .json, .jsonl, .html etc., disable colorization
        // to avoid regex complexity issues with large/complex content
        m_editor.SetColorizerEnable(false);
    }
}
