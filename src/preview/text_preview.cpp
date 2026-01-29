#include "text_preview.h"
#include "browser_model.h"
#include "streaming_preview.h"
#include "imgui/imgui.h"

bool TextPreviewRenderer::canHandle(const std::string& /*key*/) const {
    // TextPreviewRenderer is the fallback - it handles everything
    // that isn't handled by more specialized renderers
    return true;
}

void TextPreviewRenderer::render(const PreviewContext& ctx) {
    ImGui::Text("Preview: %s", ctx.filename.c_str());

    // Show streaming progress if active
    if (ctx.streamingPreview && !ctx.streamingPreview->isComplete()) {
        float progress = static_cast<float>(ctx.streamingPreview->bytesDownloaded()) /
                         static_cast<float>(ctx.streamingPreview->totalSourceBytes());
        ImGui::SameLine();
        ImGui::TextColored(ImVec4(0.5f, 0.5f, 1.0f, 1.0f), " (%.0f%%)", progress * 100.0f);
    }

    ImGui::Separator();

    if (!ctx.streamingPreview) {
        ImGui::TextColored(ImVec4(0.5f, 0.5f, 0.5f, 1.0f), "Loading...");
        return;
    }

    // Open viewer if file changed
    std::string fullKey = ctx.bucket + "/" + ctx.key;
    if (m_currentKey != fullKey) {
        m_viewer.close();
        m_viewer.open(ctx.streamingPreview);
        m_currentKey = fullKey;
    }

    // Refresh mapping if source has new data (no-op when size unchanged)
    m_viewer.refresh();

    // Render the viewer
    float availWidth = ctx.width;
    float availHeight = ctx.height - ImGui::GetCursorPosY();
    if (availHeight > 0.0f) {
        m_viewer.render(availWidth, availHeight);
    }
}

void TextPreviewRenderer::reset() {
    m_viewer.close();
    m_currentKey.clear();
}
