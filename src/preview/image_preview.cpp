#include "image_preview.h"
#include "streaming_preview.h"
#include "imgui/imgui.h"
#include "stb/stb_image.h"
#include "loguru.hpp"
#include <algorithm>
#include <cctype>

// Platform-specific texture creation functions (implemented in image_texture_*.cpp/.mm)
extern "C" bool CreateGPUTexture(unsigned char* pixels, int width, int height, void** outTexture);
extern "C" void DestroyGPUTexture(void* texture);

ImagePreviewRenderer::ImagePreviewRenderer()
    : m_texture(nullptr)
    , m_imageWidth(0)
    , m_imageHeight(0)
{
}

ImagePreviewRenderer::~ImagePreviewRenderer() {
    destroyTexture();
}

bool ImagePreviewRenderer::isImageExtension(const std::string& ext) {
    static const char* imageExtensions[] = {
        ".png", ".jpg", ".jpeg", ".gif", ".bmp",
        ".psd", ".tga", ".hdr", ".pic", ".pnm", ".pgm", ".ppm"
    };

    std::string lowerExt = ext;
    for (char& c : lowerExt) {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }

    for (const char* imgExt : imageExtensions) {
        if (lowerExt == imgExt) {
            return true;
        }
    }
    return false;
}

bool ImagePreviewRenderer::canHandle(const std::string& key) const {
    size_t dotPos = key.rfind('.');
    if (dotPos == std::string::npos) {
        return false;
    }
    return isImageExtension(key.substr(dotPos));
}

void ImagePreviewRenderer::render(const PreviewContext& ctx) {
    ImGui::Text("Preview: %s", ctx.filename.c_str());

    // Show streaming progress if active
    if (ctx.streamingPreview) {
        float progress = static_cast<float>(ctx.streamingPreview->bytesDownloaded()) /
                         static_cast<float>(ctx.streamingPreview->totalSourceBytes());
        ImGui::SameLine();
        ImGui::TextColored(ImVec4(0.5f, 0.5f, 1.0f, 1.0f), " (%.0f%%)", progress * 100.0f);
    }

    ImGui::Separator();

    // Check if we need to load a new image
    std::string fullKey = ctx.bucket + "/" + ctx.key;
    const std::string& content = ctx.previewContent;

    if (content.empty()) {
        ImGui::TextColored(ImVec4(0.5f, 0.5f, 0.5f, 1.0f), "(empty file)");
        return;
    }

    // For images, we need the complete file data before we can decode
    // Check if we're still downloading
    int64_t totalSize = ctx.selectedFileSize;
    bool downloadComplete = true;

    if (ctx.streamingPreview) {
        // Streaming is active - check if complete
        downloadComplete = ctx.streamingPreview->isComplete();
    } else if (totalSize > 0 && static_cast<int64_t>(content.size()) < totalSize) {
        // No streaming preview yet, but content is smaller than file size
        // This means we only have the initial 64KB preview
        downloadComplete = false;
    }

    if (!downloadComplete) {
        // Show download progress instead of trying to decode partial data
        float progress = 0.0f;
        if (ctx.streamingPreview) {
            progress = static_cast<float>(ctx.streamingPreview->bytesDownloaded()) /
                       static_cast<float>(ctx.streamingPreview->totalSourceBytes());
        } else if (totalSize > 0) {
            progress = static_cast<float>(content.size()) / static_cast<float>(totalSize);
        }

        ImGui::TextColored(ImVec4(0.5f, 0.5f, 1.0f, 1.0f),
            "Downloading image... %.0f%%", progress * 100.0f);

        // Show a progress bar
        ImGui::ProgressBar(progress, ImVec2(-1, 0));
        return;
    }

    // Load image if key changed or not loaded yet
    if (m_currentKey != fullKey || m_texture == nullptr) {
        m_currentKey = fullKey;
        m_errorMessage.clear();

        if (!loadImage(reinterpret_cast<const unsigned char*>(content.data()), content.size())) {
            LOG_F(WARNING, "Failed to load image: %s", m_errorMessage.c_str());
        }
    }

    // Show error if loading failed
    if (!m_errorMessage.empty()) {
        ImGui::TextColored(ImVec4(1.0f, 0.3f, 0.3f, 1.0f), "Error: %s", m_errorMessage.c_str());
        return;
    }

    if (m_texture == nullptr) {
        ImGui::TextColored(ImVec4(0.7f, 0.7f, 0.7f, 1.0f), "Loading image...");
        return;
    }

    // Calculate display size maintaining aspect ratio
    ImVec2 availSize = ImGui::GetContentRegionAvail();
    float imageAspect = static_cast<float>(m_imageWidth) / static_cast<float>(m_imageHeight);
    float availAspect = availSize.x / availSize.y;

    ImVec2 displaySize;
    if (imageAspect > availAspect) {
        // Image is wider than available space - fit to width
        displaySize.x = availSize.x;
        displaySize.y = availSize.x / imageAspect;
    } else {
        // Image is taller than available space - fit to height
        displaySize.y = availSize.y;
        displaySize.x = availSize.y * imageAspect;
    }

    // Don't scale up small images beyond their native size
    if (displaySize.x > m_imageWidth) {
        displaySize.x = static_cast<float>(m_imageWidth);
        displaySize.y = static_cast<float>(m_imageHeight);
    }

    // Center the image horizontally
    float offsetX = (availSize.x - displaySize.x) * 0.5f;
    if (offsetX > 0) {
        ImGui::SetCursorPosX(ImGui::GetCursorPosX() + offsetX);
    }

    // Display the image
    ImGui::Image(reinterpret_cast<ImTextureID>(m_texture), displaySize);

    // Show image info below
    ImGui::Spacing();
    ImGui::TextColored(ImVec4(0.5f, 0.5f, 0.5f, 1.0f),
        "%dx%d pixels", m_imageWidth, m_imageHeight);
}

void ImagePreviewRenderer::reset() {
    destroyTexture();
    m_currentKey.clear();
    m_errorMessage.clear();
    m_imageWidth = 0;
    m_imageHeight = 0;
}

bool ImagePreviewRenderer::loadImage(const unsigned char* data, size_t dataSize) {
    // First destroy any existing texture
    destroyTexture();

    // Decode image using stb_image
    int width, height, channels;
    unsigned char* pixels = stbi_load_from_memory(
        data,
        static_cast<int>(dataSize),
        &width,
        &height,
        &channels,
        4  // Force RGBA
    );

    if (pixels == nullptr) {
        m_errorMessage = stbi_failure_reason() ? stbi_failure_reason() : "Unknown error decoding image";
        return false;
    }

    LOG_F(INFO, "Decoded image: %dx%d, %d channels", width, height, channels);

    // Create GPU texture
    bool success = createTexture(pixels, width, height);

    // Free the CPU pixel data
    stbi_image_free(pixels);

    if (success) {
        m_imageWidth = width;
        m_imageHeight = height;
    }

    return success;
}

bool ImagePreviewRenderer::createTexture(unsigned char* pixels, int width, int height) {
    return CreateGPUTexture(pixels, width, height, &m_texture);
}

void ImagePreviewRenderer::destroyTexture() {
    if (m_texture != nullptr) {
        DestroyGPUTexture(m_texture);
        m_texture = nullptr;
    }
}
