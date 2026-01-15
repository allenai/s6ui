#pragma once

#include "preview_renderer.h"
#include <string>
#include <cstdint>

// Forward declaration for platform-specific texture handle
// On Metal: this is id<MTLTexture> cast to void*
// On OpenGL: this is GLuint cast to void* via uintptr_t
using TextureHandle = void*;

class ImagePreviewRenderer : public IPreviewRenderer {
public:
    ImagePreviewRenderer();
    ~ImagePreviewRenderer();

    // Non-copyable
    ImagePreviewRenderer(const ImagePreviewRenderer&) = delete;
    ImagePreviewRenderer& operator=(const ImagePreviewRenderer&) = delete;

    bool canHandle(const std::string& key) const override;
    void render(const PreviewContext& ctx) override;
    void reset() override;

private:
    // Load image from raw bytes and create GPU texture
    bool loadImage(const unsigned char* data, size_t dataSize);

    // Platform-specific texture creation/destruction
    bool createTexture(unsigned char* pixels, int width, int height);
    void destroyTexture();

    // Check if file extension is a supported image format
    static bool isImageExtension(const std::string& ext);

    std::string m_currentKey;   // bucket/key of loaded image
    TextureHandle m_texture;    // Platform-specific texture handle
    int m_imageWidth;
    int m_imageHeight;
    std::string m_errorMessage; // Error message if loading failed
};
