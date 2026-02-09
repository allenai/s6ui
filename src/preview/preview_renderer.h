#pragma once

#include <memory>
#include <string>
#include <cstdint>

class StreamingFilePreview;

// Context passed to preview renderers each frame
struct PreviewContext {
    const std::string& bucket;
    const std::string& key;
    const std::string& filename;
    std::shared_ptr<StreamingFilePreview> streamingPreview;  // May be null
    const std::string& previewContent;   // raw content for image decoder
    int64_t selectedFileSize;            // total file size
    float width;
    float height;
};

// Abstract interface for preview renderers
class IPreviewRenderer {
public:
    virtual ~IPreviewRenderer() = default;

    // Check if this renderer can handle the given file
    virtual bool canHandle(const std::string& key) const = 0;

    // Render the preview content
    // Called each frame while this file is selected
    virtual void render(const PreviewContext& ctx) = 0;

    // Reset internal state (called when file deselected or renderer changes)
    virtual void reset() = 0;

    // Check if this renderer wants to fall back to the next renderer for this file
    // This is called after canHandle() returns true, to allow content-based fallback
    virtual bool wantsFallback(const std::string& bucket, const std::string& key) const {
        (void)bucket; (void)key;
        return false;
    }
};
