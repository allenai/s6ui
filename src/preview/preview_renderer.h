#pragma once

#include <string>

class BrowserModel;
class StreamingFilePreview;

// Context passed to preview renderers each frame
struct PreviewContext {
    BrowserModel& model;
    const std::string& bucket;
    const std::string& key;
    const std::string& filename;
    StreamingFilePreview* streamingPreview;  // May be null
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
};
