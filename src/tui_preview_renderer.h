#pragma once

#include <ncurses.h>
#include <string>

// Forward declarations
class StreamingFilePreview;

// Context passed to TUI preview renderers
struct TUIPreviewContext {
    WINDOW* window;              // The window to render to
    int availHeight;             // Available content height (excluding borders)
    int availWidth;              // Available content width (excluding borders)
    std::string bucket;          // S3 bucket name
    std::string key;             // S3 object key
    std::string filename;        // Display filename
    StreamingFilePreview* streamingPreview;  // Streaming preview (may be null)
};

// Abstract base class for TUI preview renderers
// Renderers implement specific viewing modes (text, JSONL, etc.)
class TUIPreviewRenderer {
public:
    virtual ~TUIPreviewRenderer() = default;

    // Check if this renderer can handle the given file
    virtual bool canHandle(const std::string& key) const = 0;

    // Render the preview content
    // Returns true if render was successful
    virtual bool render(const TUIPreviewContext& ctx) = 0;

    // Handle keyboard input when this renderer is active
    // Returns true if input was consumed, false to pass to default handler
    virtual bool handleInput(int ch, const TUIPreviewContext& ctx) = 0;

    // Reset internal state (called when switching files or renderers)
    virtual void reset() = 0;

    // Get current scroll offset (for scrollbar rendering)
    virtual int scrollOffset() const { return 0; }

    // Get total scrollable lines (for scrollbar rendering)
    virtual int totalLines() const { return 0; }
};
