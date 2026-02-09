The goal of this project is to make a fast and responsive AWS S3 browser utility using Dear ImGUI, and as
few external dependencies as possible. Right now we support MacOSX, using DearImGUI on the glfw backend.

We don't allow any edit operations on AWS S3, but it should let you browse through a bucket, even ones
with millions of objects stored in the same prefix. The user should see things load as the requests come in,
and there should never be a lag while HTTP requests finish.

The idea is that the GUI will always be fast and responsive.

When you need to run the executable, just build it, and tell the user to run it, don't just launch the
main executable in the background.

## Build

```
make clean && make
```

## Architecture

### Core Components (src/)

| File | Purpose |
|------|---------|
| `main.mm` | macOS entry point using Metal + GLFW, sets up ImGui context and main loop |
| `browser_model.h/cpp` | Application state management - buckets, folders, file selection, preview |
| `browser_ui.h/cpp` | ImGui rendering - top bar, file browser pane, preview pane |
| `backend.h` | Abstract interface for async S3 operations |
| `events.h` | Event types for backend-to-model communication |
| `streaming_preview.h/cpp` | Streaming file download with decompression (gzip/zstd) |

### AWS Integration (src/aws/)

| File | Purpose |
|------|---------|
| `s3_backend.h/cpp` | IBackend implementation with worker threads and priority queues |
| `aws_credentials.h/cpp` | Load profiles from ~/.aws/credentials and ~/.aws/config |
| `aws_signer.h/cpp` | AWS SigV4 request signing, presigned URLs |

### Preview Renderers (src/preview/)

| File | Purpose |
|------|---------|
| `preview_renderer.h` | IPreviewRenderer interface |
| `text_preview.h/cpp` | Syntax-highlighted text editor using ImGuiColorTextEdit |
| `jsonl_preview.h/cpp` | Line-by-line JSONL viewer with pretty-printing |
| `image_preview.h/cpp` | Image display (PNG, JPG, etc.) using stb_image |

### Key Design Patterns

1. **Event-driven async model**: The `S3Backend` runs worker threads that queue `StateEvent`s, which `BrowserModel::processEvents()` polls each frame

2. **Priority queues**: High-priority (user actions) vs low-priority (prefetch) work items, with ability to boost prefetch to high priority

3. **Streaming with decompression**: `StreamingFilePreview` writes to a temp file, indexes newlines, and supports `GzipTransform`/`ZstdTransform` for transparent decompression

4. **Adaptive frame rate**: Main loop uses `glfwWaitEventsTimeout()` - 60fps when active, 2fps when idle to save CPU

5. **Prefetching**: Hovering over folders/files queues low-priority requests for instant navigation


