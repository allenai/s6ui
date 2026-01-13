#pragma once

#include <string>
#include <vector>
#include <zlib.h>

// Streaming gzip decompressor using zlib
// Thread-safe for single writer use (decompresses data incrementally)
class GzipDecompressor {
public:
    GzipDecompressor();
    ~GzipDecompressor();

    // Non-copyable
    GzipDecompressor(const GzipDecompressor&) = delete;
    GzipDecompressor& operator=(const GzipDecompressor&) = delete;

    // Initialize decompressor - must be called before decompress()
    // Returns true on success
    bool init();

    // Decompress a chunk of data
    // Returns decompressed data, or empty on error
    // Call repeatedly with streaming data
    std::vector<char> decompress(const void* data, size_t size);

    // Check if decompressor encountered an error
    bool hasError() const { return m_hasError; }

    // Get error message if hasError() is true
    const std::string& errorMessage() const { return m_errorMessage; }

    // Check if we've reached end of gzip stream
    bool isComplete() const { return m_complete; }

    // Reset decompressor for reuse
    void reset();

private:
    z_stream m_stream;
    bool m_initialized = false;
    bool m_hasError = false;
    bool m_complete = false;
    std::string m_errorMessage;

    static constexpr size_t OUTPUT_BUFFER_SIZE = 64 * 1024;  // 64KB output buffer
};
