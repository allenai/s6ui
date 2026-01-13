#pragma once

#include <string>
#include <vector>
#include <memory>
#include <mutex>
#include <zlib.h>
#include <zstd.h>

// Abstract interface for data transformation (decompression, etc.)
// Data flows: S3 chunks -> IStreamTransform -> temp file
class IStreamTransform {
public:
    virtual ~IStreamTransform() = default;

    // Transform input data, return transformed output
    // May buffer data internally if needed (e.g., for decompression blocks)
    virtual std::string transform(const char* data, size_t len) = 0;

    // Flush any remaining buffered data (call when stream is complete)
    virtual std::string flush() = 0;
};

// Pass-through transform (no transformation)
class PassThroughTransform : public IStreamTransform {
public:
    std::string transform(const char* data, size_t len) override {
        return std::string(data, len);
    }
    std::string flush() override { return ""; }
};

// Gzip decompression transform
class GzipTransform : public IStreamTransform {
public:
    GzipTransform();
    ~GzipTransform();

    // Non-copyable
    GzipTransform(const GzipTransform&) = delete;
    GzipTransform& operator=(const GzipTransform&) = delete;

    std::string transform(const char* data, size_t len) override;
    std::string flush() override;

    bool hasError() const { return m_error; }

private:
    z_stream m_zstream;
    bool m_initialized = false;
    bool m_error = false;
};

// Zstd decompression transform
class ZstdTransform : public IStreamTransform {
public:
    ZstdTransform();
    ~ZstdTransform();

    // Non-copyable
    ZstdTransform(const ZstdTransform&) = delete;
    ZstdTransform& operator=(const ZstdTransform&) = delete;

    std::string transform(const char* data, size_t len) override;
    std::string flush() override;

    bool hasError() const { return m_error; }

private:
    ZSTD_DStream* m_dstream = nullptr;
    bool m_error = false;
};

// Manages streaming download of a file to a temp file with newline indexing
class StreamingFilePreview {
public:
    // Initialize with the first chunk (typically 64KB preview)
    // totalFileSize is the size of the compressed/raw file on S3
    // Optionally pass a transform (e.g., GzipTransform) to decompress data
    StreamingFilePreview(const std::string& bucket, const std::string& key,
                         const std::string& initialData, size_t totalFileSize,
                         std::unique_ptr<IStreamTransform> transform = nullptr);
    ~StreamingFilePreview();

    // Non-copyable
    StreamingFilePreview(const StreamingFilePreview&) = delete;
    StreamingFilePreview& operator=(const StreamingFilePreview&) = delete;

    // Set a transform for the data stream (e.g., decompression)
    // Must be called before any chunks are appended
    void setTransform(std::unique_ptr<IStreamTransform> transform);

    // Append a new chunk from streaming download
    // offset is the byte offset in the source (S3) file
    void appendChunk(const std::string& data, size_t offset);

    // Mark the stream as complete (triggers flush of any buffered transform data)
    void finishStream();

    // Query methods (all thread-safe)
    size_t lineCount() const;              // Lines found so far
    size_t bytesDownloaded() const;        // Bytes received from S3
    size_t bytesWritten() const;           // Bytes written to temp file (after transform)
    size_t totalSourceBytes() const;       // Total file size on S3
    bool isComplete() const;               // Fully downloaded?
    size_t nextByteNeeded() const;         // For next range request

    // Get a specific line (0-indexed) - reads from temp file
    // Returns empty string if line doesn't exist yet
    std::string getLine(size_t lineIndex) const;

    // Get the raw content of a line (before any JSON formatting)
    std::string getRawLine(size_t lineIndex) const { return getLine(lineIndex); }

    // Get all content written so far (for non-line-based viewers)
    std::string getAllContent() const;

    // Check if a line is complete (has a terminating newline or is at end of completed file)
    bool isLineComplete(size_t lineIndex) const;

    // File identifiers
    const std::string& bucket() const { return m_bucket; }
    const std::string& key() const { return m_key; }

private:
    void writeToTempFile(const char* data, size_t len);
    void indexNewlines(const char* data, size_t len, size_t baseOffset);

    std::string m_bucket;
    std::string m_key;
    std::string m_tempFilePath;
    int m_fd = -1;  // File descriptor for temp file

    size_t m_totalSourceSize;           // Size of file on S3
    size_t m_bytesDownloaded = 0;       // Bytes received from S3
    size_t m_bytesWritten = 0;          // Bytes written to temp (after transform)
    bool m_complete = false;

    // Newline index: byte offset where each line starts in temp file
    // m_lineOffsets[0] = 0 (first line starts at byte 0)
    // m_lineOffsets[n] = byte offset where line n starts
    std::vector<size_t> m_lineOffsets;

    // Transform for data stream (default: pass-through)
    std::unique_ptr<IStreamTransform> m_transform;

    mutable std::mutex m_mutex;  // Protects all mutable state
};
