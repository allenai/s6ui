#include "streaming_preview.h"
#include "loguru.hpp"
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <cstdlib>
#include <cstring>

// ============================================================================
// GzipTransform implementation
// ============================================================================

GzipTransform::GzipTransform() {
    memset(&m_zstream, 0, sizeof(m_zstream));

    // 16 + MAX_WBITS tells zlib to detect gzip or zlib format automatically
    int ret = inflateInit2(&m_zstream, 16 + MAX_WBITS);
    if (ret != Z_OK) {
        LOG_F(ERROR, "GzipTransform: inflateInit2 failed with code %d", ret);
        m_error = true;
        return;
    }

    m_initialized = true;
    LOG_F(INFO, "GzipTransform: initialized successfully");
}

GzipTransform::~GzipTransform() {
    if (m_initialized) {
        inflateEnd(&m_zstream);
        m_initialized = false;
    }
}

std::string GzipTransform::transform(const char* data, size_t len) {
    if (!m_initialized || m_error || len == 0) {
        return "";
    }

    std::string output;
    output.reserve(len * 2);  // Guess: decompressed is ~2x compressed

    // Set up input
    m_zstream.next_in = reinterpret_cast<Bytef*>(const_cast<char*>(data));
    m_zstream.avail_in = static_cast<uInt>(len);

    // Decompress in chunks
    char outbuf[32768];  // 32KB output buffer

    do {
        m_zstream.next_out = reinterpret_cast<Bytef*>(outbuf);
        m_zstream.avail_out = sizeof(outbuf);

        int ret = inflate(&m_zstream, Z_NO_FLUSH);

        if (ret == Z_STREAM_ERROR) {
            LOG_F(ERROR, "GzipTransform: Z_STREAM_ERROR");
            m_error = true;
            return output;
        }

        switch (ret) {
            case Z_NEED_DICT:
            case Z_DATA_ERROR:
            case Z_MEM_ERROR:
                LOG_F(ERROR, "GzipTransform: inflate error %d: %s",
                      ret, m_zstream.msg ? m_zstream.msg : "unknown");
                m_error = true;
                return output;
        }

        size_t have = sizeof(outbuf) - m_zstream.avail_out;
        output.append(outbuf, have);

        // Z_STREAM_END means we've finished decompressing
        if (ret == Z_STREAM_END) {
            LOG_F(INFO, "GzipTransform: reached end of compressed stream");
            break;
        }

    } while (m_zstream.avail_out == 0);

    return output;
}

std::string GzipTransform::flush() {
    if (!m_initialized || m_error) {
        return "";
    }

    std::string output;
    char outbuf[32768];

    // Try to flush any remaining data
    m_zstream.next_in = nullptr;
    m_zstream.avail_in = 0;

    do {
        m_zstream.next_out = reinterpret_cast<Bytef*>(outbuf);
        m_zstream.avail_out = sizeof(outbuf);

        int ret = inflate(&m_zstream, Z_FINISH);

        if (ret == Z_STREAM_ERROR) {
            LOG_F(ERROR, "GzipTransform::flush: Z_STREAM_ERROR");
            m_error = true;
            break;
        }

        size_t have = sizeof(outbuf) - m_zstream.avail_out;
        if (have > 0) {
            output.append(outbuf, have);
        }

        if (ret == Z_STREAM_END) {
            break;
        }

        // Z_BUF_ERROR is OK here - just means no more output available
        if (ret == Z_BUF_ERROR) {
            break;
        }

    } while (m_zstream.avail_out == 0);

    LOG_F(INFO, "GzipTransform::flush: produced %zu bytes", output.size());
    return output;
}

// ============================================================================
// ZstdTransform implementation
// ============================================================================

ZstdTransform::ZstdTransform() {
    m_dstream = ZSTD_createDStream();
    if (!m_dstream) {
        LOG_F(ERROR, "ZstdTransform: ZSTD_createDStream failed");
        m_error = true;
        return;
    }

    size_t ret = ZSTD_initDStream(m_dstream);
    if (ZSTD_isError(ret)) {
        LOG_F(ERROR, "ZstdTransform: ZSTD_initDStream failed: %s", ZSTD_getErrorName(ret));
        m_error = true;
        return;
    }

    LOG_F(INFO, "ZstdTransform: initialized successfully");
}

ZstdTransform::~ZstdTransform() {
    if (m_dstream) {
        ZSTD_freeDStream(m_dstream);
        m_dstream = nullptr;
    }
}

std::string ZstdTransform::transform(const char* data, size_t len) {
    if (!m_dstream || m_error || len == 0) {
        return "";
    }

    std::string output;
    output.reserve(len * 4);  // Guess: decompressed is ~4x compressed

    ZSTD_inBuffer input = { data, len, 0 };

    // Decompress in chunks
    const size_t outBufSize = ZSTD_DStreamOutSize();
    std::vector<char> outbuf(outBufSize);

    while (input.pos < input.size) {
        ZSTD_outBuffer outBuffer = { outbuf.data(), outbuf.size(), 0 };

        size_t ret = ZSTD_decompressStream(m_dstream, &outBuffer, &input);

        if (ZSTD_isError(ret)) {
            LOG_F(ERROR, "ZstdTransform: ZSTD_decompressStream error: %s", ZSTD_getErrorName(ret));
            m_error = true;
            return output;
        }

        if (outBuffer.pos > 0) {
            output.append(outbuf.data(), outBuffer.pos);
        }

        // ret == 0 means end of frame, but there might be more frames
        // Continue processing input
    }

    return output;
}

std::string ZstdTransform::flush() {
    // For zstd streaming, all data should be flushed during transform()
    // This is called when stream completes, nothing special needed
    if (!m_dstream || m_error) {
        return "";
    }

    LOG_F(INFO, "ZstdTransform::flush: stream complete");
    return "";
}

// ============================================================================
// StreamingFilePreview implementation
// ============================================================================

StreamingFilePreview::StreamingFilePreview(
    const std::string& bucket,
    const std::string& key,
    const std::string& initialData,
    size_t totalFileSize,
    std::unique_ptr<IStreamTransform> transform)
    : m_bucket(bucket)
    , m_key(key)
    , m_totalSourceSize(totalFileSize)
    , m_transform(transform ? std::move(transform) : std::make_unique<PassThroughTransform>())
{
    // Create temp file
    const char* tmpdir = std::getenv("TMPDIR");
    if (!tmpdir) tmpdir = "/tmp";

    m_tempFilePath = std::string(tmpdir) + "/s6ui_preview_XXXXXX";

    // mkstemp modifies the string in place
    std::vector<char> pathBuf(m_tempFilePath.begin(), m_tempFilePath.end());
    pathBuf.push_back('\0');

    m_fd = mkstemp(pathBuf.data());
    if (m_fd < 0) {
        LOG_F(ERROR, "StreamingFilePreview: failed to create temp file: %s", strerror(errno));
        return;
    }

    m_tempFilePath = pathBuf.data();
    LOG_F(INFO, "StreamingFilePreview: created temp file %s for %s/%s (total=%zu bytes)",
          m_tempFilePath.c_str(), bucket.c_str(), key.c_str(), totalFileSize);

    // Initialize with first line offset
    m_lineOffsets.push_back(0);

    // Write initial data
    if (!initialData.empty()) {
        std::string transformed = m_transform->transform(initialData.data(), initialData.size());
        writeToTempFile(transformed.data(), transformed.size());
        m_bytesDownloaded = initialData.size();

        // Check if we got the whole file
        if (m_bytesDownloaded >= m_totalSourceSize) {
            finishStream();
        }
    }
}

StreamingFilePreview::~StreamingFilePreview() {
    if (m_fd >= 0) {
        close(m_fd);
        m_fd = -1;
    }

    // Delete temp file
    if (!m_tempFilePath.empty()) {
        LOG_F(INFO, "StreamingFilePreview: deleting temp file %s", m_tempFilePath.c_str());
        unlink(m_tempFilePath.c_str());
    }
}

void StreamingFilePreview::setTransform(std::unique_ptr<IStreamTransform> transform) {
    std::lock_guard<std::mutex> lock(m_mutex);
    if (m_bytesDownloaded > 0) {
        LOG_F(WARNING, "StreamingFilePreview: setTransform called after data received, ignoring");
        return;
    }
    m_transform = std::move(transform);
}

void StreamingFilePreview::appendChunk(const std::string& data, size_t offset) {
    std::lock_guard<std::mutex> lock(m_mutex);

    if (m_fd < 0) {
        LOG_F(WARNING, "StreamingFilePreview: appendChunk called but no temp file");
        return;
    }

    if (offset != m_bytesDownloaded) {
        LOG_F(WARNING, "StreamingFilePreview: chunk offset mismatch, expected %zu got %zu",
              m_bytesDownloaded, offset);
        // We could handle out-of-order chunks here, but for now assume sequential
        return;
    }

    // Transform the data (e.g., decompress)
    std::string transformed = m_transform->transform(data.data(), data.size());

    // Write to temp file
    writeToTempFile(transformed.data(), transformed.size());
    m_bytesDownloaded += data.size();

    LOG_F(1, "StreamingFilePreview: appended %zu bytes at offset %zu, total downloaded=%zu/%zu, lines=%zu",
          data.size(), offset, m_bytesDownloaded, m_totalSourceSize, m_lineOffsets.size());

    // Check if we're done
    if (m_bytesDownloaded >= m_totalSourceSize) {
        finishStream();
    }
}

void StreamingFilePreview::finishStream() {
    // Already holding lock from caller or constructor

    if (m_complete) return;

    // Flush any remaining transform data
    std::string remaining = m_transform->flush();
    if (!remaining.empty()) {
        writeToTempFile(remaining.data(), remaining.size());
    }

    m_complete = true;
    LOG_F(INFO, "StreamingFilePreview: stream complete, %zu bytes downloaded, %zu bytes written, %zu lines",
          m_bytesDownloaded, m_bytesWritten, m_lineOffsets.size());
}

void StreamingFilePreview::writeToTempFile(const char* data, size_t len) {
    // Caller must hold lock

    if (m_fd < 0 || len == 0) return;

    // Write data
    ssize_t written = write(m_fd, data, len);
    if (written < 0) {
        LOG_F(ERROR, "StreamingFilePreview: write failed: %s", strerror(errno));
        return;
    }

    // Index newlines in the data we just wrote
    size_t baseOffset = m_bytesWritten;
    indexNewlines(data, len, baseOffset);

    m_bytesWritten += static_cast<size_t>(written);
}

void StreamingFilePreview::indexNewlines(const char* data, size_t len, size_t baseOffset) {
    // Caller must hold lock

    for (size_t i = 0; i < len; ++i) {
        if (data[i] == '\n') {
            // The next line starts at the byte after this newline
            size_t nextLineOffset = baseOffset + i + 1;
            // Only add if there's more data (or will be more data)
            if (nextLineOffset < m_bytesWritten + len || !m_complete) {
                m_lineOffsets.push_back(nextLineOffset);
            }
        }
    }
}

size_t StreamingFilePreview::lineCount() const {
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_lineOffsets.size();
}

size_t StreamingFilePreview::bytesDownloaded() const {
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_bytesDownloaded;
}

size_t StreamingFilePreview::bytesWritten() const {
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_bytesWritten;
}

size_t StreamingFilePreview::totalSourceBytes() const {
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_totalSourceSize;
}

bool StreamingFilePreview::isComplete() const {
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_complete;
}

size_t StreamingFilePreview::nextByteNeeded() const {
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_bytesDownloaded;
}

std::string StreamingFilePreview::getLine(size_t lineIndex) const {
    std::lock_guard<std::mutex> lock(m_mutex);

    if (m_fd < 0) return "";
    if (lineIndex >= m_lineOffsets.size()) return "";

    size_t startOffset = m_lineOffsets[lineIndex];

    // Determine end offset
    size_t endOffset;
    if (lineIndex + 1 < m_lineOffsets.size()) {
        // End at start of next line (minus the newline character)
        endOffset = m_lineOffsets[lineIndex + 1] - 1;
    } else {
        // Last line - end at current write position
        endOffset = m_bytesWritten;
    }

    if (startOffset >= endOffset) return "";

    size_t lineLen = endOffset - startOffset;

    // Cap line length to prevent huge allocations
    constexpr size_t MAX_LINE_LEN = 10 * 1024 * 1024;  // 10MB max line
    if (lineLen > MAX_LINE_LEN) {
        lineLen = MAX_LINE_LEN;
    }

    // Read the line from temp file
    std::string line(lineLen, '\0');

    ssize_t bytesRead = pread(m_fd, line.data(), lineLen, static_cast<off_t>(startOffset));
    if (bytesRead < 0) {
        LOG_F(ERROR, "StreamingFilePreview: pread failed: %s", strerror(errno));
        return "";
    }

    line.resize(static_cast<size_t>(bytesRead));

    // Trim trailing newline/carriage return if present
    while (!line.empty() && (line.back() == '\n' || line.back() == '\r')) {
        line.pop_back();
    }

    return line;
}

bool StreamingFilePreview::isLineComplete(size_t lineIndex) const {
    std::lock_guard<std::mutex> lock(m_mutex);

    if (lineIndex >= m_lineOffsets.size()) return false;

    // Line is complete if:
    // 1. There's a next line (meaning we found a newline after this line)
    // 2. OR the file download is complete
    if (lineIndex + 1 < m_lineOffsets.size()) {
        return true;  // There's a next line, so this one is terminated
    }

    // This is the last known line - it's only complete if file is fully downloaded
    return m_complete;
}

std::string StreamingFilePreview::getAllContent() const {
    std::lock_guard<std::mutex> lock(m_mutex);

    if (m_fd < 0 || m_bytesWritten == 0) {
        return "";
    }

    std::string content(m_bytesWritten, '\0');

    ssize_t bytesRead = pread(m_fd, content.data(), m_bytesWritten, 0);
    if (bytesRead < 0) {
        LOG_F(ERROR, "StreamingFilePreview::getAllContent: pread failed: %s", strerror(errno));
        return "";
    }

    content.resize(static_cast<size_t>(bytesRead));
    return content;
}
