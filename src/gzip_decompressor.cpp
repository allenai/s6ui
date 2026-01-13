#include "gzip_decompressor.h"
#include "loguru.hpp"
#include <cstring>

GzipDecompressor::GzipDecompressor() {
    std::memset(&m_stream, 0, sizeof(m_stream));
}

GzipDecompressor::~GzipDecompressor() {
    if (m_initialized) {
        inflateEnd(&m_stream);
    }
}

bool GzipDecompressor::init() {
    if (m_initialized) {
        return true;
    }

    // Initialize for gzip format (window bits = 15 + 16 for gzip)
    // 15 = default window size, 16 = add gzip header detection
    int ret = inflateInit2(&m_stream, 15 + 16);
    if (ret != Z_OK) {
        m_hasError = true;
        m_errorMessage = "Failed to initialize zlib: " + std::to_string(ret);
        LOG_F(ERROR, "GzipDecompressor: %s", m_errorMessage.c_str());
        return false;
    }

    m_initialized = true;
    return true;
}

std::vector<char> GzipDecompressor::decompress(const void* data, size_t size) {
    std::vector<char> output;

    if (!m_initialized) {
        m_hasError = true;
        m_errorMessage = "Decompressor not initialized";
        return output;
    }

    if (m_hasError || m_complete) {
        return output;
    }

    if (size == 0) {
        return output;
    }

    // Set input
    m_stream.next_in = const_cast<Bytef*>(static_cast<const Bytef*>(data));
    m_stream.avail_in = static_cast<uInt>(size);

    // Output buffer
    std::vector<char> buffer(OUTPUT_BUFFER_SIZE);

    // Decompress all available input
    do {
        m_stream.next_out = reinterpret_cast<Bytef*>(buffer.data());
        m_stream.avail_out = static_cast<uInt>(buffer.size());

        int ret = inflate(&m_stream, Z_NO_FLUSH);

        if (ret == Z_STREAM_END) {
            // End of gzip stream
            size_t have = buffer.size() - m_stream.avail_out;
            output.insert(output.end(), buffer.begin(), buffer.begin() + have);
            m_complete = true;
            break;
        } else if (ret != Z_OK && ret != Z_BUF_ERROR) {
            // Error (Z_BUF_ERROR is not fatal - just means no progress possible)
            m_hasError = true;
            if (m_stream.msg) {
                m_errorMessage = m_stream.msg;
            } else {
                m_errorMessage = "zlib error: " + std::to_string(ret);
            }
            LOG_F(ERROR, "GzipDecompressor: %s", m_errorMessage.c_str());
            return output;
        }

        // Append decompressed data
        size_t have = buffer.size() - m_stream.avail_out;
        if (have > 0) {
            output.insert(output.end(), buffer.begin(), buffer.begin() + have);
        }
    } while (m_stream.avail_out == 0);  // Continue while output buffer was full

    return output;
}

void GzipDecompressor::reset() {
    if (m_initialized) {
        inflateEnd(&m_stream);
        m_initialized = false;
    }
    std::memset(&m_stream, 0, sizeof(m_stream));
    m_hasError = false;
    m_complete = false;
    m_errorMessage.clear();
}
