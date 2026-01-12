#include "streaming_file.h"
#include "loguru.hpp"

#include <vector>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <cstring>

StreamingFile::StreamingFile() = default;

StreamingFile::~StreamingFile() {
    close();
}

StreamingFile::StreamingFile(StreamingFile&& other) noexcept
    : m_fd(other.m_fd)
    , m_path(std::move(other.m_path))
    , m_mapped_data(other.m_mapped_data)
    , m_mapped_size(other.m_mapped_size)
    , m_size(other.m_size.load(std::memory_order_relaxed))
    , m_file_capacity(other.m_file_capacity)
{
    other.m_fd = -1;
    other.m_mapped_data = nullptr;
    other.m_mapped_size = 0;
    other.m_size.store(0, std::memory_order_relaxed);
    other.m_file_capacity = 0;
}

StreamingFile& StreamingFile::operator=(StreamingFile&& other) noexcept {
    if (this != &other) {
        close();
        m_fd = other.m_fd;
        m_path = std::move(other.m_path);
        m_mapped_data = other.m_mapped_data;
        m_mapped_size = other.m_mapped_size;
        m_size.store(other.m_size.load(std::memory_order_relaxed), std::memory_order_relaxed);
        m_file_capacity = other.m_file_capacity;

        other.m_fd = -1;
        other.m_mapped_data = nullptr;
        other.m_mapped_size = 0;
        other.m_size.store(0, std::memory_order_relaxed);
        other.m_file_capacity = 0;
    }
    return *this;
}

bool StreamingFile::create() {
    // Create temp file in system temp directory
    const char* tmpdir = std::getenv("TMPDIR");
    if (!tmpdir) tmpdir = "/tmp";

    std::string path_template = std::string(tmpdir) + "/s3v_preview_XXXXXX";

    // Need a mutable buffer for mkstemp
    std::vector<char> path_buf(path_template.begin(), path_template.end());
    path_buf.push_back('\0');

    m_fd = mkstemp(path_buf.data());
    if (m_fd < 0) {
        LOG_F(ERROR, "Failed to create temp file: %s", strerror(errno));
        return false;
    }

    m_path = path_buf.data();

    // Unlink immediately - file stays open but is deleted when closed
    // This ensures cleanup even on crash
    if (unlink(m_path.c_str()) != 0) {
        LOG_F(WARNING, "Failed to unlink temp file %s: %s", m_path.c_str(), strerror(errno));
        // Continue anyway, file will exist until manually deleted
    }

    // Pre-allocate initial capacity
    if (ftruncate(m_fd, INITIAL_CAPACITY) != 0) {
        LOG_F(ERROR, "Failed to allocate temp file: %s", strerror(errno));
        ::close(m_fd);
        m_fd = -1;
        return false;
    }
    m_file_capacity = INITIAL_CAPACITY;

    LOG_F(INFO, "Created streaming temp file: %s", m_path.c_str());
    return true;
}

ssize_t StreamingFile::append(const void* data, size_t size) {
    if (m_fd < 0 || !data || size == 0) {
        return -1;
    }

    size_t current_size = m_size.load(std::memory_order_relaxed);
    size_t needed = current_size + size;

    // Grow file if needed
    if (needed > m_file_capacity) {
        // Grow in chunks to avoid frequent reallocation
        size_t new_capacity = ((needed / GROWTH_CHUNK) + 1) * GROWTH_CHUNK;
        if (ftruncate(m_fd, new_capacity) != 0) {
            LOG_F(ERROR, "Failed to grow temp file to %zu bytes: %s",
                  new_capacity, strerror(errno));
            return -1;
        }
        m_file_capacity = new_capacity;
    }

    // Write data at current offset
    ssize_t written = pwrite(m_fd, data, size, current_size);
    if (written < 0) {
        LOG_F(ERROR, "Failed to write to temp file: %s", strerror(errno));
        return -1;
    }

    // Update size atomically
    m_size.store(current_size + written, std::memory_order_release);
    return written;
}

bool StreamingFile::remap() {
    std::lock_guard<std::mutex> lock(m_remap_mutex);

    size_t current_size = m_size.load(std::memory_order_acquire);

    // Nothing to map
    if (current_size == 0) {
        return true;
    }

    // Already mapped enough
    if (m_mapped_data && current_size <= m_mapped_size) {
        return true;
    }

    // Unmap old mapping
    if (m_mapped_data) {
        munmap(m_mapped_data, m_mapped_size);
        m_mapped_data = nullptr;
        m_mapped_size = 0;
    }

    // Round up to page size for mapping
    long page_size = sysconf(_SC_PAGESIZE);
    if (page_size <= 0) page_size = 4096;

    size_t map_size = ((current_size + page_size - 1) / page_size) * page_size;

    // Ensure file is large enough for the mapping
    if (map_size > m_file_capacity) {
        if (ftruncate(m_fd, map_size) != 0) {
            LOG_F(ERROR, "Failed to extend file for mapping: %s", strerror(errno));
            return false;
        }
        m_file_capacity = map_size;
    }

    void* addr = mmap(nullptr, map_size, PROT_READ, MAP_PRIVATE, m_fd, 0);
    if (addr == MAP_FAILED) {
        LOG_F(ERROR, "Failed to mmap temp file: %s", strerror(errno));
        return false;
    }

    m_mapped_data = static_cast<char*>(addr);
    m_mapped_size = map_size;

    return true;
}

void StreamingFile::close() {
    // Unmap first
    if (m_mapped_data) {
        munmap(m_mapped_data, m_mapped_size);
        m_mapped_data = nullptr;
        m_mapped_size = 0;
    }

    // Close file descriptor
    if (m_fd >= 0) {
        ::close(m_fd);
        m_fd = -1;
    }

    m_path.clear();
    m_size.store(0, std::memory_order_relaxed);
    m_file_capacity = 0;
}
