#pragma once

#include <string>
#include <cstddef>
#include <atomic>
#include <mutex>

// RAII wrapper for a memory-mapped streaming file
// Thread-safe for concurrent write (backend) and read (UI) access
class StreamingFile {
public:
    StreamingFile();
    ~StreamingFile();

    // Non-copyable, movable
    StreamingFile(const StreamingFile&) = delete;
    StreamingFile& operator=(const StreamingFile&) = delete;
    StreamingFile(StreamingFile&& other) noexcept;
    StreamingFile& operator=(StreamingFile&& other) noexcept;

    // Create a new temp file for streaming
    // Returns true on success, false on failure
    bool create();

    // Append data to the file (called from backend worker thread)
    // Returns bytes written, or -1 on error
    ssize_t append(const void* data, size_t size);

    // Get current file size (bytes written so far)
    // Note: This may be larger than mappedSize() if remap() hasn't been called recently
    size_t size() const { return m_size.load(std::memory_order_acquire); }

    // Get size of data that's safely readable via data()
    // This is the amount of data covered by the current mmap - never larger than size()
    size_t mappedSize() const { return std::min(m_mapped_size, m_size.load(std::memory_order_acquire)); }

    // Get read-only pointer to mapped data
    // Safe to call from UI thread while backend is still appending
    // Returns nullptr if not mapped or empty
    // IMPORTANT: Only read up to mappedSize() bytes, not size() bytes!
    const char* data() const { return m_mapped_data; }

    // Update the memory mapping to include newly written data
    // Must be called after append() to make new data visible to readers
    // Thread-safe, can be called from backend after writing
    bool remap();

    // Get path to temp file (for debugging/logging)
    const std::string& path() const { return m_path; }

    // Check if file is valid and open
    bool isValid() const { return m_fd >= 0; }

    // Close and delete the temp file
    void close();

private:
    int m_fd = -1;                          // File descriptor
    std::string m_path;                     // Temp file path
    char* m_mapped_data = nullptr;          // mmap'd region (read-only view)
    size_t m_mapped_size = 0;               // Current mapped size
    std::atomic<size_t> m_size{0};          // Actual written size
    size_t m_file_capacity = 0;             // Current file allocation size
    mutable std::mutex m_remap_mutex;       // Protects remap operation

    static constexpr size_t INITIAL_CAPACITY = 64 * 1024;           // 64KB
    static constexpr size_t GROWTH_CHUNK = 1024 * 1024;             // 1MB growth increments
};
