#include "browser_ui.h"
#include "imgui/imgui.h"
#include <cstring>
#include <cctype>

BrowserUI::BrowserUI(BrowserModel& model)
    : m_model(model)
{
    std::strcpy(m_pathInput, "s3://");

    // Configure the text editor for read-only preview
    m_editor.SetReadOnly(true);
    m_editor.SetPalette(TextEditor::GetDarkPalette());
    m_editor.SetShowWhitespaces(false);
}

void BrowserUI::render(int windowWidth, int windowHeight) {
    ImGui::SetNextWindowPos(ImVec2(0, 0));
    ImGui::SetNextWindowSize(ImVec2(static_cast<float>(windowWidth),
                                     static_cast<float>(windowHeight)));
    ImGui::Begin("S3 Browser", nullptr,
        ImGuiWindowFlags_NoTitleBar |
        ImGuiWindowFlags_NoResize |
        ImGuiWindowFlags_NoMove |
        ImGuiWindowFlags_NoCollapse |
        ImGuiWindowFlags_NoBringToFrontOnFocus |
        ImGuiWindowFlags_NoScrollbar |
        ImGuiWindowFlags_NoScrollWithMouse);

    renderTopBar();

    ImGui::Separator();

    // Calculate pane sizes
    ImVec2 availSize = ImGui::GetContentRegionAvail();
    float paneWidth = availSize.x * 0.5f - 4;  // Half width minus spacing
    float paneHeight = availSize.y;

    // Left pane (file browser + status bar)
    renderLeftPane(paneWidth, paneHeight);

    ImGui::SameLine();

    // Right pane (preview)
    renderPreviewPane(paneWidth, paneHeight);

    ImGui::End();
}

void BrowserUI::renderLeftPane(float width, float height) {
    ImGui::BeginChild("LeftPane", ImVec2(width, height), false);

    // Reserve space for status bar
    float statusBarHeight = ImGui::GetFrameHeightWithSpacing() + 4;
    float contentHeight = height - statusBarHeight;

    // File browser content
    ImGui::BeginChild("FileContent", ImVec2(width, contentHeight), true,
        ImGuiWindowFlags_HorizontalScrollbar);

    renderContent();

    ImGui::EndChild();

    // Status bar
    renderStatusBar();

    ImGui::EndChild();
}

void BrowserUI::renderTopBar() {
    // Profile selector
    ImGui::Text("Profile:");
    ImGui::SameLine();

    const auto& profiles = m_model.profiles();
    if (!profiles.empty()) {
        std::vector<const char*> profileNames;
        for (const auto& p : profiles) {
            profileNames.push_back(p.name.c_str());
        }

        int selectedIdx = m_model.selectedProfileIndex();
        ImGui::SetNextItemWidth(150);
        if (ImGui::Combo("##profile", &selectedIdx,
                         profileNames.data(), static_cast<int>(profileNames.size()))) {
            m_model.selectProfile(selectedIdx);
            std::strcpy(m_pathInput, "s3://");
        }

        ImGui::SameLine();
        ImGui::TextColored(ImVec4(0.5f, 0.5f, 0.5f, 1.0f),
            "(%s)", profiles[selectedIdx].region.c_str());
    } else {
        ImGui::TextColored(ImVec4(1.0f, 0.3f, 0.3f, 1.0f),
            "No AWS profiles found in ~/.aws/credentials");
    }

    // Path input field
    ImGui::SameLine();
    ImGui::Text("Path:");
    ImGui::SameLine();

    float refreshButtonWidth = 70;
    float pathInputWidth = ImGui::GetWindowWidth() - ImGui::GetCursorPosX() - refreshButtonWidth - 20;
    ImGui::SetNextItemWidth(pathInputWidth);

    if (ImGui::InputText("##path", m_pathInput, sizeof(m_pathInput),
                         ImGuiInputTextFlags_EnterReturnsTrue)) {
        m_model.navigateTo(m_pathInput);
    }

    // Update path input from model's current path
    std::string currentPath = buildS3Path(m_model.currentBucket(), m_model.currentPrefix());
    if (currentPath != m_pathInput && !ImGui::IsItemActive()) {
        std::strncpy(m_pathInput, currentPath.c_str(), sizeof(m_pathInput) - 1);
        m_pathInput[sizeof(m_pathInput) - 1] = '\0';
    }

    ImGui::SameLine();
    if (ImGui::Button("Refresh")) {
        m_model.refresh();
    }
}

void BrowserUI::renderContent() {
    if (m_model.isAtRoot()) {
        renderBucketList();
    } else {
        renderFolderContents();
    }
}

void BrowserUI::renderBucketList() {
    if (m_model.bucketsLoading()) {
        ImGui::TextColored(ImVec4(0.5f, 0.5f, 1.0f, 1.0f), "Loading buckets...");
        return;
    }

    if (!m_model.bucketsError().empty()) {
        ImGui::TextColored(ImVec4(1.0f, 0.3f, 0.3f, 1.0f),
            "Error: %s", m_model.bucketsError().c_str());
        return;
    }

    const auto& buckets = m_model.buckets();

    if (buckets.empty()) {
        ImGui::TextColored(ImVec4(0.7f, 0.7f, 0.7f, 1.0f), "No buckets found");
        return;
    }

    // Render each bucket as a selectable item
    for (const auto& bucket : buckets) {
        std::string label = "[B] " + bucket.name;
        if (ImGui::Selectable(label.c_str())) {
            m_model.navigateInto(bucket.name, "");
        }
        // Right-click context menu
        if (ImGui::BeginPopupContextItem()) {
            if (ImGui::MenuItem("Copy path")) {
                std::string path = "s3://" + bucket.name + "/";
                ImGui::SetClipboardText(path.c_str());
            }
            ImGui::EndPopup();
        }
    }
}

void BrowserUI::renderFolderContents() {
    const std::string& bucket = m_model.currentBucket();
    const std::string& prefix = m_model.currentPrefix();

    // Get or load the current folder node
    auto* node = m_model.getNode(bucket, prefix);
    if (!node) {
        // Node doesn't exist yet, trigger load
        m_model.loadFolder(bucket, prefix);
        ImGui::TextColored(ImVec4(0.5f, 0.5f, 1.0f, 1.0f), "Loading...");
        return;
    }

    // Show [..] to navigate up
    if (ImGui::Selectable("[..]")) {
        m_model.navigateUp();
        return;  // Return early to avoid rendering stale content
    }

    // Show loading indicator
    if (node->loading && node->objects.empty()) {
        ImGui::TextColored(ImVec4(0.5f, 0.5f, 1.0f, 1.0f), "Loading...");
        return;
    }

    // Show error if any
    if (!node->error.empty()) {
        ImGui::TextColored(ImVec4(1.0f, 0.3f, 0.3f, 1.0f),
            "Error: %s", node->error.c_str());
        return;
    }

    // Render folders first
    for (const auto& obj : node->objects) {
        if (!obj.is_folder) continue;

        std::string label = "[D] " + obj.display_name;
        if (ImGui::Selectable(label.c_str())) {
            m_model.navigateInto(bucket, obj.key);
        }
        // Right-click context menu
        if (ImGui::BeginPopupContextItem()) {
            if (ImGui::MenuItem("Copy path")) {
                std::string path = "s3://" + bucket + "/" + obj.key;
                ImGui::SetClipboardText(path.c_str());
            }
            ImGui::EndPopup();
        }
        // Prefetch folder contents on hover for instant navigation
        if (ImGui::IsItemHovered()) {
            m_model.prefetchFolder(bucket, obj.key);
        }
    }

    // Render files
    for (const auto& obj : node->objects) {
        if (obj.is_folder) continue;

        std::string label = "    " + obj.display_name + "  (" + formatSize(obj.size) + ")";
        // Check if this file is selected
        bool isSelected = (m_model.selectedBucket() == bucket && m_model.selectedKey() == obj.key);
        if (ImGui::Selectable(label.c_str(), isSelected)) {
            m_model.selectFile(bucket, obj.key);
        }
        // Right-click context menu
        if (ImGui::BeginPopupContextItem()) {
            if (ImGui::MenuItem("Copy path")) {
                std::string path = "s3://" + bucket + "/" + obj.key;
                ImGui::SetClipboardText(path.c_str());
            }
            ImGui::EndPopup();
        }
        // Prefetch preview content on hover for instant preview when clicked
        if (ImGui::IsItemHovered()) {
            m_model.prefetchFilePreview(bucket, obj.key);
        }
    }

    // Show inline loading indicator if loading more
    if (node->loading) {
        ImGui::TextColored(ImVec4(0.5f, 0.5f, 1.0f, 1.0f), "Loading more...");
    }

    // Show "Load more" button if truncated
    if (node->is_truncated && !node->loading && !node->next_continuation_token.empty()) {
        ImGui::Spacing();
        if (ImGui::Button("Load more")) {
            m_model.loadMore(bucket, prefix);
        }
        ImGui::SameLine();
        ImGui::TextColored(ImVec4(0.7f, 0.7f, 0.7f, 1.0f),
            "(%zu items loaded)", node->objects.size());
    }
}

void BrowserUI::renderStatusBar() {
    ImGui::Separator();

    if (m_model.isAtRoot()) {
        // At bucket list - show bucket count
        if (m_model.bucketsLoading()) {
            ImGui::Text("Loading buckets...");
        } else if (!m_model.bucketsError().empty()) {
            ImGui::TextColored(ImVec4(1.0f, 0.3f, 0.3f, 1.0f), "Error loading buckets");
        } else {
            size_t bucketCount = m_model.buckets().size();
            ImGui::Text("%zu bucket%s", bucketCount, bucketCount == 1 ? "" : "s");
        }
    } else {
        // In a folder - show object stats
        const std::string& bucket = m_model.currentBucket();
        const std::string& prefix = m_model.currentPrefix();
        auto* node = m_model.getNode(bucket, prefix);

        if (!node || (node->loading && node->objects.empty())) {
            ImGui::Text("Loading...");
        } else if (!node->error.empty()) {
            ImGui::TextColored(ImVec4(1.0f, 0.3f, 0.3f, 1.0f), "Error");
        } else {
            // Count folders and files, sum sizes
            size_t folderCount = 0;
            size_t fileCount = 0;
            int64_t totalSize = 0;

            for (const auto& obj : node->objects) {
                if (obj.is_folder) {
                    folderCount++;
                } else {
                    fileCount++;
                    totalSize += obj.size;
                }
            }

            // Build status string
            std::string status;
            if (folderCount > 0) {
                status += std::to_string(folderCount) + " folder" + (folderCount == 1 ? "" : "s");
            }
            if (fileCount > 0) {
                if (!status.empty()) status += ", ";
                status += std::to_string(fileCount) + " file" + (fileCount == 1 ? "" : "s");
                status += " (" + formatSize(totalSize) + ")";
            }
            if (status.empty()) {
                status = "Empty folder";
            }

            // Add truncation indicator
            if (node->is_truncated) {
                status += "  [more available]";
            }

            ImGui::Text("%s", status.c_str());
        }
    }
}

void BrowserUI::renderPreviewPane(float width, float height) {
    ImGui::BeginChild("PreviewPane", ImVec2(width, height), true);

    if (!m_model.hasSelection()) {
        m_editorCurrentKey.clear();
        m_editorLoadedBytes = 0;
        ImGui::TextColored(ImVec4(0.5f, 0.5f, 0.5f, 1.0f), "Select a file to preview");
    } else {
        // Show filename header
        // Extract just the filename from the key
        const std::string& key = m_model.selectedKey();
        size_t lastSlash = key.rfind('/');
        std::string filename = (lastSlash != std::string::npos) ? key.substr(lastSlash + 1) : key;
        ImGui::Text("Preview: %s", filename.c_str());
        ImGui::Separator();

        if (!m_model.previewSupported()) {
            m_editorCurrentKey.clear();
            m_editorLoadedBytes = 0;
            ImGui::TextColored(ImVec4(0.7f, 0.7f, 0.7f, 1.0f), "Preview not supported for this file type");
            ImGui::Spacing();
            ImGui::TextColored(ImVec4(0.5f, 0.5f, 0.5f, 1.0f),
                "Supported: text, code, config, and data files");
        } else if (m_model.isStreamingPreview()) {
            // Streaming preview mode
            renderStreamingPreview(filename);
        } else if (m_model.previewLoading()) {
            ImGui::TextColored(ImVec4(0.5f, 0.5f, 1.0f, 1.0f), "Loading preview...");
        } else if (!m_model.previewError().empty()) {
            m_editorCurrentKey.clear();
            m_editorLoadedBytes = 0;
            ImGui::TextColored(ImVec4(1.0f, 0.3f, 0.3f, 1.0f),
                "Error: %s", m_model.previewError().c_str());
        } else {
            // Show preview content using the syntax-highlighted editor
            const std::string& content = m_model.previewContent();
            if (content.empty()) {
                m_editorCurrentKey.clear();
                m_editorLoadedBytes = 0;
                ImGui::TextColored(ImVec4(0.5f, 0.5f, 0.5f, 1.0f), "(empty file)");
            } else {
                // Update editor content if file changed
                std::string fullKey = m_model.selectedBucket() + "/" + key;
                if (m_editorCurrentKey != fullKey) {
                    m_editorCurrentKey = fullKey;
                    m_editorLoadedBytes = content.size();
                    m_editor.SetText(content);
                    updateEditorLanguage(filename);
                    // Reset cursor and selection for new file
                    m_editor.SetCursorPosition(TextEditor::Coordinates(0, 0));
                    m_editor.SetSelection(TextEditor::Coordinates(0, 0), TextEditor::Coordinates(0, 0));
                }

                // Render the editor (read-only, with syntax highlighting)
                ImVec2 availSize = ImGui::GetContentRegionAvail();
                m_editor.Render("##preview", availSize, false);
            }
        }
    }

    ImGui::EndChild();
}

void BrowserUI::renderStreamingPreview(const std::string& filename) {
    size_t loaded = m_model.streamingPreviewSize();
    size_t total = m_model.streamingPreviewTotalSize();
    bool complete = m_model.streamingPreviewComplete();
    bool hasError = m_model.streamingPreviewHasError();

    // Progress indicator
    if (!complete) {
        if (total > 0) {
            float progress = static_cast<float>(loaded) / static_cast<float>(total);
            char overlay[64];
            snprintf(overlay, sizeof(overlay), "%zu / %zu KB", loaded / 1024, total / 1024);
            ImGui::ProgressBar(progress, ImVec2(-1, 0), overlay);
        } else {
            ImGui::Text("Loading... %zu KB", loaded / 1024);
        }
    } else {
        ImGui::TextColored(ImVec4(0.4f, 0.8f, 0.4f, 1.0f), "Loaded: %zu KB", loaded / 1024);
    }

    // Error indicator (but still show content if we have any)
    if (hasError) {
        ImGui::SameLine();
        ImGui::TextColored(ImVec4(1.0f, 0.5f, 0.3f, 1.0f), " (Error: %s)",
            m_model.streamingPreviewError().c_str());
    }

    // Get streaming data
    const char* data = m_model.streamingPreviewData();

    // Check if we should update the editor
    // Update at thresholds: 64KB, 256KB, 1MB, then every 1MB, and on completion
    bool shouldRefresh = false;
    static const size_t thresholds[] = { 64 * 1024, 256 * 1024, 1024 * 1024 };

    std::string fullKey = m_model.selectedBucket() + "/" + m_model.selectedKey();
    bool isNewFile = (m_editorCurrentKey != fullKey);

    if (isNewFile && loaded > 0) {
        shouldRefresh = true;
    } else {
        for (size_t t : thresholds) {
            if (m_editorLoadedBytes < t && loaded >= t) {
                shouldRefresh = true;
                break;
            }
        }
        // After 1MB, every 1MB
        if (!shouldRefresh && loaded >= 1024 * 1024) {
            size_t lastMB = m_editorLoadedBytes / (1024 * 1024);
            size_t currentMB = loaded / (1024 * 1024);
            if (currentMB > lastMB) {
                shouldRefresh = true;
            }
        }
        // Always refresh on completion
        if (complete && m_editorLoadedBytes < loaded) {
            shouldRefresh = true;
        }
    }

    if (shouldRefresh && data && loaded > 0) {
        m_editorCurrentKey = fullKey;

        // Cap display at 10MB for performance
        static constexpr size_t MAX_DISPLAY = 10 * 1024 * 1024;
        size_t displaySize = std::min(loaded, MAX_DISPLAY);

        std::string content(data, displaySize);
        if (loaded > MAX_DISPLAY) {
            content += "\n\n[... truncated - showing first 10MB of " +
                       std::to_string(loaded / (1024 * 1024)) + "MB ...]";
        }

        m_editor.SetText(content);
        updateEditorLanguage(filename);

        if (isNewFile) {
            m_editor.SetCursorPosition(TextEditor::Coordinates(0, 0));
            m_editor.SetSelection(TextEditor::Coordinates(0, 0), TextEditor::Coordinates(0, 0));
        }

        m_editorLoadedBytes = loaded;
    }

    // Render the editor if we have content
    if (loaded > 0 || !m_model.previewContent().empty()) {
        // If streaming just started and we have cached content, show that first
        if (loaded == 0 && !m_model.previewContent().empty()) {
            if (isNewFile) {
                m_editorCurrentKey = fullKey;
                m_editor.SetText(m_model.previewContent());
                updateEditorLanguage(filename);
                m_editor.SetCursorPosition(TextEditor::Coordinates(0, 0));
                m_editor.SetSelection(TextEditor::Coordinates(0, 0), TextEditor::Coordinates(0, 0));
                m_editorLoadedBytes = m_model.previewContent().size();
            }
        }

        ImVec2 availSize = ImGui::GetContentRegionAvail();
        m_editor.Render("##preview", availSize, false);
    } else if (!complete && !hasError) {
        ImGui::TextColored(ImVec4(0.5f, 0.5f, 1.0f, 1.0f), "Starting download...");
    }
}

void BrowserUI::updateEditorLanguage(const std::string& filename) {
    // Find the extension
    size_t dotPos = filename.rfind('.');
    if (dotPos == std::string::npos) {
        // No extension - disable colorization to avoid regex issues
        m_editor.SetColorizerEnable(false);
        return;
    }

    std::string ext = filename.substr(dotPos);
    // Convert to lowercase for comparison
    for (auto& c : ext) {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }

    // Set language based on extension
    // For files with proper language definitions, enable colorization
    if (ext == ".cpp" || ext == ".cxx" || ext == ".cc" || ext == ".hpp" || ext == ".hxx" || ext == ".h") {
        m_editor.SetColorizerEnable(true);
        m_editor.SetLanguageDefinition(TextEditor::LanguageDefinition::CPlusPlus());
    } else if (ext == ".c") {
        m_editor.SetColorizerEnable(true);
        m_editor.SetLanguageDefinition(TextEditor::LanguageDefinition::C());
    } else if (ext == ".lua") {
        m_editor.SetColorizerEnable(true);
        m_editor.SetLanguageDefinition(TextEditor::LanguageDefinition::Lua());
    } else if (ext == ".sql") {
        m_editor.SetColorizerEnable(true);
        m_editor.SetLanguageDefinition(TextEditor::LanguageDefinition::SQL());
    } else if (ext == ".hlsl" || ext == ".fx") {
        m_editor.SetColorizerEnable(true);
        m_editor.SetLanguageDefinition(TextEditor::LanguageDefinition::HLSL());
    } else if (ext == ".glsl" || ext == ".vert" || ext == ".frag" || ext == ".geom") {
        m_editor.SetColorizerEnable(true);
        m_editor.SetLanguageDefinition(TextEditor::LanguageDefinition::GLSL());
    } else if (ext == ".as") {
        m_editor.SetColorizerEnable(true);
        m_editor.SetLanguageDefinition(TextEditor::LanguageDefinition::AngelScript());
    } else {
        // For .txt, .md, .json, .jsonl, .html etc., disable colorization
        // to avoid regex complexity issues with large/complex content
        m_editor.SetColorizerEnable(false);
    }
}

std::string BrowserUI::formatSize(int64_t bytes) {
    if (bytes < 1024) return std::to_string(bytes) + " B";
    if (bytes < 1024 * 1024) return std::to_string(bytes / 1024) + " KB";
    if (bytes < 1024 * 1024 * 1024) return std::to_string(bytes / (1024 * 1024)) + " MB";
    return std::to_string(bytes / (1024 * 1024 * 1024)) + " GB";
}

std::string BrowserUI::buildS3Path(const std::string& bucket, const std::string& prefix) {
    if (bucket.empty()) {
        return "s3://";
    }
    if (prefix.empty()) {
        return "s3://" + bucket + "/";
    }
    return "s3://" + bucket + "/" + prefix;
}
