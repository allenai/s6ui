#include "browser_ui.h"
#include "preview/image_preview.h"
#include "preview/jsonl_preview.h"
#include "preview/text_preview.h"
#include "aws/aws_signer.h"
#include "imgui/imgui.h"
#include <cstring>

BrowserUI::BrowserUI(BrowserModel& model)
    : m_model(model)
{
    std::strcpy(m_pathInput, "s3://");

    // Initialize preview renderers (order matters - first match wins)
    m_previewRenderers.push_back(std::make_unique<ImagePreviewRenderer>());
    m_previewRenderers.push_back(std::make_unique<JsonlPreviewRenderer>());
    m_previewRenderers.push_back(std::make_unique<TextPreviewRenderer>());
}

void BrowserUI::render(int windowWidth, int windowHeight) {
    ImGui::SetNextWindowPos(ImVec2(0, 0));
    ImGui::SetNextWindowSize(ImVec2(static_cast<float>(windowWidth),
                                     static_cast<float>(windowHeight)));
    ImGui::Begin("S6 UI", nullptr,
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

    // Show [..] outside scrolling area when in a folder
    if (!m_model.isAtRoot()) {
        if (ImGui::Selectable("[..]")) {
            m_model.navigateUp();
        }
        // Prefetch parent folder on hover for instant navigation
        if (ImGui::IsItemHovered()) {
            const std::string& bucket = m_model.currentBucket();
            const std::string& prefix = m_model.currentPrefix();
            std::string parentPrefix = prefix;
            if (!parentPrefix.empty() && parentPrefix.back() == '/') {
                parentPrefix.pop_back();
            }
            size_t lastSlash = parentPrefix.rfind('/');
            if (lastSlash == std::string::npos) {
                parentPrefix = "";
            } else {
                parentPrefix = parentPrefix.substr(0, lastSlash + 1);
            }
            m_model.prefetchFolder(bucket, parentPrefix);
        }
        contentHeight -= ImGui::GetFrameHeightWithSpacing();
    }

    // File browser content
    ImGui::BeginChild("FileContent", ImVec2(width, contentHeight), true,
        ImGuiWindowFlags_HorizontalScrollbar);

    renderContent();

    // Right-click context menu for empty space
    if (ImGui::BeginPopupContextWindow("BrowserContextMenu", ImGuiPopupFlags_MouseButtonRight | ImGuiPopupFlags_NoOpenOverItems)) {
        if (ImGui::MenuItem("Copy path")) {
            std::string path = buildS3Path(m_model.currentBucket(), m_model.currentPrefix());
            ImGui::SetClipboardText(path.c_str());
        }
        ImGui::EndPopup();
    }

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
    float arrowButtonWidth = ImGui::GetFrameHeight();  // Square button
    float pathInputWidth = ImGui::GetWindowWidth() - ImGui::GetCursorPosX() - refreshButtonWidth - arrowButtonWidth - 24;
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

    // Recent paths dropdown arrow
    ImGui::SameLine(0, 0);
    if (ImGui::ArrowButton("##recent_paths", ImGuiDir_Down)) {
        ImGui::OpenPopup("RecentPathsPopup");
    }

    if (ImGui::BeginPopup("RecentPathsPopup")) {
        auto topPaths = m_model.topFrecentPaths(20);
        if (topPaths.empty()) {
            ImGui::TextDisabled("No recent paths");
        } else {
            for (const auto& path : topPaths) {
                if (ImGui::Selectable(path.c_str())) {
                    std::strncpy(m_pathInput, path.c_str(), sizeof(m_pathInput) - 1);
                    m_pathInput[sizeof(m_pathInput) - 1] = '\0';
                    m_model.navigateTo(path);
                }
            }
        }
        ImGui::EndPopup();
    }

    ImGui::SameLine();
    bool cmdR = ImGui::GetIO().KeySuper && ImGui::IsKeyPressed(ImGuiKey_R);
    if (ImGui::Button("Refresh") || cmdR) {
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
            ImGui::SetScrollY(0);
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

    // Rebuild sorted view if objects changed (e.g., pagination added more)
    node->rebuildSortedViewIfNeeded();

    // Use ImGuiListClipper for virtual scrolling - only render visible rows
    ImGuiListClipper clipper;
    clipper.Begin(static_cast<int>(node->sortedView.size()));

    while (clipper.Step()) {
        for (int i = clipper.DisplayStart; i < clipper.DisplayEnd; ++i) {
            size_t objIndex = node->sortedView[i];
            const auto& obj = node->objects[objIndex];
            bool isFolder = (static_cast<size_t>(i) < node->folderCount);

            ImGui::PushID(static_cast<int>(objIndex));

            if (isFolder) {
                // Render folder
                std::string label = "[D] " + obj.display_name;
                if (ImGui::Selectable(label.c_str())) {
                    m_model.navigateInto(bucket, obj.key);
                    ImGui::SetScrollY(0);
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
            } else {
                // Render file
                std::string label = "    " + obj.display_name + "  (" + formatSize(obj.size) + ")";
                // Check if this file is selected
                bool isSelected = (m_model.preview().selectedBucket() == bucket && m_model.preview().selectedKey() == obj.key);
                if (ImGui::Selectable(label.c_str(), isSelected)) {
                    m_model.selectFile(bucket, obj.key);
                }
                // Right-click context menu
                if (ImGui::BeginPopupContextItem()) {
                    if (ImGui::MenuItem("Copy path")) {
                        std::string path = "s3://" + bucket + "/" + obj.key;
                        ImGui::SetClipboardText(path.c_str());
                    }
                    if (ImGui::MenuItem("Copy pre-signed URL (7 days)")) {
                        const auto& profiles = m_model.profiles();
                        int idx = m_model.selectedProfileIndex();
                        if (idx >= 0 && idx < static_cast<int>(profiles.size())) {
                            const auto& profile = profiles[idx];
                            std::string url = aws_generate_presigned_url(
                                bucket,
                                obj.key,
                                profile.region,
                                profile.access_key_id,
                                profile.secret_access_key,
                                profile.session_token,
                                604800  // 7 days in seconds
                            );
                            ImGui::SetClipboardText(url.c_str());
                        }
                    }
                    ImGui::EndPopup();
                }
                // Prefetch preview content on hover for instant preview when clicked
                if (ImGui::IsItemHovered()) {
                    m_model.prefetchFilePreview(bucket, obj.key);
                }
            }

            ImGui::PopID();
        }
    }

    // Show inline loading indicator if loading more (outside clipper)
    if (node->loading) {
        ImGui::TextColored(ImVec4(0.5f, 0.5f, 1.0f, 1.0f), "Loading more...");
    }

    // Show "Load more" button if truncated (outside clipper)
    if (node->is_truncated && !node->loading && !node->next_continuation_token.empty()) {
        ImGui::Spacing();
        if (ImGui::Button("Load more")) {
            m_model.loadMore(bucket, prefix);
        }
        ImGui::SameLine();
        ImGui::TextColored(ImVec4(0.7f, 0.7f, 0.7f, 1.0f),
            "(%s items loaded)", formatNumber(node->objects.size()).c_str());
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
            ImGui::Text("%s bucket%s", formatNumber(bucketCount).c_str(), bucketCount == 1 ? "" : "s");
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
                status += formatNumber(folderCount) + " folder" + (folderCount == 1 ? "" : "s");
            }
            if (fileCount > 0) {
                if (!status.empty()) status += ", ";
                status += formatNumber(fileCount) + " file" + (fileCount == 1 ? "" : "s");
                status += " (" + formatSize(totalSize) + ")";
            }
            if (status.empty()) {
                status = "Empty folder";
            }

            // Add loading/truncation indicator
            if (node->loading) {
                status += "  Loading...";
            } else if (node->is_truncated) {
                status += "  [more available]";
            }

            ImGui::Text("%s", status.c_str());
        }
    }
}

void BrowserUI::renderPreviewPane(float width, float height) {
    ImGui::BeginChild("PreviewPane", ImVec2(width, height), true);

    auto& preview = m_model.preview();

    if (!preview.hasSelection()) {
        // No file selected - reset active renderer
        if (m_activeRenderer) {
            m_activeRenderer->reset();
            m_activeRenderer = nullptr;
        }
        ImGui::TextColored(ImVec4(0.5f, 0.5f, 0.5f, 1.0f), "Select a file to preview");
    } else {
        const std::string& key = preview.selectedKey();
        size_t lastSlash = key.rfind('/');
        std::string filename = (lastSlash != std::string::npos) ? key.substr(lastSlash + 1) : key;

        if (!preview.previewSupported()) {
            if (m_activeRenderer) {
                m_activeRenderer->reset();
                m_activeRenderer = nullptr;
            }
            ImGui::Text("Preview: %s", filename.c_str());
            ImGui::Separator();
            ImGui::TextColored(ImVec4(0.7f, 0.7f, 0.7f, 1.0f), "Preview not supported for this file type");
            ImGui::Spacing();
            ImGui::TextColored(ImVec4(0.5f, 0.5f, 0.5f, 1.0f),
                "Supported: .txt, .md, .html, .htm, .json, .jsonl");
        } else if (preview.previewLoading()) {
            ImGui::Text("Preview: %s", filename.c_str());
            ImGui::Separator();
            ImGui::TextColored(ImVec4(0.5f, 0.5f, 1.0f, 1.0f), "Loading preview...");
        } else if (!preview.previewError().empty()) {
            if (m_activeRenderer) {
                m_activeRenderer->reset();
                m_activeRenderer = nullptr;
            }
            ImGui::Text("Preview: %s", filename.c_str());
            ImGui::Separator();
            ImGui::TextColored(ImVec4(1.0f, 0.3f, 0.3f, 1.0f),
                "Error: %s", preview.previewError().c_str());
        } else {
            // Find appropriate renderer
            IPreviewRenderer* renderer = nullptr;
            for (auto& r : m_previewRenderers) {
                // For JSONL renderer, also check that streaming preview is available
                if (r->canHandle(key)) {
                    // Special case: JSONL renderer needs streaming preview and valid JSONL content
                    if (dynamic_cast<JsonlPreviewRenderer*>(r.get())) {
                        if (!preview.hasStreamingPreview()) {
                            // If no streaming preview, skip JSONL renderer and use text
                            continue;
                        }
                        // Check if this file was determined to not be valid JSONL
                        if (r->wantsFallback(preview.selectedBucket(), key)) {
                            continue;
                        }
                    }
                    renderer = r.get();
                    break;
                }
            }

            if (renderer) {
                // Switch renderers if needed
                if (renderer != m_activeRenderer) {
                    if (m_activeRenderer) {
                        m_activeRenderer->reset();
                    }
                    m_activeRenderer = renderer;
                }

                ImVec2 availSize = ImGui::GetContentRegionAvail();
                std::string content = preview.previewContent();
                PreviewContext ctx{
                    preview.selectedBucket(),
                    key,
                    filename,
                    preview.streamingPreview(),
                    content,
                    preview.selectedFileSize(),
                    availSize.x,
                    availSize.y
                };
                renderer->render(ctx);
            } else {
                // Fallback: no renderer found
                ImGui::Text("Preview: %s", filename.c_str());
                ImGui::Separator();
                ImGui::TextColored(ImVec4(0.7f, 0.7f, 0.7f, 1.0f), "No preview renderer available");
            }
        }
    }

    ImGui::EndChild();
}

std::string BrowserUI::formatNumber(int64_t number) {
    std::string numStr = std::to_string(number);
    std::string result;
    int count = 0;

    for (auto it = numStr.rbegin(); it != numStr.rend(); ++it) {
        if (count > 0 && count % 3 == 0) {
            result = ',' + result;
        }
        result = *it + result;
        count++;
    }
    return result;
}

std::string BrowserUI::formatSize(int64_t bytes) {
    if (bytes < 1024) return formatNumber(bytes) + " B";
    if (bytes < 1024 * 1024) return formatNumber(bytes / 1024) + " KB";
    if (bytes < 1024 * 1024 * 1024) return formatNumber(bytes / (1024 * 1024)) + " MB";
    return formatNumber(bytes / (1024 * 1024 * 1024)) + " GB";
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
