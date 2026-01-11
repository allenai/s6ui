#include "browser_ui.h"
#include "imgui/imgui.h"
#include <cstring>
#include <sstream>

BrowserUI::BrowserUI(BrowserModel& model)
    : m_model(model)
{
    std::strcpy(m_pathInput, "s3://");
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

    // Scrollable content area
    ImVec2 contentSize = ImGui::GetContentRegionAvail();
    ImGui::BeginChild("ScrollingContent", contentSize, true,
        ImGuiWindowFlags_HorizontalScrollbar |
        ImGuiWindowFlags_AlwaysVerticalScrollbar);

    renderBucketTree();

    ImGui::EndChild();

    ImGui::End();
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

void BrowserUI::renderBucketTree() {
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

    for (const auto& bucket : buckets) {
        ImGuiTreeNodeFlags flags = ImGuiTreeNodeFlags_OpenOnArrow |
                                   ImGuiTreeNodeFlags_OpenOnDoubleClick;

        // Check for pending expansion
        auto* bucketNode = m_model.getNode(bucket.name, "");
        if (bucketNode && bucketNode->pending_expand) {
            ImGui::SetNextItemOpen(true, ImGuiCond_Always);
            bucketNode->pending_expand = false;
        }

        std::string nodeId = "bucket##" + bucket.name;
        bool bucketOpen = ImGui::TreeNodeEx(nodeId.c_str(), flags,
            "[B] %s", bucket.name.c_str());

        // Scroll to this bucket if it's the target
        if (m_model.hasScrollTarget() &&
            bucket.name == m_model.scrollTargetBucket() &&
            m_model.scrollTargetPrefix().empty()) {
            ImGui::SetScrollHereY(0.5f);
            m_model.clearScrollTarget();
        }

        if (bucketOpen) {
            auto* node = m_model.getNode(bucket.name, "");
            bool needsLoad = !node || (node->objects.empty() && !node->loading && node->error.empty());
            bool wasExpanded = node && node->expanded;

            if (!wasExpanded) {
                m_model.expandNode(bucket.name, "");
            } else if (needsLoad && node && !node->loading) {
                m_model.expandNode(bucket.name, "");
            }

            renderFolder(bucket.name, "");
            ImGui::TreePop();
        } else {
            m_model.collapseNode(bucket.name, "");
        }
    }

    if (buckets.empty()) {
        ImGui::TextColored(ImVec4(0.7f, 0.7f, 0.7f, 1.0f), "No buckets found");
    }
}

void BrowserUI::renderFolder(const std::string& bucket, const std::string& prefix) {
    auto* node = m_model.getNode(bucket, prefix);
    if (!node) return;

    // Render folders first
    for (const auto& obj : node->objects) {
        if (!obj.is_folder) continue;

        // Check for pending expansion
        auto* childNode = m_model.getNode(bucket, obj.key);
        if (childNode && childNode->pending_expand) {
            ImGui::SetNextItemOpen(true, ImGuiCond_Always);
            childNode->pending_expand = false;
        }

        ImGuiTreeNodeFlags flags = ImGuiTreeNodeFlags_OpenOnArrow |
                                   ImGuiTreeNodeFlags_OpenOnDoubleClick;

        std::string nodeId = "folder##" + bucket + "/" + obj.key;
        bool nodeOpen = ImGui::TreeNodeEx(nodeId.c_str(), flags,
            "[D] %s", obj.display_name.c_str());

        // Scroll to this folder if it's the target
        if (m_model.hasScrollTarget() &&
            bucket == m_model.scrollTargetBucket() &&
            obj.key == m_model.scrollTargetPrefix()) {
            ImGui::SetScrollHereY(0.5f);
            m_model.clearScrollTarget();
        }

        if (nodeOpen) {
            bool wasExpanded = childNode && childNode->expanded;
            if (!wasExpanded) {
                m_model.expandNode(bucket, obj.key);
            }

            renderFolder(bucket, obj.key);
            ImGui::TreePop();
        } else {
            m_model.collapseNode(bucket, obj.key);
        }
    }

    // Render files
    for (const auto& obj : node->objects) {
        if (obj.is_folder) continue;

        std::string nodeId = "file##" + bucket + "/" + obj.key;
        ImGui::TreeNodeEx(nodeId.c_str(),
            ImGuiTreeNodeFlags_Leaf | ImGuiTreeNodeFlags_NoTreePushOnOpen,
            "    %s  (%s)", obj.display_name.c_str(), formatSize(obj.size).c_str());
    }

    // Show loading indicator
    if (node->loading) {
        ImGui::TextColored(ImVec4(0.5f, 0.5f, 1.0f, 1.0f), "  Loading...");
    }

    // Show error
    if (!node->error.empty()) {
        ImGui::TextColored(ImVec4(1.0f, 0.3f, 0.3f, 1.0f),
            "  Error: %s", node->error.c_str());
    }

    // Show "Load more" button if truncated
    if (node->is_truncated && !node->loading && !node->next_continuation_token.empty()) {
        ImGui::Indent();
        std::string buttonId = "Load more##" + bucket + prefix;
        if (ImGui::SmallButton(buttonId.c_str())) {
            m_model.loadMore(bucket, prefix);
        }
        ImGui::SameLine();
        ImGui::TextColored(ImVec4(0.7f, 0.7f, 0.7f, 1.0f),
            "(%zu items loaded)", node->objects.size());
        ImGui::Unindent();
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
