// S3 File Browser - Dear ImGui + GLFW + Metal

#import <Metal/Metal.h>
#import <QuartzCore/QuartzCore.h>

#define GLFW_INCLUDE_NONE
#define GLFW_EXPOSE_NATIVE_COCOA
#include <GLFW/glfw3.h>
#include <GLFW/glfw3native.h>

#include "imgui/imgui.h"
#include "imgui/imgui_impl_glfw.h"
#include "imgui/imgui_impl_metal.h"

#include "aws_credentials.h"
#include "s3_client.h"

#include <stdio.h>
#include <string>
#include <memory>

static void glfw_error_callback(int error, const char* description)
{
    fprintf(stderr, "GLFW Error %d: %s\n", error, description);
}

// Format file size for display
static std::string format_size(int64_t bytes) {
    if (bytes < 1024) return std::to_string(bytes) + " B";
    if (bytes < 1024 * 1024) return std::to_string(bytes / 1024) + " KB";
    if (bytes < 1024 * 1024 * 1024) return std::to_string(bytes / (1024 * 1024)) + " MB";
    return std::to_string(bytes / (1024 * 1024 * 1024)) + " GB";
}

// Global state
static S3BrowserState g_state;
static std::unique_ptr<S3Client> g_client;

// Load buckets for the current profile
static void load_buckets() {
    if (g_state.profiles.empty()) return;
    if (g_state.buckets_loading.load()) return;

    g_state.buckets_loading = true;
    g_state.buckets_error.clear();

    const auto& profile = g_state.profiles[g_state.selected_profile_idx];
    g_client->list_buckets_async(profile, [](std::vector<S3Bucket> buckets, std::string error) {
        std::lock_guard<std::mutex> lock(g_state.mutex);
        g_state.buckets = buckets;
        g_state.buckets_error = error;
        g_state.buckets_loading = false;
        // Clear path nodes when switching profiles
        g_state.path_nodes.clear();
    });
}

// Load objects for a path
static void load_objects(const std::string& bucket, const std::string& prefix,
                        const std::string& continuation_token = "") {
    auto node = g_state.get_path_node(bucket, prefix);
    if (node->loading.load()) return;

    node->loading = true;
    if (continuation_token.empty()) {
        node->error.clear();
    }

    const auto& profile = g_state.profiles[g_state.selected_profile_idx];
    g_client->list_objects_async(profile, bucket, prefix, continuation_token,
        [node](S3ListResult result) {
            std::lock_guard<std::mutex> lock(g_state.mutex);
            if (!result.error.empty()) {
                node->error = result.error;
            } else {
                // Append objects
                for (const auto& obj : result.objects) {
                    node->objects.push_back(obj);
                }
                node->continuation_token = result.continuation_token;
                node->is_truncated = result.is_truncated;
            }
            node->loading = false;
        });
}

// Render a folder node recursively
static void render_folder(const std::string& bucket, const std::string& prefix) {
    auto node = g_state.get_path_node(bucket, prefix);

    // Copy state under lock
    std::vector<S3Object> objects;
    bool loading, is_truncated;
    std::string error, continuation_token;
    {
        std::lock_guard<std::mutex> lock(g_state.mutex);
        objects = node->objects;
        loading = node->loading.load();
        is_truncated = node->is_truncated;
        error = node->error;
        continuation_token = node->continuation_token;
    }

    // Render folders first
    for (const auto& obj : objects) {
        if (!obj.is_folder) continue;

        ImGuiTreeNodeFlags flags = ImGuiTreeNodeFlags_OpenOnArrow | ImGuiTreeNodeFlags_OpenOnDoubleClick;
        bool node_open = ImGui::TreeNodeEx(obj.key.c_str(), flags, "[D] %s", obj.display_name.c_str());

        if (ImGui::IsItemClicked() && !ImGui::IsItemToggledOpen()) {
            // Toggle expansion
        }

        if (node_open) {
            auto child_node = g_state.get_path_node(bucket, obj.key);
            bool child_expanded;
            {
                std::lock_guard<std::mutex> lock(g_state.mutex);
                child_expanded = child_node->expanded;
            }

            if (!child_expanded) {
                std::lock_guard<std::mutex> lock(g_state.mutex);
                child_node->expanded = true;
                child_node->objects.clear();
            }

            // Load if needed
            if (child_node->objects.empty() && !child_node->loading.load() && child_node->error.empty()) {
                load_objects(bucket, obj.key);
            }

            render_folder(bucket, obj.key);
            ImGui::TreePop();
        } else {
            // Collapsed - mark as not expanded
            auto child_node = g_state.get_path_node(bucket, obj.key);
            std::lock_guard<std::mutex> lock(g_state.mutex);
            child_node->expanded = false;
        }
    }

    // Render files
    for (const auto& obj : objects) {
        if (obj.is_folder) continue;

        ImGui::TreeNodeEx(obj.key.c_str(),
            ImGuiTreeNodeFlags_Leaf | ImGuiTreeNodeFlags_NoTreePushOnOpen,
            "    %s  (%s)", obj.display_name.c_str(), format_size(obj.size).c_str());
    }

    // Show loading indicator
    if (loading) {
        ImGui::TextColored(ImVec4(0.5f, 0.5f, 1.0f, 1.0f), "  Loading...");
    }

    // Show error
    if (!error.empty()) {
        ImGui::TextColored(ImVec4(1.0f, 0.3f, 0.3f, 1.0f), "  Error: %s", error.c_str());
    }

    // Show "Load more" button if truncated
    if (is_truncated && !loading && !continuation_token.empty()) {
        ImGui::Indent();
        if (ImGui::SmallButton(("Load more##" + bucket + prefix).c_str())) {
            load_objects(bucket, prefix, continuation_token);
        }
        ImGui::SameLine();
        ImGui::TextColored(ImVec4(0.7f, 0.7f, 0.7f, 1.0f), "(%zu items loaded)", objects.size());
        ImGui::Unindent();
    }
}

int main(int, char**)
{
    // Initialize S3 client
    g_client = std::make_unique<S3Client>();

    // Load AWS profiles
    g_state.profiles = load_aws_profiles();

    // Setup GLFW
    glfwSetErrorCallback(glfw_error_callback);
    if (!glfwInit())
        return 1;

    // Create window with no graphics API (we'll use Metal)
    glfwWindowHint(GLFW_CLIENT_API, GLFW_NO_API);
    GLFWwindow* window = glfwCreateWindow(1200, 800, "S3 Browser", nullptr, nullptr);
    if (window == nullptr)
        return 1;

    // Setup Metal
    id<MTLDevice> device = MTLCreateSystemDefaultDevice();
    id<MTLCommandQueue> commandQueue = [device newCommandQueue];

    // Setup CAMetalLayer
    NSWindow* nswin = glfwGetCocoaWindow(window);
    CAMetalLayer* layer = [CAMetalLayer layer];
    layer.device = device;
    layer.pixelFormat = MTLPixelFormatBGRA8Unorm;
    nswin.contentView.layer = layer;
    nswin.contentView.wantsLayer = YES;

    // Setup Dear ImGui context
    IMGUI_CHECKVERSION();
    ImGui::CreateContext();
    ImGuiIO& io = ImGui::GetIO(); (void)io;
    io.ConfigFlags |= ImGuiConfigFlags_NavEnableKeyboard;

    // Setup Dear ImGui style
    ImGui::StyleColorsDark();

    // Setup Platform/Renderer backends
    ImGui_ImplGlfw_InitForOther(window, true);
    ImGui_ImplMetal_Init(device);

    ImVec4 clear_color = ImVec4(0.1f, 0.1f, 0.1f, 1.0f);

    // Initial bucket load
    if (!g_state.profiles.empty()) {
        const auto& profile = g_state.profiles[g_state.selected_profile_idx];
        auto [buckets, error] = g_client->list_buckets(profile);
        g_state.buckets = buckets;
        g_state.buckets_error = error;
    }

    // Main loop
    while (!glfwWindowShouldClose(window))
    {
        @autoreleasepool
        {
            glfwPollEvents();

            int width, height;
            glfwGetFramebufferSize(window, &width, &height);
            layer.drawableSize = CGSizeMake(width, height);

            id<CAMetalDrawable> drawable = [layer nextDrawable];
            if (drawable == nil)
                continue;

            id<MTLCommandBuffer> commandBuffer = [commandQueue commandBuffer];

            MTLRenderPassDescriptor* renderPassDescriptor = [MTLRenderPassDescriptor new];
            renderPassDescriptor.colorAttachments[0].texture = drawable.texture;
            renderPassDescriptor.colorAttachments[0].loadAction = MTLLoadActionClear;
            renderPassDescriptor.colorAttachments[0].storeAction = MTLStoreActionStore;
            renderPassDescriptor.colorAttachments[0].clearColor = MTLClearColorMake(
                clear_color.x, clear_color.y, clear_color.z, clear_color.w
            );

            ImGui_ImplMetal_NewFrame(renderPassDescriptor);
            ImGui_ImplGlfw_NewFrame();
            ImGui::NewFrame();

            // Full window S3 Browser
            ImGui::SetNextWindowPos(ImVec2(0, 0));
            ImGui::SetNextWindowSize(ImVec2((float)width, (float)height));
            ImGui::Begin("S3 Browser", nullptr,
                ImGuiWindowFlags_NoTitleBar |
                ImGuiWindowFlags_NoResize |
                ImGuiWindowFlags_NoMove |
                ImGuiWindowFlags_NoCollapse |
                ImGuiWindowFlags_NoBringToFrontOnFocus);

            // Profile selector
            ImGui::Text("Profile:");
            ImGui::SameLine();

            if (!g_state.profiles.empty()) {
                std::vector<const char*> profile_names;
                for (const auto& p : g_state.profiles) {
                    profile_names.push_back(p.name.c_str());
                }

                int prev_idx = g_state.selected_profile_idx;
                ImGui::SetNextItemWidth(200);
                if (ImGui::Combo("##profile", &g_state.selected_profile_idx,
                                 profile_names.data(), (int)profile_names.size())) {
                    if (prev_idx != g_state.selected_profile_idx) {
                        // Profile changed - reload buckets
                        std::lock_guard<std::mutex> lock(g_state.mutex);
                        g_state.buckets.clear();
                        g_state.path_nodes.clear();
                        load_buckets();
                    }
                }

                ImGui::SameLine();
                ImGui::TextColored(ImVec4(0.5f, 0.5f, 0.5f, 1.0f),
                    "(%s)", g_state.profiles[g_state.selected_profile_idx].region.c_str());
            } else {
                ImGui::TextColored(ImVec4(1.0f, 0.3f, 0.3f, 1.0f),
                    "No AWS profiles found in ~/.aws/credentials");
            }

            ImGui::SameLine(ImGui::GetWindowWidth() - 150);
            if (ImGui::Button("Refresh")) {
                std::lock_guard<std::mutex> lock(g_state.mutex);
                g_state.buckets.clear();
                g_state.path_nodes.clear();
                load_buckets();
            }

            ImGui::Separator();

            // Buckets and objects tree
            if (g_state.buckets_loading.load()) {
                ImGui::TextColored(ImVec4(0.5f, 0.5f, 1.0f, 1.0f), "Loading buckets...");
            } else if (!g_state.buckets_error.empty()) {
                ImGui::TextColored(ImVec4(1.0f, 0.3f, 0.3f, 1.0f),
                    "Error: %s", g_state.buckets_error.c_str());
            } else {
                // Copy buckets under lock
                std::vector<S3Bucket> buckets;
                {
                    std::lock_guard<std::mutex> lock(g_state.mutex);
                    buckets = g_state.buckets;
                }

                for (const auto& bucket : buckets) {
                    ImGuiTreeNodeFlags flags = ImGuiTreeNodeFlags_OpenOnArrow |
                                               ImGuiTreeNodeFlags_OpenOnDoubleClick;

                    bool bucket_open = ImGui::TreeNodeEx(bucket.name.c_str(), flags,
                        "[B] %s", bucket.name.c_str());

                    if (bucket_open) {
                        auto node = g_state.get_path_node(bucket.name, "");
                        bool needs_load;
                        {
                            std::lock_guard<std::mutex> lock(g_state.mutex);
                            needs_load = node->objects.empty() && !node->loading.load() && node->error.empty();
                            if (!node->expanded) {
                                node->expanded = true;
                                node->objects.clear();
                                needs_load = true;
                            }
                        }

                        if (needs_load) {
                            load_objects(bucket.name, "");
                        }

                        render_folder(bucket.name, "");
                        ImGui::TreePop();
                    } else {
                        // Collapsed - get_path_node already locks internally
                        auto node = g_state.get_path_node(bucket.name, "");
                        std::lock_guard<std::mutex> lock(g_state.mutex);
                        node->expanded = false;
                    }
                }

                if (buckets.empty()) {
                    ImGui::TextColored(ImVec4(0.7f, 0.7f, 0.7f, 1.0f), "No buckets found");
                }
            }

            ImGui::End();

            // Rendering
            ImGui::Render();
            ImDrawData* draw_data = ImGui::GetDrawData();

            id<MTLRenderCommandEncoder> renderEncoder = [commandBuffer
                renderCommandEncoderWithDescriptor:renderPassDescriptor];
            [renderEncoder pushDebugGroup:@"Dear ImGui rendering"];
            ImGui_ImplMetal_RenderDrawData(draw_data, commandBuffer, renderEncoder);
            [renderEncoder popDebugGroup];
            [renderEncoder endEncoding];

            [commandBuffer presentDrawable:drawable];
            [commandBuffer commit];
        }
    }

    // Cleanup
    g_client.reset();
    ImGui_ImplMetal_Shutdown();
    ImGui_ImplGlfw_Shutdown();
    ImGui::DestroyContext();

    glfwDestroyWindow(window);
    glfwTerminate();

    return 0;
}
