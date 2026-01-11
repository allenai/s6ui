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
static char g_path_input[2048] = "s3://";

// Navigation scroll state
static bool g_scroll_to_current = false;
static std::string g_scroll_target_bucket;
static std::string g_scroll_target_prefix;

// Forward declarations
static void load_objects(const std::string& bucket, const std::string& prefix,
                        const std::string& continuation_token = "");

// Parse s3:// path into bucket and prefix
static bool parse_s3_path(const std::string& path, std::string& bucket, std::string& prefix) {
    bucket.clear();
    prefix.clear();

    // Check for s3:// prefix
    std::string p = path;
    if (p.substr(0, 5) == "s3://") {
        p = p.substr(5);
    } else if (p.substr(0, 3) == "s3:") {
        p = p.substr(3);
    }

    // Remove leading slashes
    while (!p.empty() && p[0] == '/') {
        p = p.substr(1);
    }

    if (p.empty()) {
        return true;  // Valid empty path (root)
    }

    // Split bucket/prefix
    size_t slash = p.find('/');
    if (slash == std::string::npos) {
        bucket = p;
    } else {
        bucket = p.substr(0, slash);
        prefix = p.substr(slash + 1);
    }

    return true;
}

// Build s3:// path from bucket and prefix
static std::string build_s3_path(const std::string& bucket, const std::string& prefix) {
    if (bucket.empty()) {
        return "s3://";
    }
    if (prefix.empty()) {
        return "s3://" + bucket + "/";
    }
    return "s3://" + bucket + "/" + prefix;
}

// Navigate to a specific path (updates path display only, does not acquire locks)
static void navigate_to_path(const std::string& bucket, const std::string& prefix) {
    g_state.current_bucket = bucket;
    g_state.current_prefix = prefix;

    // Update path input
    std::string path = build_s3_path(bucket, prefix);
    strncpy(g_path_input, path.c_str(), sizeof(g_path_input) - 1);
    g_path_input[sizeof(g_path_input) - 1] = '\0';
}

// Navigate to a path from user input - adds bucket if needed and expands path
static void navigate_to_path_from_input(const std::string& bucket, const std::string& prefix) {
    if (bucket.empty()) return;

    // Add bucket to list if not present
    {
        std::lock_guard<std::mutex> lock(g_state.mutex);
        bool found = false;
        for (const auto& b : g_state.buckets) {
            if (b.name == bucket) {
                found = true;
                break;
            }
        }
        if (!found) {
            S3Bucket new_bucket;
            new_bucket.name = bucket;
            new_bucket.creation_date = "(manually added)";
            g_state.buckets.push_back(new_bucket);
        }
    }

    // Mark all path components for expansion (one-shot)
    // First, expand the bucket root
    {
        auto node = g_state.get_path_node(bucket, "");
        std::lock_guard<std::mutex> lock(g_state.mutex);
        node->pending_expand = true;
        node->expanded = true;
        node->objects.clear();
    }

    // Then expand each prefix component along the path
    if (!prefix.empty()) {
        std::string current_prefix;
        size_t pos = 0;
        while (pos < prefix.size()) {
            size_t next_slash = prefix.find('/', pos);
            if (next_slash == std::string::npos) {
                // Last component - might be a file or incomplete folder name
                current_prefix = prefix;
                pos = prefix.size();
            } else {
                current_prefix = prefix.substr(0, next_slash + 1);
                pos = next_slash + 1;
            }

            auto node = g_state.get_path_node(bucket, current_prefix);
            std::lock_guard<std::mutex> lock(g_state.mutex);
            node->pending_expand = true;
            node->expanded = true;
            node->objects.clear();
        }
    }

    // Update path display
    navigate_to_path(bucket, prefix);

    // Set scroll target
    g_scroll_to_current = true;
    g_scroll_target_bucket = bucket;
    g_scroll_target_prefix = prefix;

    // Load the bucket root first (needed to see folder structure)
    load_objects(bucket, "");

    // Load the target prefix if different from root
    if (!prefix.empty()) {
        load_objects(bucket, prefix);
    }
}

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
                        const std::string& continuation_token) {
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

        // Check if this folder has pending expansion (one-shot)
        auto child_node = g_state.get_path_node(bucket, obj.key);
        bool pending;
        {
            std::lock_guard<std::mutex> lock(g_state.mutex);
            pending = child_node->pending_expand;
            if (pending) {
                child_node->pending_expand = false;
            }
        }
        if (pending) {
            ImGui::SetNextItemOpen(true, ImGuiCond_Always);
        }

        ImGuiTreeNodeFlags flags = ImGuiTreeNodeFlags_OpenOnArrow | ImGuiTreeNodeFlags_OpenOnDoubleClick;
        // Use unique ID with folder## prefix to avoid conflicts
        std::string node_id = "folder##" + bucket + "/" + obj.key;
        bool node_open = ImGui::TreeNodeEx(node_id.c_str(), flags, "[D] %s", obj.display_name.c_str());

        // Scroll to this folder if it's the target
        if (g_scroll_to_current && bucket == g_scroll_target_bucket &&
            obj.key == g_scroll_target_prefix) {
            ImGui::SetScrollHereY(0.5f);
            g_scroll_to_current = false;
        }

        if (node_open) {
            bool just_expanded = false;
            {
                std::lock_guard<std::mutex> lock(g_state.mutex);
                if (!child_node->expanded) {
                    child_node->expanded = true;
                    child_node->objects.clear();
                    just_expanded = true;
                }
            }

            // Update current path when folder is opened (outside lock)
            if (just_expanded) {
                navigate_to_path(bucket, obj.key);
            }

            // Load if needed
            if (child_node->objects.empty() && !child_node->loading.load() && child_node->error.empty()) {
                load_objects(bucket, obj.key);
            }

            render_folder(bucket, obj.key);
            ImGui::TreePop();
        } else {
            // Collapsed - mark as not expanded
            std::lock_guard<std::mutex> lock(g_state.mutex);
            child_node->expanded = false;
        }
    }

    // Render files
    for (const auto& obj : objects) {
        if (obj.is_folder) continue;

        // Use unique ID with file## prefix to avoid conflicts
        std::string node_id = "file##" + bucket + "/" + obj.key;
        ImGui::TreeNodeEx(node_id.c_str(),
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

            // Get framebuffer size for Metal (in pixels)
            int fb_width, fb_height;
            glfwGetFramebufferSize(window, &fb_width, &fb_height);
            layer.drawableSize = CGSizeMake(fb_width, fb_height);

            // Get window size for ImGui (in screen coordinates)
            int win_width, win_height;
            glfwGetWindowSize(window, &win_width, &win_height);

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

            // Full window S3 Browser (use window size, not framebuffer size)
            ImGui::SetNextWindowPos(ImVec2(0, 0));
            ImGui::SetNextWindowSize(ImVec2((float)win_width, (float)win_height));
            ImGui::Begin("S3 Browser", nullptr,
                ImGuiWindowFlags_NoTitleBar |
                ImGuiWindowFlags_NoResize |
                ImGuiWindowFlags_NoMove |
                ImGuiWindowFlags_NoCollapse |
                ImGuiWindowFlags_NoBringToFrontOnFocus |
                ImGuiWindowFlags_NoScrollbar |
                ImGuiWindowFlags_NoScrollWithMouse);

            // === Fixed Top Bar ===
            // Profile selector
            ImGui::Text("Profile:");
            ImGui::SameLine();

            if (!g_state.profiles.empty()) {
                std::vector<const char*> profile_names;
                for (const auto& p : g_state.profiles) {
                    profile_names.push_back(p.name.c_str());
                }

                int prev_idx = g_state.selected_profile_idx;
                ImGui::SetNextItemWidth(150);
                if (ImGui::Combo("##profile", &g_state.selected_profile_idx,
                                 profile_names.data(), (int)profile_names.size())) {
                    if (prev_idx != g_state.selected_profile_idx) {
                        // Profile changed - reload buckets
                        std::lock_guard<std::mutex> lock(g_state.mutex);
                        g_state.buckets.clear();
                        g_state.path_nodes.clear();
                        g_state.current_bucket.clear();
                        g_state.current_prefix.clear();
                        strncpy(g_path_input, "s3://", sizeof(g_path_input));
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

            // Path input field
            ImGui::SameLine();
            ImGui::Text("Path:");
            ImGui::SameLine();

            float refresh_button_width = 70;
            float path_input_width = ImGui::GetWindowWidth() - ImGui::GetCursorPosX() - refresh_button_width - 20;
            ImGui::SetNextItemWidth(path_input_width);

            if (ImGui::InputText("##path", g_path_input, sizeof(g_path_input),
                                 ImGuiInputTextFlags_EnterReturnsTrue)) {
                // Parse and navigate to the entered path
                std::string bucket, prefix;
                if (parse_s3_path(g_path_input, bucket, prefix)) {
                    navigate_to_path_from_input(bucket, prefix);
                }
            }

            ImGui::SameLine();
            if (ImGui::Button("Refresh")) {
                std::lock_guard<std::mutex> lock(g_state.mutex);
                g_state.buckets.clear();
                g_state.path_nodes.clear();
                load_buckets();
            }

            ImGui::Separator();

            // === Scrollable Content Area ===
            // Use available space for the child window
            ImVec2 content_size = ImGui::GetContentRegionAvail();
            ImGui::BeginChild("ScrollingContent", content_size, true,
                ImGuiWindowFlags_HorizontalScrollbar |
                ImGuiWindowFlags_AlwaysVerticalScrollbar);

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

                    // Auto-expand if this bucket has pending expansion (one-shot)
                    auto bucket_node = g_state.get_path_node(bucket.name, "");
                    bool pending;
                    {
                        std::lock_guard<std::mutex> lock(g_state.mutex);
                        pending = bucket_node->pending_expand;
                        if (pending) {
                            bucket_node->pending_expand = false;
                        }
                    }
                    if (pending) {
                        ImGui::SetNextItemOpen(true, ImGuiCond_Always);
                    }

                    // Use unique ID with bucket## prefix to avoid conflicts
                    std::string node_id = "bucket##" + bucket.name;
                    bool bucket_open = ImGui::TreeNodeEx(node_id.c_str(), flags,
                        "[B] %s", bucket.name.c_str());

                    // Scroll to this bucket if it's the target and prefix is empty
                    if (g_scroll_to_current && bucket.name == g_scroll_target_bucket &&
                        g_scroll_target_prefix.empty()) {
                        ImGui::SetScrollHereY(0.5f);
                        g_scroll_to_current = false;
                    }

                    if (bucket_open) {
                        auto node = g_state.get_path_node(bucket.name, "");
                        bool needs_load;
                        bool just_expanded = false;
                        {
                            std::lock_guard<std::mutex> lock(g_state.mutex);
                            needs_load = node->objects.empty() && !node->loading.load() && node->error.empty();
                            if (!node->expanded) {
                                node->expanded = true;
                                node->objects.clear();
                                needs_load = true;
                                just_expanded = true;
                            }
                        }

                        // Update current path when bucket is opened (outside lock)
                        if (just_expanded) {
                            navigate_to_path(bucket.name, "");
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

            ImGui::EndChild();  // End ScrollingContent

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
