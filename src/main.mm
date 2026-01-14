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

#include "browser_model.h"
#include "browser_ui.h"
#include "aws/s3_backend.h"
#include "loguru.hpp"

#include <cstdio>
#include <cstring>
#include <memory>
#include <vector>

#include "embedded_font.h"

static void glfw_error_callback(int error, const char* description)
{
    fprintf(stderr, "GLFW Error %d: %s\n", error, description);
}

int main(int argc, char* argv[])
{
    // Check for verbose flag, endpoint URL, and S3 path, filter before passing to loguru
    bool verbose = false;
    std::string initialPath;
    std::string endpointUrl;
    std::vector<char*> filtered_argv;
    filtered_argv.push_back(argv[0]);
    for (int i = 1; i < argc; ++i) {
        if (strcmp(argv[i], "-v") == 0 || strcmp(argv[i], "--verbose") == 0) {
            verbose = true;
        } else if (strcmp(argv[i], "--endpoint-url") == 0 && i + 1 < argc) {
            endpointUrl = argv[++i];
        } else if (strncmp(argv[i], "s3://", 5) == 0 || strncmp(argv[i], "s3:", 3) == 0) {
            initialPath = argv[i];
        } else {
            filtered_argv.push_back(argv[i]);
        }
    }
    filtered_argv.push_back(nullptr);  // argv must be null-terminated
    int filtered_argc = static_cast<int>(filtered_argv.size()) - 1;  // Don't count the nullptr

    // Initialize logging - stderr off by default, file always on
    loguru::g_stderr_verbosity = verbose ? loguru::Verbosity_INFO : loguru::Verbosity_OFF;
    loguru::init(filtered_argc, filtered_argv.data());
    LOG_F(INFO, "S3 Browser starting (verbose=%s)", verbose ? "true" : "false");

    // Initialize model and load profiles
    BrowserModel model;
    model.loadProfiles();

    // Apply endpoint URL to all profiles if specified
    if (!endpointUrl.empty()) {
        for (auto& profile : model.profiles()) {
            profile.endpoint_url = endpointUrl;
        }
        LOG_F(INFO, "Using custom endpoint URL: %s", endpointUrl.c_str());
    }

    // Create backend with selected profile (respects AWS_PROFILE env var)
    if (!model.profiles().empty()) {
        auto backend = std::make_unique<S3Backend>(model.profiles()[model.selectedProfileIndex()]);
        model.setBackend(std::move(backend));
        model.refresh();
    }

    // Navigate to initial path if provided
    LOG_F(INFO, "Initial path from args: '%s' (empty=%d)", initialPath.c_str(), initialPath.empty());
    if (!initialPath.empty()) {
        LOG_F(INFO, "Navigating to initial path: %s", initialPath.c_str());
        model.navigateTo(initialPath);
    }

    // Create UI
    BrowserUI ui(model);

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

    // Load embedded font with 2x oversampling for smoother rendering
    ImFontConfig fontConfig;
    fontConfig.OversampleH = 2;
    fontConfig.OversampleV = 2;
    io.Fonts->AddFontFromMemoryCompressedTTF(RobotoMono_Medium_compressed_data, RobotoMono_Medium_compressed_size, 14.0f, &fontConfig);

    // Setup Dear ImGui style
    ImGui::StyleColorsDark();

    // Setup Platform/Renderer backends
    ImGui_ImplGlfw_InitForOther(window, true);
    ImGui_ImplMetal_Init(device);

    ImVec4 clear_color = ImVec4(0.1f, 0.1f, 0.1f, 1.0f);

    // Main loop
    while (!glfwWindowShouldClose(window))
    {
        @autoreleasepool
        {
            glfwPollEvents();

            // Process any pending events from backend
            model.processEvents();

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

            // Render browser UI
            ui.render(win_width, win_height);

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
    ImGui_ImplMetal_Shutdown();
    ImGui_ImplGlfw_Shutdown();
    ImGui::DestroyContext();

    glfwDestroyWindow(window);
    glfwTerminate();

    return 0;
}
