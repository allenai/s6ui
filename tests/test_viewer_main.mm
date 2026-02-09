// Standalone test harness for MmapTextViewer
// Usage: ./test_viewer [--size <MB>] [filepath]

#import <Metal/Metal.h>
#import <QuartzCore/QuartzCore.h>

#define GLFW_INCLUDE_NONE
#define GLFW_EXPOSE_NATIVE_COCOA
#include <GLFW/glfw3.h>
#include <GLFW/glfw3native.h>

#include "imgui/imgui.h"
#include "imgui/imgui_impl_glfw.h"
#include "imgui/imgui_impl_metal.h"

#include "preview/mmap_text_viewer.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <random>
#include <fstream>

static void glfw_error_callback(int error, const char* description) {
    fprintf(stderr, "GLFW Error %d: %s\n", error, description);
}

static std::string generateTestFile(int sizeMB) {
    std::string path = "/tmp/mmap_viewer_test.txt";
    fprintf(stderr, "Generating %d MB test file at %s ...\n", sizeMB, path.c_str());

    std::ofstream out(path, std::ios::binary);
    if (!out) {
        fprintf(stderr, "Failed to create test file\n");
        return "";
    }

    std::mt19937 rng(42);
    std::uniform_int_distribution<int> lineLenDist(20, 200);
    std::uniform_int_distribution<int> charDist(32, 126); // printable ASCII
    std::uniform_int_distribution<int> wordLenDist(1, 15);

    uint64_t targetBytes = static_cast<uint64_t>(sizeMB) * 1024ULL * 1024ULL;
    uint64_t written = 0;
    uint64_t lineNum = 0;

    while (written < targetBytes) {
        int lineLen;
        // Every ~10000 lines, generate a long line (1MB+)
        if (lineNum > 0 && lineNum % 10000 == 0) {
            lineLen = 1024 * 1024 + static_cast<int>(rng() % (512 * 1024));
        } else {
            lineLen = lineLenDist(rng);
        }

        std::string line;
        line.reserve(lineLen + 1);
        int col = 0;
        while (col < lineLen) {
            int wl = wordLenDist(rng);
            for (int j = 0; j < wl && col < lineLen; ++j, ++col) {
                line += static_cast<char>(charDist(rng));
            }
            if (col < lineLen) {
                line += ' ';
                col++;
            }
        }
        line += '\n';
        out.write(line.data(), static_cast<std::streamsize>(line.size()));
        written += line.size();
        lineNum++;
    }

    out.close();
    fprintf(stderr, "Generated %llu bytes, %llu lines\n",
            static_cast<unsigned long long>(written),
            static_cast<unsigned long long>(lineNum));
    return path;
}

int main(int argc, char* argv[]) {
    int sizeMB = 100;
    std::string filePath;
    bool generatedFile = false;

    // Parse args
    for (int i = 1; i < argc; ++i) {
        if (strcmp(argv[i], "--size") == 0 && i + 1 < argc) {
            sizeMB = atoi(argv[++i]);
        } else {
            filePath = argv[i];
        }
    }

    // Generate file if none provided
    if (filePath.empty()) {
        filePath = generateTestFile(sizeMB);
        if (filePath.empty())
            return 1;
        generatedFile = true;
    }

    // Setup GLFW
    glfwSetErrorCallback(glfw_error_callback);
    if (!glfwInit())
        return 1;

    glfwWindowHint(GLFW_CLIENT_API, GLFW_NO_API);
    GLFWwindow* window = glfwCreateWindow(1200, 800, "MmapTextViewer Test", nullptr, nullptr);
    if (!window) {
        glfwTerminate();
        return 1;
    }

    // Setup Metal
    id<MTLDevice> device = MTLCreateSystemDefaultDevice();
    id<MTLCommandQueue> commandQueue = [device newCommandQueue];

    NSWindow* nswin = glfwGetCocoaWindow(window);
    CAMetalLayer* layer = [CAMetalLayer layer];
    layer.device = device;
    layer.pixelFormat = MTLPixelFormatBGRA8Unorm;
    nswin.contentView.layer = layer;
    nswin.contentView.wantsLayer = YES;

    // Setup ImGui
    IMGUI_CHECKVERSION();
    ImGui::CreateContext();
    ImGuiIO& io = ImGui::GetIO(); (void)io;
    io.ConfigFlags |= ImGuiConfigFlags_NavEnableKeyboard;
    ImGui::StyleColorsDark();
    ImGui_ImplGlfw_InitForOther(window, true);
    ImGui_ImplMetal_Init(device);

    // Open viewer
    MmapTextViewer viewer;
    if (!viewer.open(filePath)) {
        fprintf(stderr, "Failed to open file: %s\n", filePath.c_str());
        return 1;
    }

    bool wordWrap = false;
    ImVec4 clearColor = ImVec4(0.1f, 0.1f, 0.1f, 1.0f);
    bool hadActivity = true;

    // Main loop
    while (!glfwWindowShouldClose(window)) {
        @autoreleasepool {
            double timeout = hadActivity ? 0.016 : 0.5;
            glfwWaitEventsTimeout(timeout);

            int fb_width, fb_height;
            glfwGetFramebufferSize(window, &fb_width, &fb_height);
            layer.drawableSize = CGSizeMake(fb_width, fb_height);

            int win_width, win_height;
            glfwGetWindowSize(window, &win_width, &win_height);

            id<CAMetalDrawable> drawable = [layer nextDrawable];
            if (!drawable)
                continue;

            id<MTLCommandBuffer> commandBuffer = [commandQueue commandBuffer];

            MTLRenderPassDescriptor* rpd = [MTLRenderPassDescriptor new];
            rpd.colorAttachments[0].texture = drawable.texture;
            rpd.colorAttachments[0].loadAction = MTLLoadActionClear;
            rpd.colorAttachments[0].storeAction = MTLStoreActionStore;
            rpd.colorAttachments[0].clearColor = MTLClearColorMake(
                clearColor.x, clearColor.y, clearColor.z, clearColor.w);

            ImGui_ImplMetal_NewFrame(rpd);
            ImGui_ImplGlfw_NewFrame();
            ImGui::NewFrame();

            // Fullscreen window
            ImGui::SetNextWindowPos(ImVec2(0, 0));
            ImGui::SetNextWindowSize(ImVec2(static_cast<float>(win_width), static_cast<float>(win_height)));
            ImGui::Begin("Viewer", nullptr,
                         ImGuiWindowFlags_NoTitleBar | ImGuiWindowFlags_NoResize |
                         ImGuiWindowFlags_NoMove | ImGuiWindowFlags_NoCollapse |
                         ImGuiWindowFlags_NoBringToFrontOnFocus |
                         ImGuiWindowFlags_NoScrollbar | ImGuiWindowFlags_NoScrollWithMouse);

            // Toolbar
            ImGui::Text("%s", filePath.c_str());
            ImGui::SameLine();

            // File size
            uint64_t fs = viewer.fileSize();
            if (fs >= 1024ULL * 1024ULL * 1024ULL)
                ImGui::Text("(%.2f GB)", static_cast<double>(fs) / (1024.0 * 1024.0 * 1024.0));
            else if (fs >= 1024ULL * 1024ULL)
                ImGui::Text("(%.1f MB)", static_cast<double>(fs) / (1024.0 * 1024.0));
            else
                ImGui::Text("(%llu bytes)", static_cast<unsigned long long>(fs));

            ImGui::SameLine();
            ImGui::Text("| %llu lines", static_cast<unsigned long long>(viewer.lineCount()));

            ImGui::SameLine();
            if (ImGui::Checkbox("Word Wrap", &wordWrap)) {
                viewer.setWordWrap(wordWrap);
            }

            ImGui::Separator();

            // Viewer fills remaining space
            float viewerHeight = static_cast<float>(win_height) - ImGui::GetCursorPosY() - 4.0f;
            float viewerWidth = static_cast<float>(win_width) - 16.0f;
            viewer.render(viewerWidth, viewerHeight);

            ImGui::End();

            // Rendering
            ImGui::Render();
            ImDrawData* drawData = ImGui::GetDrawData();

            id<MTLRenderCommandEncoder> enc = [commandBuffer renderCommandEncoderWithDescriptor:rpd];
            [enc pushDebugGroup:@"ImGui"];
            ImGui_ImplMetal_RenderDrawData(drawData, commandBuffer, enc);
            [enc popDebugGroup];
            [enc endEncoding];

            [commandBuffer presentDrawable:drawable];
            [commandBuffer commit];

            hadActivity = io.MouseDelta.x != 0 || io.MouseDelta.y != 0 ||
                          io.MouseClicked[0] || io.MouseReleased[0] ||
                          io.MouseWheel != 0.0f ||
                          ImGui::IsKeyDown(ImGuiKey_UpArrow) || ImGui::IsKeyDown(ImGuiKey_DownArrow) ||
                          ImGui::IsKeyDown(ImGuiKey_PageUp) || ImGui::IsKeyDown(ImGuiKey_PageDown);
        }
    }

    // Cleanup
    viewer.close();

    ImGui_ImplMetal_Shutdown();
    ImGui_ImplGlfw_Shutdown();
    ImGui::DestroyContext();

    glfwDestroyWindow(window);
    glfwTerminate();

    if (generatedFile) {
        remove(filePath.c_str());
        fprintf(stderr, "Cleaned up generated file: %s\n", filePath.c_str());
    }

    return 0;
}
