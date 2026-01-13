// S3 File Browser - Dear ImGui + GLFW + OpenGL

#define GLFW_INCLUDE_NONE
#include <GLFW/glfw3.h>

#include "imgui/imgui.h"
#include "imgui/imgui_impl_glfw.h"
#include "imgui/imgui_impl_opengl3.h"

#include "browser_model.h"
#include "browser_ui.h"
#include "aws/s3_backend.h"
#include "loguru.hpp"

#include <cstdio>
#include <cstring>
#include <memory>
#include <vector>

// OpenGL loader - we'll use GLEW or glad; for simplicity using GL directly for now
#include <GL/gl.h>

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

    // Create backend with first profile (if available)
    if (!model.profiles().empty()) {
        auto backend = std::make_unique<S3Backend>(model.profiles()[0]);
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

    // GL 3.0 + GLSL 130
    const char* glsl_version = "#version 130";
    glfwWindowHint(GLFW_CONTEXT_VERSION_MAJOR, 3);
    glfwWindowHint(GLFW_CONTEXT_VERSION_MINOR, 0);

    // Create window with graphics context
    GLFWwindow* window = glfwCreateWindow(1200, 800, "S3 Browser", nullptr, nullptr);
    if (window == nullptr)
        return 1;
    glfwMakeContextCurrent(window);
    glfwSwapInterval(1); // Enable vsync

    // Setup Dear ImGui context
    IMGUI_CHECKVERSION();
    ImGui::CreateContext();
    ImGuiIO& io = ImGui::GetIO(); (void)io;
    io.ConfigFlags |= ImGuiConfigFlags_NavEnableKeyboard;

    // Setup Dear ImGui style
    ImGui::StyleColorsDark();

    // Setup Platform/Renderer backends
    ImGui_ImplGlfw_InitForOpenGL(window, true);
    ImGui_ImplOpenGL3_Init(glsl_version);

    ImVec4 clear_color = ImVec4(0.1f, 0.1f, 0.1f, 1.0f);

    // Main loop
    while (!glfwWindowShouldClose(window))
    {
        glfwPollEvents();

        // Process any pending events from backend
        model.processEvents();

        // Start the Dear ImGui frame
        ImGui_ImplOpenGL3_NewFrame();
        ImGui_ImplGlfw_NewFrame();
        ImGui::NewFrame();

        // Get window size for ImGui (in screen coordinates)
        int win_width, win_height;
        glfwGetWindowSize(window, &win_width, &win_height);

        // Render browser UI
        ui.render(win_width, win_height);

        // Rendering
        ImGui::Render();
        int display_w, display_h;
        glfwGetFramebufferSize(window, &display_w, &display_h);
        glViewport(0, 0, display_w, display_h);
        glClearColor(clear_color.x, clear_color.y, clear_color.z, clear_color.w);
        glClear(GL_COLOR_BUFFER_BIT);
        ImGui_ImplOpenGL3_RenderDrawData(ImGui::GetDrawData());

        glfwSwapBuffers(window);
    }

    // Cleanup
    ImGui_ImplOpenGL3_Shutdown();
    ImGui_ImplGlfw_Shutdown();
    ImGui::DestroyContext();

    glfwDestroyWindow(window);
    glfwTerminate();

    return 0;
}
