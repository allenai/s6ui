# S3 Browser - Dear ImGui + GLFW + Metal/OpenGL + libcurl

# Detect OS
UNAME_S := $(shell uname -s)

# Enable parallel builds by default
ifeq ($(UNAME_S), Darwin)
	MAKEFLAGS += -j$(shell sysctl -n hw.ncpu 2>/dev/null || echo 4)
else
	MAKEFLAGS += -j$(shell nproc 2>/dev/null || echo 4)
endif

CC = clang
CXX = clang++
OBJCXX = clang++

# Detect Homebrew prefix (Apple Silicon vs Intel) on macOS
ifeq ($(UNAME_S), Darwin)
	HOMEBREW_PREFIX := $(shell brew --prefix 2>/dev/null || echo /usr/local)
else
	HOMEBREW_PREFIX := /usr/local
endif

# Directories
LIBS_DIR = libs
SRC_DIR = src
AWS_DIR = $(SRC_DIR)/aws
BUILD_DIR = build

# Compiler flags for our source code
CXXFLAGS = -std=c++17 -Wall -Wextra -O2
CXXFLAGS += -I$(HOMEBREW_PREFIX)/include
CXXFLAGS += -Iinclude
CXXFLAGS += -I$(LIBS_DIR)
CXXFLAGS += -I$(LIBS_DIR)/imgui
CXXFLAGS += -I$(LIBS_DIR)/loguru
CXXFLAGS += -I$(LIBS_DIR)/zstd
CXXFLAGS += -I$(LIBS_DIR)/stb
CXXFLAGS += -I$(SRC_DIR)

# Compiler flags for third-party libraries (suppress warnings)
LIBS_CXXFLAGS = -std=c++17 -O2 -w
LIBS_CXXFLAGS += -I$(HOMEBREW_PREFIX)/include
LIBS_CXXFLAGS += -I$(LIBS_DIR)
LIBS_CXXFLAGS += -I$(LIBS_DIR)/imgui
LIBS_CXXFLAGS += -I$(LIBS_DIR)/loguru
LIBS_CXXFLAGS += -I$(LIBS_DIR)/zstd
LIBS_CXXFLAGS += -I$(LIBS_DIR)/stb
LIBS_CXXFLAGS += -I$(SRC_DIR)

# C compiler flags for third-party C libraries (suppress warnings)
LIBS_CFLAGS = -O2 -w
LIBS_CFLAGS += -I$(LIBS_DIR)/zstd
LIBS_CFLAGS += -I$(LIBS_DIR)/stb

# Objective-C++ flags (only on macOS)
ifeq ($(UNAME_S), Darwin)
	OBJCXXFLAGS = $(CXXFLAGS) -fobjc-arc
	LIBS_OBJCXXFLAGS = $(LIBS_CXXFLAGS) -fobjc-arc
else
	OBJCXXFLAGS = $(CXXFLAGS)
	LIBS_OBJCXXFLAGS = $(LIBS_CXXFLAGS)
endif

# Linker flags
LDFLAGS = -L$(HOMEBREW_PREFIX)/lib
LDFLAGS += -lglfw
LDFLAGS += -lcurl
LDFLAGS += -lz
LDFLAGS += -lssl
LDFLAGS += -lcrypto

# Platform-specific linker flags
ifeq ($(UNAME_S), Darwin)
	LDFLAGS += -framework Metal
	LDFLAGS += -framework MetalKit
	LDFLAGS += -framework Cocoa
	LDFLAGS += -framework IOKit
	LDFLAGS += -framework CoreVideo
	LDFLAGS += -framework QuartzCore
	LDFLAGS += -framework Security
	LDFLAGS += -lncurses
else
	LDFLAGS += -lGL
	LDFLAGS += -ldl
	LDFLAGS += -lpthread
	LDFLAGS += -lncursesw
endif

# Source files
IMGUI_DIR = $(LIBS_DIR)/imgui
IMGUI_SOURCES = $(IMGUI_DIR)/imgui.cpp \
                $(IMGUI_DIR)/imgui_demo.cpp \
                $(IMGUI_DIR)/imgui_draw.cpp \
                $(IMGUI_DIR)/imgui_tables.cpp \
                $(IMGUI_DIR)/imgui_widgets.cpp \
                $(IMGUI_DIR)/imgui_impl_glfw.cpp

# Platform-specific ImGui backend
ifeq ($(UNAME_S), Darwin)
	IMGUI_METAL_SOURCES = $(IMGUI_DIR)/imgui_impl_metal.mm
	IMGUI_OPENGL_SOURCES =
else
	IMGUI_METAL_SOURCES =
	IMGUI_OPENGL_SOURCES = $(IMGUI_DIR)/imgui_impl_opengl3.cpp
endif

LOGURU_DIR = $(LIBS_DIR)/loguru
LOGURU_SOURCES = $(LOGURU_DIR)/loguru.cpp

TEXTEDIT_DIR = $(LIBS_DIR)/imguicolortextedit
TEXTEDIT_SOURCES = $(TEXTEDIT_DIR)/TextEditor.cpp

ZSTD_DIR = $(LIBS_DIR)/zstd
ZSTD_SOURCES = $(ZSTD_DIR)/zstddeclib.c

STB_DIR = $(LIBS_DIR)/stb
STB_SOURCES = $(STB_DIR)/stb_image_impl.c

AWS_SOURCES = $(AWS_DIR)/aws_credentials.cpp \
              $(AWS_DIR)/aws_signer.cpp \
              $(AWS_DIR)/s3_backend.cpp

PREVIEW_DIR = $(SRC_DIR)/preview
PREVIEW_SOURCES = $(PREVIEW_DIR)/text_preview.cpp \
                  $(PREVIEW_DIR)/jsonl_preview.cpp \
                  $(PREVIEW_DIR)/image_preview.cpp

# Platform-specific image texture sources
ifeq ($(UNAME_S), Darwin)
	IMAGE_TEXTURE_SOURCES = $(PREVIEW_DIR)/image_texture_metal.mm
else
	IMAGE_TEXTURE_SOURCES = $(PREVIEW_DIR)/image_texture_opengl.cpp
endif

TUI_PREVIEW_SOURCES = $(SRC_DIR)/tui_text_preview.cpp \
                      $(SRC_DIR)/tui_jsonl_preview.cpp

APP_SOURCES = $(SRC_DIR)/browser_model.cpp \
              $(SRC_DIR)/browser_ui.cpp \
              $(SRC_DIR)/browser_tui.cpp \
              $(SRC_DIR)/streaming_preview.cpp \
              $(TUI_PREVIEW_SOURCES) \
              $(PREVIEW_SOURCES)

# Platform-specific main file
ifeq ($(UNAME_S), Darwin)
	MAIN_SOURCES = $(SRC_DIR)/main.mm
else
	MAIN_SOURCES = $(SRC_DIR)/main_linux.cpp
endif

# Object files (in build directory)
IMGUI_OBJS = $(patsubst $(LIBS_DIR)/%.cpp,$(BUILD_DIR)/libs/%.o,$(IMGUI_SOURCES))
IMGUI_METAL_OBJS = $(patsubst $(LIBS_DIR)/%.mm,$(BUILD_DIR)/libs/%.o,$(IMGUI_METAL_SOURCES))
IMGUI_OPENGL_OBJS = $(patsubst $(LIBS_DIR)/%.cpp,$(BUILD_DIR)/libs/%.o,$(IMGUI_OPENGL_SOURCES))
LOGURU_OBJS = $(patsubst $(LIBS_DIR)/%.cpp,$(BUILD_DIR)/libs/%.o,$(LOGURU_SOURCES))
TEXTEDIT_OBJS = $(patsubst $(LIBS_DIR)/%.cpp,$(BUILD_DIR)/libs/%.o,$(TEXTEDIT_SOURCES))
ZSTD_OBJS = $(patsubst $(LIBS_DIR)/%.c,$(BUILD_DIR)/libs/%.o,$(ZSTD_SOURCES))
STB_OBJS = $(patsubst $(LIBS_DIR)/%.c,$(BUILD_DIR)/libs/%.o,$(STB_SOURCES))
AWS_OBJS = $(patsubst $(SRC_DIR)/%.cpp,$(BUILD_DIR)/src/%.o,$(AWS_SOURCES))
APP_OBJS = $(patsubst $(SRC_DIR)/%.cpp,$(BUILD_DIR)/src/%.o,$(APP_SOURCES))

# Platform-specific main object files
ifeq ($(UNAME_S), Darwin)
	MAIN_OBJS = $(patsubst $(SRC_DIR)/%.mm,$(BUILD_DIR)/src/%.o,$(MAIN_SOURCES))
	IMAGE_TEXTURE_OBJS = $(patsubst $(SRC_DIR)/%.mm,$(BUILD_DIR)/src/%.o,$(IMAGE_TEXTURE_SOURCES))
else
	MAIN_OBJS = $(patsubst $(SRC_DIR)/%.cpp,$(BUILD_DIR)/src/%.o,$(MAIN_SOURCES))
	IMAGE_TEXTURE_OBJS = $(patsubst $(SRC_DIR)/%.cpp,$(BUILD_DIR)/src/%.o,$(IMAGE_TEXTURE_SOURCES))
endif

ALL_OBJS = $(IMGUI_OBJS) $(IMGUI_METAL_OBJS) $(IMGUI_OPENGL_OBJS) $(LOGURU_OBJS) $(TEXTEDIT_OBJS) $(ZSTD_OBJS) $(STB_OBJS) $(AWS_OBJS) $(APP_OBJS) $(IMAGE_TEXTURE_OBJS) $(MAIN_OBJS)

# Output
TARGET = s6ui

# Rules
all: $(TARGET)

$(TARGET): $(ALL_OBJS)
ifeq ($(UNAME_S), Darwin)
	$(OBJCXX) $(ALL_OBJS) $(LDFLAGS) -o $@
else
	$(CXX) $(ALL_OBJS) $(LDFLAGS) -o $@
endif

# Create build directories
$(BUILD_DIR)/libs/imgui $(BUILD_DIR)/libs/loguru $(BUILD_DIR)/libs/imguicolortextedit $(BUILD_DIR)/src/aws $(BUILD_DIR)/src/preview:
	mkdir -p $@

# Compile libs C++ files (with warnings suppressed)
$(BUILD_DIR)/libs/%.o: $(LIBS_DIR)/%.cpp | $(BUILD_DIR)/libs/imgui $(BUILD_DIR)/libs/loguru $(BUILD_DIR)/libs/imguicolortextedit
	@mkdir -p $(dir $@)
	$(CXX) $(LIBS_CXXFLAGS) -c $< -o $@

# Compile libs Objective-C++ files (with warnings suppressed)
$(BUILD_DIR)/libs/%.o: $(LIBS_DIR)/%.mm | $(BUILD_DIR)/libs/imgui
	@mkdir -p $(dir $@)
	$(OBJCXX) $(LIBS_OBJCXXFLAGS) -c $< -o $@

# Compile libs C files (with warnings suppressed)
$(BUILD_DIR)/libs/%.o: $(LIBS_DIR)/%.c
	@mkdir -p $(dir $@)
	$(CC) $(LIBS_CFLAGS) -c $< -o $@

# Compile src C++ files
$(BUILD_DIR)/src/%.o: $(SRC_DIR)/%.cpp | $(BUILD_DIR)/src/aws
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) -c $< -o $@

# Compile src Objective-C++ files
$(BUILD_DIR)/src/%.o: $(SRC_DIR)/%.mm | $(BUILD_DIR)/src/aws
	@mkdir -p $(dir $@)
	$(OBJCXX) $(OBJCXXFLAGS) -c $< -o $@

clean:
	rm -rf $(BUILD_DIR) $(TARGET)

# Debug build with symbols and no optimization
debug: CXXFLAGS = -std=c++17 -Wall -Wextra -g -O0
debug: CXXFLAGS += -I$(HOMEBREW_PREFIX)/include
debug: CXXFLAGS += -Iinclude
debug: CXXFLAGS += -I$(LIBS_DIR)
debug: CXXFLAGS += -I$(LIBS_DIR)/imgui
debug: CXXFLAGS += -I$(LIBS_DIR)/loguru
debug: CXXFLAGS += -I$(LIBS_DIR)/zstd
debug: CXXFLAGS += -I$(SRC_DIR)
debug: LIBS_CXXFLAGS = -std=c++17 -g -O0 -w
debug: LIBS_CXXFLAGS += -I$(HOMEBREW_PREFIX)/include
debug: LIBS_CXXFLAGS += -I$(LIBS_DIR)
debug: LIBS_CXXFLAGS += -I$(LIBS_DIR)/imgui
debug: LIBS_CXXFLAGS += -I$(LIBS_DIR)/loguru
debug: LIBS_CXXFLAGS += -I$(LIBS_DIR)/zstd
debug: LIBS_CXXFLAGS += -I$(SRC_DIR)
debug: LIBS_CFLAGS = -g -O0 -w
debug: LIBS_CFLAGS += -I$(LIBS_DIR)/zstd
debug: clean $(TARGET)

# Address Sanitizer build (catches memory errors)
asan: CXXFLAGS = -std=c++17 -Wall -Wextra -g -O1 -fsanitize=address -fno-omit-frame-pointer
asan: CXXFLAGS += -I$(HOMEBREW_PREFIX)/include
asan: CXXFLAGS += -Iinclude
asan: CXXFLAGS += -I$(LIBS_DIR)
asan: CXXFLAGS += -I$(LIBS_DIR)/imgui
asan: CXXFLAGS += -I$(LIBS_DIR)/loguru
asan: CXXFLAGS += -I$(LIBS_DIR)/zstd
asan: CXXFLAGS += -I$(SRC_DIR)
asan: LIBS_CXXFLAGS = -std=c++17 -g -O1 -w -fsanitize=address -fno-omit-frame-pointer
asan: LIBS_CXXFLAGS += -I$(HOMEBREW_PREFIX)/include
asan: LIBS_CXXFLAGS += -I$(LIBS_DIR)
asan: LIBS_CXXFLAGS += -I$(LIBS_DIR)/imgui
asan: LIBS_CXXFLAGS += -I$(LIBS_DIR)/loguru
asan: LIBS_CXXFLAGS += -I$(LIBS_DIR)/zstd
asan: LIBS_CXXFLAGS += -I$(SRC_DIR)
asan: LIBS_CFLAGS = -g -O1 -w -fsanitize=address -fno-omit-frame-pointer
asan: LIBS_CFLAGS += -I$(LIBS_DIR)/zstd
asan: LDFLAGS += -fsanitize=address
asan: clean $(TARGET)

# Install dependencies (convenience target)
deps:
	brew install glfw

.PHONY: all clean debug asan deps
