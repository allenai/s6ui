# S3 Browser - Dear ImGui + GLFW + Metal + libcurl

# Enable parallel builds by default
MAKEFLAGS += -j$(shell sysctl -n hw.ncpu 2>/dev/null || echo 4)

CXX = clang++
OBJCXX = clang++

# Detect Homebrew prefix (Apple Silicon vs Intel)
HOMEBREW_PREFIX := $(shell brew --prefix 2>/dev/null || echo /usr/local)

# Directories
LIBS_DIR = libs
SRC_DIR = src
AWS_DIR = $(SRC_DIR)/aws
BUILD_DIR = build

# Compiler flags for our source code
CXXFLAGS = -std=c++17 -Wall -Wextra -O2
CXXFLAGS += -I$(HOMEBREW_PREFIX)/include
CXXFLAGS += -I$(LIBS_DIR)
CXXFLAGS += -I$(LIBS_DIR)/imgui
CXXFLAGS += -I$(LIBS_DIR)/loguru
CXXFLAGS += -I$(SRC_DIR)

# Compiler flags for third-party libraries (suppress warnings)
LIBS_CXXFLAGS = -std=c++17 -O2 -w
LIBS_CXXFLAGS += -I$(HOMEBREW_PREFIX)/include
LIBS_CXXFLAGS += -I$(LIBS_DIR)
LIBS_CXXFLAGS += -I$(LIBS_DIR)/imgui
LIBS_CXXFLAGS += -I$(LIBS_DIR)/loguru
LIBS_CXXFLAGS += -I$(SRC_DIR)

# Objective-C++ flags
OBJCXXFLAGS = $(CXXFLAGS) -fobjc-arc
LIBS_OBJCXXFLAGS = $(LIBS_CXXFLAGS) -fobjc-arc

# Linker flags
LDFLAGS = -L$(HOMEBREW_PREFIX)/lib
LDFLAGS += -lglfw
LDFLAGS += -lcurl
LDFLAGS += -framework Metal
LDFLAGS += -framework MetalKit
LDFLAGS += -framework Cocoa
LDFLAGS += -framework IOKit
LDFLAGS += -framework CoreVideo
LDFLAGS += -framework QuartzCore
LDFLAGS += -framework Security

# Source files
IMGUI_DIR = $(LIBS_DIR)/imgui
IMGUI_SOURCES = $(IMGUI_DIR)/imgui.cpp \
                $(IMGUI_DIR)/imgui_demo.cpp \
                $(IMGUI_DIR)/imgui_draw.cpp \
                $(IMGUI_DIR)/imgui_tables.cpp \
                $(IMGUI_DIR)/imgui_widgets.cpp \
                $(IMGUI_DIR)/imgui_impl_glfw.cpp

IMGUI_METAL_SOURCES = $(IMGUI_DIR)/imgui_impl_metal.mm

LOGURU_DIR = $(LIBS_DIR)/loguru
LOGURU_SOURCES = $(LOGURU_DIR)/loguru.cpp

TEXTEDIT_DIR = $(LIBS_DIR)/imguicolortextedit
TEXTEDIT_SOURCES = $(TEXTEDIT_DIR)/TextEditor.cpp

AWS_SOURCES = $(AWS_DIR)/aws_credentials.cpp \
              $(AWS_DIR)/aws_signer.cpp \
              $(AWS_DIR)/s3_backend.cpp

APP_SOURCES = $(SRC_DIR)/browser_model.cpp \
              $(SRC_DIR)/browser_ui.cpp

MAIN_SOURCES = $(SRC_DIR)/main.mm

# Object files (in build directory)
IMGUI_OBJS = $(patsubst $(LIBS_DIR)/%.cpp,$(BUILD_DIR)/libs/%.o,$(IMGUI_SOURCES))
IMGUI_METAL_OBJS = $(patsubst $(LIBS_DIR)/%.mm,$(BUILD_DIR)/libs/%.o,$(IMGUI_METAL_SOURCES))
LOGURU_OBJS = $(patsubst $(LIBS_DIR)/%.cpp,$(BUILD_DIR)/libs/%.o,$(LOGURU_SOURCES))
TEXTEDIT_OBJS = $(patsubst $(LIBS_DIR)/%.cpp,$(BUILD_DIR)/libs/%.o,$(TEXTEDIT_SOURCES))
AWS_OBJS = $(patsubst $(SRC_DIR)/%.cpp,$(BUILD_DIR)/src/%.o,$(AWS_SOURCES))
APP_OBJS = $(patsubst $(SRC_DIR)/%.cpp,$(BUILD_DIR)/src/%.o,$(APP_SOURCES))
MAIN_OBJS = $(patsubst $(SRC_DIR)/%.mm,$(BUILD_DIR)/src/%.o,$(MAIN_SOURCES))

ALL_OBJS = $(IMGUI_OBJS) $(IMGUI_METAL_OBJS) $(LOGURU_OBJS) $(TEXTEDIT_OBJS) $(AWS_OBJS) $(APP_OBJS) $(MAIN_OBJS)

# Output
TARGET = s3v

# Rules
all: $(TARGET)

$(TARGET): $(ALL_OBJS)
	$(OBJCXX) $(ALL_OBJS) $(LDFLAGS) -o $@

# Create build directories
$(BUILD_DIR)/libs/imgui $(BUILD_DIR)/libs/loguru $(BUILD_DIR)/libs/imguicolortextedit $(BUILD_DIR)/src/aws:
	mkdir -p $@

# Compile libs C++ files (with warnings suppressed)
$(BUILD_DIR)/libs/%.o: $(LIBS_DIR)/%.cpp | $(BUILD_DIR)/libs/imgui $(BUILD_DIR)/libs/loguru $(BUILD_DIR)/libs/imguicolortextedit
	@mkdir -p $(dir $@)
	$(CXX) $(LIBS_CXXFLAGS) -c $< -o $@

# Compile libs Objective-C++ files (with warnings suppressed)
$(BUILD_DIR)/libs/%.o: $(LIBS_DIR)/%.mm | $(BUILD_DIR)/libs/imgui
	@mkdir -p $(dir $@)
	$(OBJCXX) $(LIBS_OBJCXXFLAGS) -c $< -o $@

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
debug: CXXFLAGS += -I$(LIBS_DIR)
debug: CXXFLAGS += -I$(LIBS_DIR)/imgui
debug: CXXFLAGS += -I$(LIBS_DIR)/loguru
debug: CXXFLAGS += -I$(SRC_DIR)
debug: LIBS_CXXFLAGS = -std=c++17 -g -O0 -w
debug: LIBS_CXXFLAGS += -I$(HOMEBREW_PREFIX)/include
debug: LIBS_CXXFLAGS += -I$(LIBS_DIR)
debug: LIBS_CXXFLAGS += -I$(LIBS_DIR)/imgui
debug: LIBS_CXXFLAGS += -I$(LIBS_DIR)/loguru
debug: LIBS_CXXFLAGS += -I$(SRC_DIR)
debug: OBJCXXFLAGS = $(CXXFLAGS) -fobjc-arc
debug: LIBS_OBJCXXFLAGS = $(LIBS_CXXFLAGS) -fobjc-arc
debug: clean $(TARGET)

# Address Sanitizer build (catches memory errors)
asan: CXXFLAGS = -std=c++17 -Wall -Wextra -g -O1 -fsanitize=address -fno-omit-frame-pointer
asan: CXXFLAGS += -I$(HOMEBREW_PREFIX)/include
asan: CXXFLAGS += -I$(LIBS_DIR)
asan: CXXFLAGS += -I$(LIBS_DIR)/imgui
asan: CXXFLAGS += -I$(LIBS_DIR)/loguru
asan: CXXFLAGS += -I$(SRC_DIR)
asan: LIBS_CXXFLAGS = -std=c++17 -g -O1 -w -fsanitize=address -fno-omit-frame-pointer
asan: LIBS_CXXFLAGS += -I$(HOMEBREW_PREFIX)/include
asan: LIBS_CXXFLAGS += -I$(LIBS_DIR)
asan: LIBS_CXXFLAGS += -I$(LIBS_DIR)/imgui
asan: LIBS_CXXFLAGS += -I$(LIBS_DIR)/loguru
asan: LIBS_CXXFLAGS += -I$(SRC_DIR)
asan: OBJCXXFLAGS = $(CXXFLAGS) -fobjc-arc
asan: LIBS_OBJCXXFLAGS = $(LIBS_CXXFLAGS) -fobjc-arc
asan: LDFLAGS += -fsanitize=address
asan: clean $(TARGET)

# Install dependencies (convenience target)
deps:
	brew install glfw

.PHONY: all clean debug asan deps
