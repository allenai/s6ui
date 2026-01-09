# Dear ImGui GLFW + Metal Makefile for macOS

CXX = clang++
OBJCXX = clang++

# Detect Homebrew prefix (Apple Silicon vs Intel)
HOMEBREW_PREFIX := $(shell brew --prefix 2>/dev/null || echo /usr/local)

# Compiler flags
CXXFLAGS = -std=c++17 -Wall -Wextra -O2
CXXFLAGS += -I$(HOMEBREW_PREFIX)/include
CXXFLAGS += -I.

# Objective-C++ flags
OBJCXXFLAGS = $(CXXFLAGS) -fobjc-arc

# Linker flags
LDFLAGS = -L$(HOMEBREW_PREFIX)/lib
LDFLAGS += -lglfw
LDFLAGS += -framework Metal
LDFLAGS += -framework MetalKit
LDFLAGS += -framework Cocoa
LDFLAGS += -framework IOKit
LDFLAGS += -framework CoreVideo
LDFLAGS += -framework QuartzCore

# Source files
IMGUI_DIR = imgui
IMGUI_SOURCES = $(IMGUI_DIR)/imgui.cpp \
                $(IMGUI_DIR)/imgui_demo.cpp \
                $(IMGUI_DIR)/imgui_draw.cpp \
                $(IMGUI_DIR)/imgui_tables.cpp \
                $(IMGUI_DIR)/imgui_widgets.cpp \
                $(IMGUI_DIR)/imgui_impl_glfw.cpp

IMGUI_METAL_SOURCES = $(IMGUI_DIR)/imgui_impl_metal.mm

MAIN_SOURCES = main.mm

# Object files
IMGUI_OBJS = $(IMGUI_SOURCES:.cpp=.o)
IMGUI_METAL_OBJS = $(IMGUI_METAL_SOURCES:.mm=.o)
MAIN_OBJS = $(MAIN_SOURCES:.mm=.o)

ALL_OBJS = $(IMGUI_OBJS) $(IMGUI_METAL_OBJS) $(MAIN_OBJS)

# Output
TARGET = imgui_demo

# Rules
all: $(TARGET)

$(TARGET): $(ALL_OBJS)
	$(OBJCXX) $(ALL_OBJS) $(LDFLAGS) -o $@

# Compile C++ files
%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

# Compile Objective-C++ files
%.o: %.mm
	$(OBJCXX) $(OBJCXXFLAGS) -c $< -o $@

clean:
	rm -f $(ALL_OBJS) $(TARGET)

# Install dependencies (convenience target)
deps:
	brew install glfw

.PHONY: all clean deps
