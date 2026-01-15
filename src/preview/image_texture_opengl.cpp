// OpenGL texture creation for Linux
// This file is only compiled on Linux

#include <GL/gl.h>
#include "loguru.hpp"

extern "C" bool CreateGPUTexture(unsigned char* pixels, int width, int height, void** outTexture) {
    GLuint textureId;
    glGenTextures(1, &textureId);
    glBindTexture(GL_TEXTURE_2D, textureId);

    // Set texture parameters
    glTexParameteri(GL_TEXTURE_2D, GL_TEXTURE_MIN_FILTER, GL_LINEAR);
    glTexParameteri(GL_TEXTURE_2D, GL_TEXTURE_MAG_FILTER, GL_LINEAR);
    glTexParameteri(GL_TEXTURE_2D, GL_TEXTURE_WRAP_S, GL_CLAMP_TO_EDGE);
    glTexParameteri(GL_TEXTURE_2D, GL_TEXTURE_WRAP_T, GL_CLAMP_TO_EDGE);

    // Upload pixel data
    glTexImage2D(GL_TEXTURE_2D, 0, GL_RGBA, width, height, 0, GL_RGBA, GL_UNSIGNED_BYTE, pixels);

    glBindTexture(GL_TEXTURE_2D, 0);

    LOG_F(INFO, "Created OpenGL texture: %dx%d, id=%u", width, height, textureId);

    // Return the texture ID as a void pointer
    *outTexture = reinterpret_cast<void*>(static_cast<uintptr_t>(textureId));
    return true;
}

extern "C" void DestroyGPUTexture(void* texture) {
    if (texture != nullptr) {
        GLuint textureId = static_cast<GLuint>(reinterpret_cast<uintptr_t>(texture));
        glDeleteTextures(1, &textureId);
        LOG_F(INFO, "Destroyed OpenGL texture: id=%u", textureId);
    }
}
