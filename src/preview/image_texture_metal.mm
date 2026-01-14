// Metal texture creation for macOS
// This file is only compiled on macOS

#import <Metal/Metal.h>
#include "loguru.hpp"

// Get the default Metal device (singleton, created on first use)
static id<MTLDevice> GetMetalDevice() {
    static id<MTLDevice> device = nil;
    if (device == nil) {
        device = MTLCreateSystemDefaultDevice();
        if (device == nil) {
            LOG_F(ERROR, "Failed to create Metal device");
        }
    }
    return device;
}

extern "C" bool CreateGPUTexture(unsigned char* pixels, int width, int height, void** outTexture) {
    id<MTLDevice> device = GetMetalDevice();
    if (device == nil) {
        LOG_F(ERROR, "CreateGPUTexture: No Metal device available");
        return false;
    }

    // Create texture descriptor
    MTLTextureDescriptor* descriptor = [MTLTextureDescriptor
        texture2DDescriptorWithPixelFormat:MTLPixelFormatRGBA8Unorm
        width:width
        height:height
        mipmapped:NO];

    descriptor.usage = MTLTextureUsageShaderRead;
    descriptor.storageMode = MTLStorageModeManaged;

    // Create the texture
    id<MTLTexture> texture = [device newTextureWithDescriptor:descriptor];
    if (texture == nil) {
        LOG_F(ERROR, "CreateGPUTexture: Failed to create Metal texture");
        return false;
    }

    // Upload pixel data
    MTLRegion region = MTLRegionMake2D(0, 0, width, height);
    NSUInteger bytesPerRow = 4 * width;
    [texture replaceRegion:region
               mipmapLevel:0
                 withBytes:pixels
               bytesPerRow:bytesPerRow];

    LOG_F(INFO, "Created Metal texture: %dx%d", width, height);

    // Return the texture as a void pointer
    // We use CFBridgingRetain to ensure the texture is retained
    *outTexture = (void*)CFBridgingRetain(texture);
    return true;
}

extern "C" void DestroyGPUTexture(void* texture) {
    if (texture != nullptr) {
        // Release the texture using CFBridgingRelease
        id<MTLTexture> mtlTexture = (id<MTLTexture>)CFBridgingRelease(texture);
        LOG_F(INFO, "Destroyed Metal texture");
        (void)mtlTexture; // Silence unused variable warning
    }
}
