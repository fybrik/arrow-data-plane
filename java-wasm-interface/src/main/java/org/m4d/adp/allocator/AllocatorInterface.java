package org.m4d.adp.allocator;


import java.nio.file.Path;
import java.nio.file.Paths;
import org.m4d.adp.WasmInterface;
import java.io.IOException;
import org.scijava.nativelib.NativeLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AllocatorInterface is used to define functions which are implemented in a JNI library.
 * These functions are used by WasmAllocationManager in order to manage memory allocations using a WASM module.
 */
public class AllocatorInterface {

    private static Logger logger = LoggerFactory.getLogger(AllocatorInterface.class);

    static{
        if (!WasmInterface.LOADED_EMBEDDED_LIBRARY) {
            try {
                NativeLoader.loadLibrary("wasm_interface");
            } catch (IOException e) {
                logger.error("AllocateInterface failed to load wasm_interface library: " + e.getMessage());
            }
        }
    }

    public static native long wasmInstance(String path);
    public static native void wasmDrop(long instancePtr);
    public static native long wasmAlloc(long instancePtr, long size);
    public static native long wasmMemPtr(long instancePtr);
    public static native void wasmDealloc(long instancePtr, long offset, long size);
}