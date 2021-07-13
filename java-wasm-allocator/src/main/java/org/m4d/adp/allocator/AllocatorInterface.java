package org.m4d.adp.allocator;


import java.nio.file.Path;
import java.nio.file.Paths;
import org.m4d.adp.WasmInterface;
import java.io.IOException;
import org.scijava.nativelib.NativeLoader;


/**
 * AllocatorInterface is used to define functions which are implemented in a JNI library.
 * These functions are used by WasmAllocationManager in order to manage memory allocations using a WASM module.
 */
public class AllocatorInterface {

    static{
        if (!WasmInterface.LOADED_EMBEDDED_LIBRARY) {
            try {
                System.out.println("nativeLoader");
                NativeLoader.loadLibrary("wasm_interface");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static native long wasmInstance(String path);
    public static native void wasmDrop(long instancePtr);
    public static native long wasmAlloc(long instancePtr, long size);
    public static native long wasmMemPtr(long instancePtr);
    public static native void wasmDealloc(long instancePtr, long offset, long size);
}