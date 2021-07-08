package org.m4d.adp;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.io.IOException;
import org.scijava.nativelib.NativeLoader;

/**
 * WasmInterface is used to load a JNI library. 
 */
public class WasmInterface {

    public static final boolean LOADED_EMBEDDED_LIBRARY;
    static {
        try {
            System.out.println("nativeLoader");
            NativeLoader.loadLibrary("wasm_interface");
        } catch (IOException e) {
            e.printStackTrace();
        }
        LOADED_EMBEDDED_LIBRARY = true;
    }
}