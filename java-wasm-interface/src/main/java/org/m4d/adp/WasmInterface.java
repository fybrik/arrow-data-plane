package org.m4d.adp;

import java.io.IOException;
import org.scijava.nativelib.NativeLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * WasmInterface is used to load a JNI library. 
 */
public class WasmInterface {

    public static boolean LOADED_EMBEDDED_LIBRARY = false;
    private static Logger logger = LoggerFactory.getLogger(WasmInterface.class);
    
    static {
        try {
            NativeLoader.loadLibrary("wasm_interface");
        } catch (IOException e) {
            logger.error("WasmInterface failed to load wasm_interface library: " + e.getMessage());
        }
        LOADED_EMBEDDED_LIBRARY = true;
    }
}