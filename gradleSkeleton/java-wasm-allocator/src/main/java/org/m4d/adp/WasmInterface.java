package org.m4d.adp;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * WasmInterface is used to load a JNI library. 
 */
public class WasmInterface {

    public static final boolean LOADED_EMBEDDED_LIBRARY;
    static {
        Path p = Paths.get("../wasm_interface/target/release/libwasm_interface.so");
        System.load(p.toAbsolutePath().toString());
        LOADED_EMBEDDED_LIBRARY = true;
    }
}