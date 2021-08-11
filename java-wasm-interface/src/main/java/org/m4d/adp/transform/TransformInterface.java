package org.m4d.adp.transform;

import org.m4d.adp.WasmInterface;
import java.io.IOException;
import org.scijava.nativelib.NativeLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;




/**
 * AllocatorInterface is used to define functions which are implemented in a JNI library.
 * These functions are used by WasmAllocationManager in order to manage memory allocations using a WASM module.
 */
public class TransformInterface {

    private static Logger logger = LoggerFactory.getLogger(TransformInterface.class);

    static{
        if (!WasmInterface.LOADED_EMBEDDED_LIBRARY) {
            try {
                NativeLoader.loadLibrary("wasm_interface");
            } catch (IOException e) {
                logger.error("TransformInterface failed to load wasm_interface library: " + e.getMessage());
            }
        }
    }


    
    public static native long GetFirstElemOfTuple(long instance_ptr, long tuple_ptr);
    public static native long GetSecondElemOfTuple(long instance_ptr, long tuple_ptr);
    public static native void DropTuple(long instance_ptr, long tuple_ptr);
    public static native long TransformationIPC(long instance_ptr, long allocatedAddress, long size);

} 