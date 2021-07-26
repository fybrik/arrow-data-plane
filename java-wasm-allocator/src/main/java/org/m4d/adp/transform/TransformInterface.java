package org.m4d.adp.transform;


import org.m4d.adp.WasmInterface;
import java.io.IOException;
import org.scijava.nativelib.NativeLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

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


    public static native long convertVSR2FFI(VectorSchemaRoot vsr, long FFIArrowSchemaPtr); // returns a pointer to a tuple of (FFI_ArrowArray, FFI_ArrowSchema)
    public static native long getFFIArrowArray(long FFIPtr); // gets a pointer to FFI tuple, returns a pointer to FFI_ArrowArray
    public static native long getFFIArrowSchema(long FFIPtr); // gets a pointer to FFI tuple, returns a pointer to FFI_ArrowSchema
    public static native VectorSchemaRoot createOrUpdateVSR(long FFIPtr, VectorSchemaRoot vsr); // gets a pointer to FFI tuple and a vsr for ouput, creates vsr if the input is empty else update it
    public static native void dropFFI(long FFIPtr);
    public static native void dropFFIArrowArray(long FFIArrowArrayPtr);
    public static native void dropFFIArrowSchema(long FFIArrowSchemaPtr);

    public static native long transformation(long FFIPtr); // gets a pointer to FFI tuple, returns a pointer to FFI tuple after transformation
}