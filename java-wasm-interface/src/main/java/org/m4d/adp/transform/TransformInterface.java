package org.m4d.adp.transform;


import org.m4d.adp.WasmInterface;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.scijava.nativelib.NativeLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.types.pojo.Field;


import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.memory.AllocationManager;
import org.m4d.adp.allocator.WasmAllocationManager;
import org.m4d.adp.allocator.AllocatorInterface;
import org.m4d.adp.allocator.WasmAllocationFactory;
import java.nio.channels.WritableByteChannel;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import org.apache.arrow.memory.util.MemoryUtil;



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


    // public static native long convertVSR2FFI(VectorSchemaRoot vsr, long FFIArrowSchemaPtr); // returns a pointer to a tuple of (FFI_ArrowArray, FFI_ArrowSchema)
    public static native long convertVSR2FFI(VectorSchemaRoot vsr, long instance_ptr);
    public static native long getFFIArrowArray(long FFIPtr); // gets a pointer to FFI tuple, returns a pointer to FFI_ArrowArray
    public static native long getFFIArrowSchema(long FFIPtr); // gets a pointer to FFI tuple, returns a pointer to FFI_ArrowSchema
    public static native VectorSchemaRoot createOrUpdateVSR(long FFIPtr, VectorSchemaRoot vsr, long instance_ptr); // gets a pointer to FFI tuple and a vsr for ouput, creates vsr if the input is empty else update it
    public static native void dropFFI(long FFIPtr);
    public static native void dropFFIArrowArray(long FFIArrowArrayPtr);
    public static native void dropFFIArrowSchema(long FFIArrowSchemaPtr);

    public static native void writeByteBuffer(byte[] bytes_array, long instance_ptr, long allocatedAddress, long size);
    public static native long GetFirstElemOfTuple(long tuple_ptr);
    public static native long GetSecondElemOfTuple(long tuple_ptr);

    public static native long transformation(long FFIPtr, long instance_ptr, BigIntVector vec); // gets a pointer to FFI tuple, returns a pointer to FFI tuple after transformation
    public static native long transformationIPC(long instance_ptr, long wasm_mem_address, long allocatedAddress, long size);

    public static VectorSchemaRoot createDummyVSR(BufferAllocator allocator) {
        IntVector a = new IntVector("a", allocator);
        IntVector b = new IntVector("b", allocator);
        IntVector c = new IntVector("c", allocator);
        IntVector d = new IntVector("d", allocator);

        for (int j = 0; j < 5; j++) {
            a.setSafe(j, j+1);
            b.setSafe(j, j+1);
            c.setSafe(j, j+1);
            d.setSafe(j, j+1);
        }
        a.setValueCount(5);
        b.setValueCount(5);
        c.setValueCount(5);
        d.setValueCount(5);
        List<Field> fields = Arrays.asList(a.getField(), b.getField(), c.getField(), d.getField());
        List<FieldVector> vectors = Arrays.asList(a, b, c, d);
        VectorSchemaRoot vsr = new VectorSchemaRoot(fields, vectors);
        System.out.println("row count: " + vsr.getRowCount());
        return vsr;
    }

    public static void tmpFunc(String st) {
        System.out.println("tmpFunc****");
    }

    private static BigIntVector getConstIntVector(BufferAllocator allocator) {
        BigIntVector c = new BigIntVector("a", allocator);
        for (int j = 0; j < 5; j++) {
            c.setSafe(j, 7);
        }
        c.setValueCount(5);
        return c;
    }

    private static BufferAllocator createWasmAllocator(AllocationManager.Factory factory) {
        return new RootAllocator(RootAllocator.configBuilder().allocationManagerFactory(factory)
            .build());
    }

    public static void main(String[] args) throws IOException {
        WasmAllocationFactory wasmAllocationFactory = new WasmAllocationFactory();
        final BufferAllocator allocator = createWasmAllocator(wasmAllocationFactory);
        long instance_ptr = wasmAllocationFactory.instancePtr;
        // final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        VectorSchemaRoot vsr_src = createDummyVSR(allocator);
        System.out.println("vsr_src content: \n" + vsr_src.contentToTSVString());
        Schema schema =  vsr_src.getSchema();

        /////IPC/////
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        // ArrowFileWriter writer = new ArrowFileWriter(vsr_src, null, Channels.newChannel(out));
        ArrowStreamWriter writer = new ArrowStreamWriter(vsr_src, null, Channels.newChannel(out));
        writer.start();
        // write the first batch
        writer.writeBatch();
        writer.end();
        byte[] bytes_array = out.toByteArray();
        long size = bytes_array.length;
        for(int i = 0; i < size; i++){
            System.out.print(bytes_array[i] + " ,");
        }
        System.out.println("transform IPC " + bytes_array[0] + "size " + size);

        long allocatedAddress = AllocatorInterface.wasmAlloc(instance_ptr, size);
        long wasm_mem_address = AllocatorInterface.wasmMemPtr(instance_ptr);
        ByteBuffer buffer = MemoryUtil.directBuffer(allocatedAddress + wasm_mem_address, (int) size);
        buffer.put(bytes_array);
        // TransformInterface.writeByteBuffer(bytes_array, instance_ptr, allocatedAddress, size);
        // writer.close();
        long transformed_bytes_tuple = transformationIPC(instance_ptr, wasm_mem_address, allocatedAddress, size);
        AllocatorInterface.wasmDealloc(instance_ptr, allocatedAddress, size);
        long transformed_bytes_address = GetFirstElemOfTuple(transformed_bytes_tuple);
        long transformed_bytes_len = GetSecondElemOfTuple(transformed_bytes_tuple);
        ByteBuffer transformed_buffer = MemoryUtil.directBuffer(transformed_bytes_address, (int) transformed_bytes_len);
        byte[] transformed_bytes = new byte[(int) transformed_bytes_len];
        transformed_buffer.get(transformed_bytes);
        for(int i = 0; i <  (int) transformed_bytes_len; i++){
            System.out.print(transformed_bytes[i] + " ,");
        }
        System.out.println("");
        ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(transformed_bytes),
                            allocator);
            VectorSchemaRoot readBatch = reader.getVectorSchemaRoot();
            reader.loadNextBatch();
            System.out.println("transformed batch\n" + readBatch.contentToTSVString());
        /////IPC/////



        // BigIntVector vec = getConstIntVector(allocator);
        // long addr_vec = vec.getDataBufferAddress();
        // // long addr_vec = vsr_src.getVector(0).getDataBufferAddress();
        // System.out.println("addr vec = " + addr_vec + " vec " + vsr_src.getVector("d"));
        // VectorSchemaRoot vsr_target = VectorSchemaRoot.create(schema, allocator);
        // long ffi_ptr = TransformInterface.convertVSR2FFI(vsr_src, instance_ptr);
        // System.out.println("after conver vsr to ffi");
        
        // // long ffi_transformed_ptr = transformation(addr_vec, instance_ptr, vec);
        // long ffi_transformed_ptr = transformation(ffi_ptr, instance_ptr, vec);
        // System.out.println("after transform");
        // VectorSchemaRoot vsr_out = createOrUpdateVSR(ffi_transformed_ptr, vsr_target, instance_ptr);
        // System.out.println("after conver ffi to vsr");
        // System.out.println("vsr_target content: \n" + vsr_out.contentToTSVString());
    }


} 