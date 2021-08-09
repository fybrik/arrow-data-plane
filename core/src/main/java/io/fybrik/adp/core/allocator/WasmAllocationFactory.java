package io.fybrik.adp.core.allocator;

import java.io.IOException;

import org.apache.arrow.memory.AllocationManager;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.ReferenceManager;
import org.apache.arrow.memory.AllocationManager.Factory;

import io.fybrik.adp.core.Instance;
import io.fybrik.adp.core.jni.JniWrapper;

/**
 * A factory class for creating WasmAllocationManager.
 */
public class WasmAllocationFactory implements Factory {
    private final long instancePtr;

    public WasmAllocationFactory(Instance instance) throws IOException {
        this.instancePtr = instance.getInstancePtr();
    }

    public AllocationManager create(BufferAllocator accountingAllocator, long size) {
        return new WasmAllocationManager(this.instancePtr, accountingAllocator, size);
    }

    public ArrowBuf empty() {
        long memAddr = JniWrapper.get().wasmMemPtr(this.instancePtr);
        long offset = JniWrapper.get().wasmAlloc(this.instancePtr, 0L);
        return new ArrowBuf(ReferenceManager.NO_OP, null, 0, memAddr + offset);
    }
}
