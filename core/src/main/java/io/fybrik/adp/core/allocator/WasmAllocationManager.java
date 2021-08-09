package io.fybrik.adp.core.allocator;

import org.apache.arrow.memory.AllocationManager;
import org.apache.arrow.memory.BufferAllocator;

import io.fybrik.adp.core.jni.JniWrapper;

/**
 * Allocation manager that uses allocation and deallocation functions from a
 * WASM module.
 */
public final class WasmAllocationManager extends AllocationManager {

    private final long allocatedSize;

    private final long allocatedAddress;

    private final long instancePtr;

    /**
     * A constructor of WasmAllocationManager that allocated a memory chunk with the
     * requested size in a WASM module memory using allocation function of that
     * module.
     */
    WasmAllocationManager(long instancePtr, BufferAllocator accountingAllocator, long requestedSize) {
        super(accountingAllocator);
        this.instancePtr = instancePtr;
        this.allocatedAddress = JniWrapper.get().wasmAlloc(instancePtr, requestedSize);
        this.allocatedSize = requestedSize;
    }

    /**
     * Release the memory chunk that is managed by this WasmAllocationManager.
     */
    @Override
    protected void release0() {
        JniWrapper.get().wasmDealloc(instancePtr, allocatedAddress, allocatedSize);
    }

    /**
     * Get the size of the memory chunk that is managed by this
     * WasmAllocationManager.
     */
    @Override
    public long getSize() {
        return allocatedSize;
    }

    /**
     * Get the address pointing to the first byte of the memory chunk that is
     * managed by this WasmAllocationManager.
     */
    @Override
    protected long memoryAddress() {
        long wasmMemoryAddress = JniWrapper.get().wasmMemPtr(instancePtr);
        return allocatedAddress + wasmMemoryAddress;
    }
}