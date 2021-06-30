package org.m4d.adp.allocator;


import org.apache.arrow.memory.AllocationManager;
import org.apache.arrow.memory.BufferAllocator;


/**
 * Allocation manager that uses allocation and deallocation functions from a WASM module.
 */

public final class WasmAllocationManager extends AllocationManager {

    public static final WasmAllocationFactory FACTORY = new WasmAllocationFactory();
        
    private final long allocatedSize;

    private final long allocatedAddress;

    private static long instancePtr;

    private final long wasmMemoryAddress;

    /**
     * A constructor of WasmAllocationManager that allocated a memory chunk with the requested size in a WASM module memory
     * using allocation function of that module.
     */
    WasmAllocationManager(long instancePtr, BufferAllocator accountingAllocator, long requestedSize) {
        super(accountingAllocator);
        WasmAllocationManager.instancePtr = instancePtr;
        wasmMemoryAddress = AllocatorInterface.wasmMemPtr(instancePtr);
        allocatedAddress = AllocatorInterface.wasmAlloc(instancePtr,requestedSize);
        allocatedSize = requestedSize;
    }

    /**
     * Release memory chunk that is managed by this WasmAllocationManager.
     */
    @Override
    protected void release0() {
        AllocatorInterface.wasmDealloc(instancePtr, allocatedAddress, allocatedSize);
    }

    /**
     * Get the size of the memory chunk that is managed by this WasmAllocationManager.
     */
    @Override
    public long getSize() {
        return allocatedSize;
    }

    /**
     * Get the address address pointing to the fist byte of the memory chunk that is managed by this WasmAllocationManager.
     */
    @Override
    protected long memoryAddress() {
        return allocatedAddress + wasmMemoryAddress;
    }
}