package org.m4d.adp.allocator;

import java.util.ArrayList;
import org.apache.arrow.memory.AllocationManager;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.ReferenceManager;

/**
 * A factory class for creating WasmAllocationManager.
 */
public class WasmAllocationFactory implements AllocationManager.Factory, AutoCloseable {
    // Pointer to an instance of the WASM module that is used to allocate and
    // deallocate memory.
    private long wasmData;

    public WasmAllocationFactory(ArrayList<String> wasmImages) {
        System.out.println("wasmallocationfactory");
        wasmData = AllocatorInterface.wasmTimeData(wasmImages);
    }

    public WasmAllocationFactory() {
        ArrayList<String> wasmImages = new ArrayList<String>();
        wasmImages.add("ghcr.io/the-mesh-for-data/alloc-transform:v1");
        wasmData = AllocatorInterface.wasmTimeData(wasmImages);
    }

    @Override
    public AllocationManager create(BufferAllocator accountingAllocator, long size) {
        return new WasmAllocationManager(wasmData, accountingAllocator, size);
    }

    @Override
    public ArrowBuf empty() {
        long memAddr = AllocatorInterface.wasmMemPtr(wasmData);
        long offset = AllocatorInterface.wasmAlloc(wasmData, 0);
        return new ArrowBuf(ReferenceManager.NO_OP,
                null,
                0,
                memAddr + offset);
    }

    @Override
    public void close() throws Exception {
        AllocatorInterface.wasmDrop(wasmData);
    }

    public long wasmDataPtr() {
        return wasmData;
    }
};