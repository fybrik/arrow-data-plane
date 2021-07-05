package org.m4d.adp.allocator;

import org.apache.arrow.memory.AllocationManager;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.ReferenceManager;


/**
 * A factory class for creating WasmAllocationManager.
 */
public class WasmAllocationFactory implements AllocationManager.Factory, AutoCloseable{
    // Pointer to an instance of the WASM module that is used to allocate and deallocate memory.
    long instancePtr = AllocatorInterface.wasmInstance("../wasm-modules/allocator/target/wasm32-unknown-unknown/release/alloc.wasm");

    @Override
    public AllocationManager create(BufferAllocator accountingAllocator, long size) {
        return new WasmAllocationManager(instancePtr, accountingAllocator, size);
    }

    @Override
    public ArrowBuf empty() {
      long memAddr = AllocatorInterface.wasmMemPtr(instancePtr);
      long offset = AllocatorInterface.wasmAlloc(instancePtr,0);
      return new ArrowBuf(ReferenceManager.NO_OP,
      null,
      0,
      memAddr+offset
      );
    }

    @Override
    public void close() throws Exception {
        AllocatorInterface.wasmDrop(instancePtr);
        
    }
};