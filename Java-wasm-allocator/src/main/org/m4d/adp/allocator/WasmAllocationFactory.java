package org.m4d.adp.allocator;

import org.apache.arrow.memory.AllocationManager;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.ReferenceManager;

public class WasmAllocationFactory implements AllocationManager.Factory, AutoCloseable{
    long instancePtr = AllocatorInterface.wasmInstance("lib/hello.wasm");


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