package org.m4d.adp.allocator;


import org.apache.arrow.memory.AllocationManager;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.ReferenceManager;
import org.apache.arrow.memory.util.MemoryUtil;




public final class WasmAllocationManager extends AllocationManager {

    public static final AllocationManager.Factory FACTORY = new AllocationManager.Factory(){
        long instancePtr = AllocatorInterface.wasmInstance("../wasm-modules/target/wasm32-unknown-unknown/alloc.wasm");
    

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

        public void close(){
          AllocatorInterface.wasmDrop(instancePtr);
        }
    };

    private final long allocatedSize;

    private final long allocatedAddress;

    private static long instancePtr;

    private final long wasmMemoryAddress;

    WasmAllocationManager(long instancePtr, BufferAllocator accountingAllocator, long requestedSize) {
      super(accountingAllocator);
      WasmAllocationManager.instancePtr = instancePtr;
      wasmMemoryAddress = AllocatorInterface.wasmMemPtr(instancePtr);
      allocatedAddress = AllocatorInterface.wasmAlloc(instancePtr,requestedSize);
      allocatedSize = requestedSize;
    }


    @Override
    protected void release0() {
      System.out.println("release WasmAllocator");
      AllocatorInterface.wasmDealloc(instancePtr, allocatedAddress, allocatedSize);
    }

    @Override
    public long getSize() {
      return allocatedSize;
    }
  
    @Override
    protected long memoryAddress() {
      System.out.println("memoryAddress");
      return allocatedAddress + wasmMemoryAddress;
    }
}

