package org.m4d.adp.allocator;


import org.apache.arrow.memory.AllocationManager;
import org.apache.arrow.memory.BufferAllocator;



public final class WasmAllocationManager extends AllocationManager {

  public static final WasmAllocationFactory FACTORY = new WasmAllocationFactory();
      
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