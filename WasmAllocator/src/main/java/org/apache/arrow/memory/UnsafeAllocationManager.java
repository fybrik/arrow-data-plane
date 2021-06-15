//package com.example;
package org.apache.arrow.memory;
// import org.apache.arrow.memory.AllocationManager;
// import org.apache.arrow.memory.ArrowBuf;
// import org.apache.arrow.memory.BufferAllocator;
// import org.apache.arrow.memory.ReferenceManager;
import org.apache.arrow.memory.util.MemoryUtil;

/**
 * Allocation manager based on unsafe API.
 */
public final class UnsafeAllocationManager extends AllocationManager {

  private static final ArrowBuf EMPTY = new ArrowBuf(ReferenceManager.NO_OP,
      null,
      0,
      MemoryUtil.UNSAFE.allocateMemory(0)
  );

  public static final AllocationManager.Factory FACTORY = new Factory() {
    @Override
    public AllocationManager create(BufferAllocator accountingAllocator, long size) {
      return new UnsafeAllocationManager(accountingAllocator, size);
    }

    @Override
    public ArrowBuf empty() {
      return EMPTY;
    }
  };

  private final long allocatedSize;

  private final long allocatedAddress;

  UnsafeAllocationManager(BufferAllocator accountingAllocator, long requestedSize) {
    super(accountingAllocator);
    System.out.println( "UnsafeAllocationManager constructor" );
    allocatedAddress = MemoryUtil.UNSAFE.allocateMemory(requestedSize);
    allocatedSize = requestedSize;
  }

  @Override
  public long getSize() {
    return allocatedSize;
  }

  @Override
  protected long memoryAddress() {
    return allocatedAddress;
  }

  @Override
  protected void release0() {
    MemoryUtil.UNSAFE.freeMemory(allocatedAddress);
  }

}

