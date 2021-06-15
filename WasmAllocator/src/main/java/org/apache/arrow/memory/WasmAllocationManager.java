//package com.example;
package org.apache.arrow.memory;
import org.apache.arrow.flatbuf.Null;
// import org.apache.arrow.memory.AllocationManager;
// import org.apache.arrow.memory.ArrowBuf;
// import org.apache.arrow.memory.BufferAllocator;
// import org.apache.arrow.memory.ReferenceManager;
import org.apache.arrow.memory.util.MemoryUtil;

import org.wasmer.Instance;
import org.wasmer.Memory;
import org.wasmer.exports.Function;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import sun.misc.Unsafe;


/**
 * Allocation manager based on unsafe API.
 */
public final class WasmAllocationManager extends AllocationManager {

    public static final AllocationManager.Factory FACTORY = new AllocationManager.Factory() {

        @Override
        public AllocationManager create(BufferAllocator accountingAllocator, long size) {
            try {
                return new WasmAllocationManager(accountingAllocator, size);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return null;
        }
    
        @Override
        public ArrowBuf empty() {
          return EMPTY_BUFFER;
        }
    };

//   public static final AllocationManager.Factory FACTORY = new Factory() {
//     @Override
//     public AllocationManager create(BufferAllocator accountingAllocator, long size) {
//       return new UnsafeAllocationManager(accountingAllocator, size);
//     }

//     @Override
//     public ArrowBuf empty() {
//       return EMPTY;
//     }
//   };
    //static final UnsafeDirectLittleEndian EMPTY = INNER_ALLOCATOR.empty;    
    static ArrowBuf EMPTY_BUFFER
    = new ArrowBuf(ReferenceManager.NO_OP,
      null,
      0,
      //0
      MemoryUtil.UNSAFE.allocateMemory(0)
      //(long) wasmInstance.exports.getFunction("alloc").apply(0)[0]
    );
    // static final ArrowBuf EMPTY_BUFFER = new ArrowBuf(ReferenceManager.NO_OP,
    //   null,
    //   0,
    //   WasmAllocationManager.EMPTY.memoryAddress());

    private final long allocatedSize;

    private final long allocatedAddress;

    private final Instance wasmInstance;

    private final long wasmMemoryAddress;

  WasmAllocationManager(BufferAllocator accountingAllocator, long requestedSize) throws IOException {
    super(accountingAllocator);
    System.out.println( "WasmAllocationManager constructor" );
    byte[] bytes;
    Long memAddr = (long) 0;
    Long bufAddr = (long) 0;

    bytes = Files.readAllBytes(Paths.get("lib/hello.wasm"));
    Instance instance = new Instance(bytes);
    Memory memory = instance.exports.getMemory("memory");
    wasmInstance = instance;

    try {
      Field memPtr = Memory.class.getDeclaredField("memoryPointer");
      memPtr.setAccessible(true);
      Long fieldVal = (Long) memPtr.get(memory);
      memAddr = fieldVal;
      System.out.println("memory address by memoryPointer" + fieldVal);
    } catch (Exception e) {
      e.printStackTrace();
    }
    
    
    
    Function alloc = instance.exports.getFunction("alloc");
    //long allocatedAddress2 = MemoryUtil.UNSAFE.allocateMemory(requestedSize);
    //MemoryUtil.UNSAFE.setMemory(allocatedAddress2, 32, (byte) 0);
    Integer offset = (Integer) alloc.apply(requestedSize)[0];
    ByteBuffer memoryBuffer = memory.buffer();
    //memory.grow(1);
    //System.out.println("setMemory1, " + memoryBuffer);
    
    //allocatedAddress = Long.valueOf(offset) + memAddr;
    //System.out.println(allocatedAddress);
    System.out.println("offset = " + (offset));
    byte[] subject = "Wasmer".getBytes(StandardCharsets.UTF_8);
    //MemoryUtil.UNSAFE.setMemory(bufAddr+offset, 1, subject[0]);
    //System.out.println("setMemory2   :" + MemoryUtil.UNSAFE.getByte(bufAddr+offset));
    
    
    //ByteBuffer memoryBuffer = memory.buffer();
    
    memoryBuffer.position(offset);
    //System.out.println("memoryBuf = " + memoryBuffer);
    
    try {
      Field bufAddrField = Buffer.class.getDeclaredField("address");
      bufAddrField.setAccessible(true);
      Long bufAddrVal = (Long) bufAddrField.get(memoryBuffer);
      bufAddr = bufAddrVal;
      System.out.println("address of buffer" + bufAddrVal);
    } catch (Exception e) {
      e.printStackTrace();
    }
    //memoryBuffer.getBufferAddress();
    memoryBuffer.put(subject);
    //MemoryUtil.UNSAFE.setMemory(bufAddr+offset, 32, (byte) 3);
    //System.out.println("memoryBuf = " + memoryBuffer);
    System.out.println("setMemory2   :" + MemoryUtil.UNSAFE.getByte(bufAddr+offset));
    wasmMemoryAddress = bufAddr;
    allocatedAddress = bufAddr+offset;
    System.out.println("allocatedAddress = " + allocatedAddress);
    ByteBuffer memoryBuffer2 = memory.buffer();
    // ByteBuffer memoryBuffer3 = memory.buffer();
    memoryBuffer2.position(offset);
    // memoryBuffer3.position(offset);
    byte[] b = new byte[1];
    // byte[] b2 = new byte[1];
    memoryBuffer2.get(b);
    // memoryBuffer3.get(b2);
    // StringBuilder output = new StringBuilder();
    // output.appendCodePoint(b[0]);
    System.out.println("bytebuffer   :" + "byte = " + b[0]);
    // //System.out.println(alloc.apply(requestedSize)[0]);
    // System.out.println(allocatedAddress);
    
    // System.out.println(Long.toHexString(allocatedAddress));
    allocatedSize = requestedSize;
    System.out.println("allocated size = " + allocatedSize);
    //System.out.println("ArrowBuf" + EMPTY_BUFFER);
    //long tmp = (long) 0;
    EMPTY_BUFFER = new ArrowBuf(ReferenceManager.NO_OP,
      null,
      0,
      //0
      //MemoryUtil.UNSAFE.allocateMemory(0)
      //(Long) ((Integer) wasmInstance.exports.getFunction("alloc").apply(tmp)[0] + memAddr)
      allocatedAddress
      //memAddr+111111
    );
    System.out.println("ArrowBuf" + EMPTY_BUFFER);
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
    System.out.println("release WasmAllocator");
    // byte[] bytes = null;
    // Long memAddr = (long) 0;

    // try {
    //     bytes = Files.readAllBytes(Paths.get("lib/hello.wasm"));
    // } catch (IOException e) {
    //     // TODO Auto-generated catch block
    //     e.printStackTrace();
    // }
    //Instance instance = new Instance(bytes);
    Function dealloc = this.wasmInstance.exports.getFunction("dealloc");
    dealloc.apply(allocatedAddress - wasmMemoryAddress, allocatedSize);
    //MemoryUtil.UNSAFE.freeMemory(allocatedAddress);
  }

}

