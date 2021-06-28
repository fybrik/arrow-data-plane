package org.m4d.adp.allocator;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.scijava.nativelib.NativeLoader;
import org.wasmer.Instance;
import org.wasmer.Memory;


public class AllocatorInterface {

  static {
    Path p = Paths.get("../wasm_interface/target/release/libwasm_interface.so");
    System.load(p.toAbsolutePath().toString());
  }

  public static native long wasmInstance(String path);
  public static native void wasmDrop(long instancePtr);
  public static native long wasmAlloc(long instancePtr, long size);
  public static native long wasmMemPtr(long instancePtr);
  public static native void wasmDealloc(long instancePtr, long offset, long size);
  public static native void setModules(String[] paths);
}