package io.fybrik.adp.core;

import io.fybrik.adp.core.jni.JniWrapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Instance implements AutoCloseable {
    private final long instancePtr;

    public Instance(String modulePath) throws IOException {
        Path path = Paths.get(modulePath);
        byte[] moduleBytes = Files.readAllBytes(path);
        this.instancePtr = JniWrapper.get().newInstance(moduleBytes);
    }

    public Instance(byte[] moduleBytes) {
        this.instancePtr = JniWrapper.get().newInstance(moduleBytes);
    }

    public long getInstancePtr() {
        return this.instancePtr;
    }

    public void close() throws Exception {
        JniWrapper.get().dropInstance(this.instancePtr);
    }
}
