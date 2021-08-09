package io.fybrik.adp.core.transformer;

import io.fybrik.adp.core.Instance;
import io.fybrik.adp.core.jni.JniWrapper;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.VectorSchemaRoot;

public class WasmTransformer implements Transformer {
    private final BufferAllocator allocator;
    private final long instancePtr;
    private VectorSchemaRoot originalRoot;
    private VectorSchemaRoot transformedRoot;
    private boolean closed;

    public WasmTransformer(BufferAllocator allocator, Instance instance) {
        this.allocator = allocator;
        this.instancePtr = instance.getInstancePtr();
    }

    public void init(VectorSchemaRoot root) {
        Preconditions.checkState(this.originalRoot == null, "init can only be called once");
        this.originalRoot = root;
    }

    public VectorSchemaRoot root() {
        return this.transformedRoot;
    }

    public void next() {
        long base = 0L;
        long arrayPtr = 0L;
        long schemaPtr = 0L;
        long context = JniWrapper.get().prepare(this.instancePtr);
        System.out.printf("XXXXX base %d context %d schema %d array %d%n", base, context, schemaPtr, arrayPtr);
        base = JniWrapper.get().wasmMemPtr(this.instancePtr);
        System.out.printf("XXXXX base %d context %d schema %d array %d%n", base, context, schemaPtr, arrayPtr);
        base = JniWrapper.get().wasmMemPtr(this.instancePtr);
        System.out.printf("XXXXX base %d context %d schema %d array %d%n", base, context, schemaPtr, arrayPtr);
        schemaPtr = JniWrapper.get().getInputSchema(this.instancePtr, context);
        System.out.printf("XXXXX base %d context %d schema %d array %d%n", base, context, schemaPtr, arrayPtr);
        schemaPtr = JniWrapper.get().getInputSchema(this.instancePtr, context);
        System.out.printf("XXXXX base %d context %d schema %d array %d%n", base, context, schemaPtr, arrayPtr);
        arrayPtr = JniWrapper.get().getInputArray(this.instancePtr, context);
        System.out.printf("XXXXX base %d context %d schema %d array %d%n", base, context, schemaPtr, arrayPtr);
        arrayPtr = JniWrapper.get().getInputArray(this.instancePtr, context);
        System.out.printf("XXXXX base %d context %d schema %d array %d%n", base, context, schemaPtr, arrayPtr);
        System.exit(0);
    }

    public void close() throws Exception {
        if (!this.closed) {
            if (this.transformedRoot != null) {
                this.transformedRoot.close();
            }

            this.closed = true;
        }
    }
}
