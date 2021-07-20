package com.ibm.arrowconverter;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Arrays;
import java.util.List;

class ArrowJNIAdapter {
    // Returns a pointer to a Rust CDataInterface object, based on the scheme provided
    private static native void Convert2CdataInterface(VectorSchemaRoot vsr_src, VectorSchemaRoot vsr_target);

    static {
        // This actually loads the shared object that we'll be creating.
        // The actual location of the .so or .dll may differ based on your
        // platform.
        System.loadLibrary("mylib");
    }

    public static VectorSchemaRoot createDummyVSR(BufferAllocator allocator) {
        IntVector a = new IntVector("a", allocator);
        IntVector b = new IntVector("b", allocator);
        IntVector c = new IntVector("c", allocator);
        IntVector d = new IntVector("d", allocator);

        for (int j = 0; j < 5; j++) {
            a.setSafe(j, j);
            b.setSafe(j, j);
            c.setSafe(j, j);
            d.setSafe(j, j);
        }
        a.setValueCount(5);
        b.setValueCount(5);
        c.setValueCount(5);
        d.setValueCount(5);
        List<Field> fields = Arrays.asList(a.getField(), b.getField(), c.getField(), d.getField());
        List<FieldVector> vectors = Arrays.asList(a, b, c, d);
        VectorSchemaRoot vsr = new VectorSchemaRoot(fields, vectors);
        System.out.println("row count: " + vsr.getRowCount());
        return vsr;
    }

    public static void main(String[] args) {
        final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        VectorSchemaRoot vsr_src = createDummyVSR(allocator);
        Schema schema =  vsr_src.getSchema();
        VectorSchemaRoot vsr_target = VectorSchemaRoot.create(schema, allocator);
        ArrowJNIAdapter.Convert2CdataInterface(vsr_src, vsr_target);
        System.out.println("vsr_target content: \n" + vsr_target.contentToTSVString());
    }
}