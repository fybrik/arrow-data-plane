package org.m4d.adp.ipc;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

public class IpcExampleStream {
    public static void main(String[] args) throws IOException {
        final BufferAllocator a = new RootAllocator(Long.MAX_VALUE);
        BufferAllocator allocator = a.newChildAllocator("flight-client", 0, Long.MAX_VALUE);

        BitVector bitVector = new BitVector("boolean", allocator);
        VarCharVector varCharVector = new VarCharVector("varchar", allocator);
        for (int i = 0; i < 10; i++) {
            bitVector.setSafe(i, i % 2 == 0 ? 0 : 1);
            varCharVector.setSafe(i, ("test" + i).getBytes(StandardCharsets.UTF_8));
        }
        bitVector.setValueCount(10);
        varCharVector.setValueCount(10);

        List<Field> fields = Arrays.asList(bitVector.getField(), varCharVector.getField());
        List<FieldVector> vectors = Arrays.asList(bitVector, varCharVector);
        VectorSchemaRoot root = new VectorSchemaRoot(fields, vectors);



        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ArrowStreamWriter writer = new ArrowStreamWriter(root, /*DictionaryProvider=*/null, Channels.newChannel(out));

        writer.start();
        // write the first batch
        writer.writeBatch();

        // write another four batches.
        for (int i = 0; i < 4; i++) {
            // populate VectorSchemaRoot data and write the second batch
            //BitVector childVector1 = (BitVector)root.getVector(0);
            //VarCharVector childVector2 = (VarCharVector)root.getVector(1);
            //childVector1.reset();
            //childVector2.reset();
            // ... do some populate work here, could be different for each batch
            writer.writeBatch();
        }

        // end
        writer.end();

        byte[] outByteArray = out.toByteArray();
        byte[] transformedBytes = outByteArray.clone();

        // send outByteArray and transformedBytes to WASM for transformation.
        // we fake it by simply copying the bytes

        try (ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(transformedBytes), allocator)) {
            Schema schema = reader.getVectorSchemaRoot().getSchema();
            for (int i = 0; i < 5; i++) {
                // This will be loaded with new values on every call to loadNextBatch
                VectorSchemaRoot readBatch = reader.getVectorSchemaRoot();
                reader.loadNextBatch();
                System.out.println(readBatch);
                //... do something with readBatch
            }
        }
    }
}
