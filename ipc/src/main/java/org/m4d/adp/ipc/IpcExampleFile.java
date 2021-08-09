package org.m4d.adp.ipc;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

public class IpcExampleFile {
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
        ArrowFileWriter writer = new ArrowFileWriter(root, null, Channels.newChannel(out));
        writer.start();
        // write the first batch
        writer.writeBatch();
        // write another four batches.
        for (int i = 0; i < 4; i++) {
            //... do populate work
            writer.writeBatch();
        }
        writer.end();

        try (ArrowFileReader reader = new ArrowFileReader(
                new ByteArrayReadableSeekableByteChannel(out.toByteArray()), allocator)) {

            // read the 4-th batch
            ArrowBlock block = reader.getRecordBlocks().get(3);
            reader.loadRecordBatch(block);
            VectorSchemaRoot readBatch = reader.getVectorSchemaRoot();
            System.out.println(readBatch);
        }
    }
}
