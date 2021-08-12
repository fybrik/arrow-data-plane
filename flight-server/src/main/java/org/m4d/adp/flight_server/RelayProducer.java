package org.m4d.adp.flight_server;

import com.google.common.collect.ImmutableList;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.perf.impl.PerfOuterClass;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.util.MemoryUtil;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.m4d.adp.allocator.AllocatorInterface;
import org.m4d.adp.allocator.WasmAllocationFactory;
import org.m4d.adp.transform.TransformInterface;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;

/**
 * Flight Producer for flight server that serves as relay to another flight
 * server (e.g. the flight server with ExampleProducer). Depending on the
 * transform boolean, this server may transform the first column with a constant
 * column.
 */
public class RelayProducer extends NoOpFlightProducer implements AutoCloseable {
    BufferAllocator allocator;
    private final boolean zeroCopy;
    private final boolean transform;
    private final FlightClient client;
    private final Location location;

    public RelayProducer(Location location, Location remote_location, BufferAllocator allocator, boolean transform,
            boolean zeroCopy) {
        this.transform = transform;
        this.zeroCopy = zeroCopy;
        this.allocator = allocator;
        this.location = location;
        this.client = FlightClient.builder().allocator(allocator).location(remote_location).build();
    }

    private byte[] WASMTransformByteArray(long instance_ptr, byte[] recordBatchByteArray, int size) {
        // Allocate a block in wasm memory and copy the byte array to this block
        long allocatedAddress = AllocatorInterface.wasmAlloc(instance_ptr, size);
        long wasm_mem_address = AllocatorInterface.wasmMemPtr(instance_ptr);
        ByteBuffer buffer = MemoryUtil.directBuffer(allocatedAddress + wasm_mem_address, size);
        buffer.put(recordBatchByteArray, 0, size);

        // Transform the vector schema root that is represented as a byte array in
        // `allocatedAddress` memory address with length `length`
        // The function returns a tuple of `(address, lenght)` of as byte array that
        // represents the transformed vector schema root
        long transformed_bytes_tuple = TransformInterface.TransformationIPC(instance_ptr, allocatedAddress, size);

        // Get the byte array from the memory address
        long transformed_bytes_address = TransformInterface.GetFirstElemOfTuple(instance_ptr, transformed_bytes_tuple);
        long transformed_bytes_len = TransformInterface.GetSecondElemOfTuple(instance_ptr, transformed_bytes_tuple);
        wasm_mem_address = AllocatorInterface.wasmMemPtr(instance_ptr);
        ByteBuffer transformed_buffer = MemoryUtil.directBuffer(transformed_bytes_address + wasm_mem_address,
                (int) transformed_bytes_len);
        byte[] transformedRecordBatchByteArray = new byte[(int) transformed_bytes_len];
        transformed_buffer.get(transformedRecordBatchByteArray);
        // Deallocate transformed bytes
        TransformInterface.DropTuple(instance_ptr, transformed_bytes_tuple);
        return transformedRecordBatchByteArray;
    }

    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
        try {
            if (transform) {
                getStreamTransform(context, ticket, listener);
            } else {
                getStreamNoTransform(context, ticket, listener);
            }
            System.out.println("relay producer - completed");
        } catch (Exception e) {
            listener.error(e);
            e.printStackTrace();
            System.out.println("relay producer - failed");
        }
    }

    public void getStreamTransform(CallContext context, Ticket ticket, ServerStreamListener listener) throws Exception {
        try (FlightStream stream = client.getStream(ticket);
                WasmAllocationFactory wasmAllocationFactory = new WasmAllocationFactory()) {
            long instance_ptr = wasmAllocationFactory.wasmInstancePtr();
            VectorLoader loader = null;
            while (stream.next()) {
                try (NoCopyByteArrayOutputStream out = new NoCopyByteArrayOutputStream()) {
                    // Write the input vector schema root to the byte array output stream in IPC
                    // format
                    try (ArrowStreamWriter writer = new ArrowStreamWriter(stream.getRoot(), null,
                            Channels.newChannel(out))) {
                        writer.start();
                        writer.writeBatch();
                        writer.end();
                    }

                    // Transform the original record batch
                    byte[] transformedRecordBatchByteArray = WASMTransformByteArray(instance_ptr, out.getBuffer(),
                            out.size());

                    // Read the byte array to get the transformed batch
                    try (ArrowStreamReader reader = new ArrowStreamReader(
                            new ByteArrayInputStream(transformedRecordBatchByteArray), allocator)) {
                        VectorSchemaRoot transformedVSR = reader.getVectorSchemaRoot();
                        reader.loadNextBatch();
                        VectorUnloader unloader = new VectorUnloader(transformedVSR);

                        // First time initialization
                        if (loader == null) {
                            VectorSchemaRoot root_out = VectorSchemaRoot.create(transformedVSR.getSchema(), allocator);
                            loader = new VectorLoader(root_out);
                            listener.setUseZeroCopy(zeroCopy);
                            listener.start(root_out);
                        }
                        loader.load(unloader.getRecordBatch());
                        listener.putNext();
                    }
                }
            }
            listener.completed();
        }
    }

    public void getStreamNoTransform(CallContext context, Ticket ticket, ServerStreamListener listener)
            throws Exception {
        VectorSchemaRoot root_out = null;
        VectorUnloader unloader = null;
        VectorLoader loader = null;
        FlightStream stream = client.getStream(ticket);
        try {
            while (stream.next()) {
                // First time initialization
                if (unloader == null) {
                    VectorSchemaRoot root = stream.getRoot();
                    unloader = new VectorUnloader(root);
                    root_out = VectorSchemaRoot.create(root.getSchema(), allocator);
                    loader = new VectorLoader(root_out);
                    listener.setUseZeroCopy(zeroCopy);
                    listener.start(root_out);
                }

                loader.load(unloader.getRecordBatch());
                listener.putNext();
            }
            listener.completed();
        } finally {
            stream.close();
            if (root_out != null) {
                root_out.close();
            }
        }
    }

    @Override
    public FlightInfo getFlightInfo(FlightProducer.CallContext context, FlightDescriptor descriptor) {
        Preconditions.checkArgument(descriptor.isCommand());
        // Perf exec = Perf.parseFrom(descriptor.getCommand());

        final Schema pojoSchema = new Schema(ImmutableList.of(Field.nullable("a", Types.MinorType.BIGINT.getType()),
                Field.nullable("b", Types.MinorType.BIGINT.getType()),
                Field.nullable("c", Types.MinorType.BIGINT.getType()),
                Field.nullable("d", Types.MinorType.BIGINT.getType())));

        int streamCount = 1;
        PerfOuterClass.Token token = PerfOuterClass.Token.newBuilder().build();
        final Ticket ticket = new Ticket(token.toByteArray());

        List<FlightEndpoint> endpoints = new ArrayList<>();
        for (int i = 0; i < streamCount; i++) {
            endpoints.add(new FlightEndpoint(ticket, location));
        }

        return new FlightInfo(pojoSchema, descriptor, endpoints, -1, -1);
    }

    @Override
    public void close() throws Exception {
        this.client.close();        
    }
}
