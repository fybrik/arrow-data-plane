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
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;

/** Flight Producer for flight server that serves as relay to another
 *  flight server (e.g. the flight server with ExampleProducer).
 *  Depending on the transform boolean, this server may transform
 *  the first column with a constant column.
 */
public class RelayProducer extends NoOpFlightProducer {
    BufferAllocator allocator;
    private boolean transform = false;
    private final FlightClient client;
    private final Location location;

    public RelayProducer(Location location, Location remote_location,
                          BufferAllocator allocator, boolean transform) {
        this.transform = transform;
        this.allocator = allocator;
        this.location = location;
        this.client = FlightClient.builder()
                .allocator(allocator)
                .location(remote_location)
                .build();
    }

    private VectorSchemaRoot WASMTransformVectorSchemaRoot(VectorSchemaRoot v) {
        try {
            WasmAllocationFactory wasmAllocationFactory = new WasmAllocationFactory();
            long instance_ptr = wasmAllocationFactory.wasmInstancePtr();
            // Write the vector schema root to an IPC stream
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ArrowStreamWriter writer = new ArrowStreamWriter(v, null, Channels.newChannel(out));
            writer.start();
            writer.writeBatch();
            writer.end();
            // Get a byte array of the vector schema root
            byte[] recordBatchByteArray = out.toByteArray();
            long length = recordBatchByteArray.length;
            // Allocate a block in wasm memory and copy the byte array to this block
            long allocatedAddress = AllocatorInterface.wasmAlloc(instance_ptr, length);
            long wasm_mem_address = AllocatorInterface.wasmMemPtr(instance_ptr);
            ByteBuffer buffer = MemoryUtil.directBuffer(allocatedAddress + wasm_mem_address, (int) length);
            buffer.put(recordBatchByteArray);
            wasm_mem_address = AllocatorInterface.wasmMemPtr(instance_ptr);
            writer.close();

            // Transform the vector schema root that is represented as a byte array in `allocatedAddress` memory address with length `length`
            // The function returns a tuple of `(address, lenght)` of as byte array that represents the transformed vector schema root 
            long transformed_bytes_tuple = TransformInterface.transformationIPC(instance_ptr, allocatedAddress, length);
            AllocatorInterface.wasmDealloc(instance_ptr, allocatedAddress, length);
            // Get the byte array from the memory address
            long transformed_bytes_address = TransformInterface.GetFirstElemOfTuple(transformed_bytes_tuple);
            long transformed_bytes_len = TransformInterface.GetSecondElemOfTuple(transformed_bytes_tuple);
            ByteBuffer transformed_buffer = MemoryUtil.directBuffer(transformed_bytes_address, (int) transformed_bytes_len);
            byte[] transformedRecordBatchByteArray = new byte[(int) transformed_bytes_len];
            transformed_buffer.get(transformedRecordBatchByteArray);
            
            // Read the byte array to get the transformed vector schema root
            ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(transformedRecordBatchByteArray), allocator);
            VectorSchemaRoot readBatch = reader.getVectorSchemaRoot();
            reader.loadNextBatch();
            wasmAllocationFactory.close();
            return readBatch;
            // do we need to run reader.close() ? Maybe in the "final" clause?
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public void getStream(CallContext context, Ticket ticket,
                          ServerStreamListener listener) {
        FlightStream s = client.getStream(ticket);
        VectorSchemaRoot root_in;
        VectorSchemaRoot root_out = null;
        VectorLoader loader = null;
        VectorUnloader unloader = null;

        boolean first = true;
        while (s.next()) {
            root_in = s.getRoot();
            if (first) {
                root_out = VectorSchemaRoot.create(root_in.getSchema(), allocator);
                loader = new VectorLoader(root_out);
                listener.setUseZeroCopy(false);
                listener.start(root_out);
                first = false;
            }
            if (transform) {
                root_in = WASMTransformVectorSchemaRoot(root_in);
            }

            unloader = new VectorUnloader(root_in);
            loader.load(unloader.getRecordBatch());

            listener.putNext();
        }
        listener.completed();
        System.out.println("relay producer - completed");
    }

    @Override
    public FlightInfo getFlightInfo(FlightProducer.CallContext context,
                                    FlightDescriptor descriptor) {
        Preconditions.checkArgument(descriptor.isCommand());
        //Perf exec = Perf.parseFrom(descriptor.getCommand());

        final Schema pojoSchema = new Schema(ImmutableList.of(
                Field.nullable("a", Types.MinorType.BIGINT.getType()),
                Field.nullable("b", Types.MinorType.BIGINT.getType()),
                Field.nullable("c", Types.MinorType.BIGINT.getType()),
                Field.nullable("d", Types.MinorType.BIGINT.getType())
        ));

        int streamCount = 1;
        PerfOuterClass.Token token = PerfOuterClass.Token.newBuilder()
                .build();
        final Ticket ticket = new Ticket(token.toByteArray());

        List<FlightEndpoint> endpoints = new ArrayList<>();
        for (int i = 0; i < streamCount; i++) {
            endpoints.add(new FlightEndpoint(ticket, location));
        }

        return new FlightInfo(pojoSchema, descriptor, endpoints, -1, -1);
    }
}
