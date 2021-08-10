package org.m4d.adp.flight_server;

import com.google.common.collect.ImmutableList;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.perf.impl.PerfOuterClass;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.memory.util.MemoryUtil;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
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
import java.io.IOException;
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
    private final BigIntVector constVec;
    private int RecordsPerBatch = 1024*1024;

    private BigIntVector getConstIntVector() {
        BigIntVector c = new BigIntVector("a", allocator);
        for (int j = 0; j < this.RecordsPerBatch; j++) {
            c.setSafe(j, 13);
        }
        c.setValueCount(this.RecordsPerBatch);
        return c;
    }

    public RelayProducer(Location location, Location remote_location,
                          BufferAllocator allocator, boolean transform) {
        this.transform = transform;
        this.allocator = allocator;
        this.constVec = getConstIntVector();
        this.location = location;
        this.client = FlightClient.builder()
                .allocator(allocator)
                .location(remote_location)
                .build();
    }

    private VectorSchemaRoot transformVectorSchemaRoot(VectorSchemaRoot v) {
        v = v.removeVector(0);
        return v.addVector(0, this.constVec);
    }

    private VectorSchemaRoot WASMTransformVectorSchemaRoot(VectorSchemaRoot v) {
        try {
            WasmAllocationFactory wasmAllocationFactory = new WasmAllocationFactory();
            long instance_ptr = wasmAllocationFactory.instancePtr;
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ArrowStreamWriter writer = new ArrowStreamWriter(v, null, Channels.newChannel(out));
            writer.start();
            writer.writeBatch();
            writer.end();

            byte[] recordBatchByteArray = out.toByteArray();
            long length = recordBatchByteArray.length;
            // for(int i = 0; i < length; i++){
            //     System.out.print(recordBatchByteArray[i] + " ,");
            // }

            long allocatedAddress = AllocatorInterface.wasmAlloc(instance_ptr, length);
            long wasm_mem_address = AllocatorInterface.wasmMemPtr(instance_ptr);
            System.out.println("mem addr before byte buffer = " + wasm_mem_address + " len " + length + " alloc addr " + allocatedAddress);
            ByteBuffer buffer = MemoryUtil.directBuffer(allocatedAddress + wasm_mem_address, (int) length);
            buffer.put(recordBatchByteArray);
            // TransformInterface.writeByteBuffer(recordBatchByteArray, instance_ptr, allocatedAddress, length);

            
            wasm_mem_address = AllocatorInterface.wasmMemPtr(instance_ptr);
            System.out.println("mem addr after byte buffer put = " + wasm_mem_address);
            long transformed_bytes_tuple = TransformInterface.transformationIPC(instance_ptr, wasm_mem_address, allocatedAddress, length);
            AllocatorInterface.wasmDealloc(instance_ptr, allocatedAddress, length);
            long transformed_bytes_address = TransformInterface.GetFirstElemOfTuple(transformed_bytes_tuple);
            long transformed_bytes_len = TransformInterface.GetSecondElemOfTuple(transformed_bytes_tuple);
            ByteBuffer transformed_buffer = MemoryUtil.directBuffer(transformed_bytes_address, (int) transformed_bytes_len);
            byte[] transformedRecordBatchByteArray = new byte[(int) transformed_bytes_len];
            transformed_buffer.get(transformedRecordBatchByteArray);
            // for(int i = 0; i < length; i++){
                //     System.out.print(transformedRecordBatchByteArray[i] + " ,");
                // }
                
                
                // send outByteArray and transformedBytes to WASM for transformation.
                // we fake it by simply copying the bytes
                
                ArrowStreamReader reader =
                new ArrowStreamReader(new ByteArrayInputStream(transformedRecordBatchByteArray),
                allocator);
                VectorSchemaRoot readBatch = reader.getVectorSchemaRoot();
                reader.loadNextBatch();
                // System.out.println("transformed batch\n" + readBatch.contentToTSVString());
                // reader.close();
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
        

        WasmAllocationFactory wasmAllocationFactory = new WasmAllocationFactory();
        long instance_ptr = wasmAllocationFactory.instancePtr;
        FlightStream s = client.getStream(ticket);
        VectorSchemaRoot root_in;
        VectorSchemaRoot root_out = null;
        VectorLoader loader = null;
        VectorUnloader unloader = null;
        // final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

        boolean first = true;
        System.out.println("relay producer");
        while (s.next()) {
            root_in = s.getRoot();
        // System.out.println("vsr_src content: \n" + root_in.contentToTSVString());

            // root_out = VectorSchemaRoot.create(root_in.getSchema(), allocator);
            // long ffi_in_ptr = TransformInterface.convertVSR2FFI(root_in);
            // long ffi_out_ptr = TransformInterface.transformation(ffi_in_ptr);
            // root_out = TransformInterface.createOrUpdateVSR(ffi_out_ptr, root_out);
            if (first) {
                System.out.println("relay producer - first");

                root_out = VectorSchemaRoot.create(root_in.getSchema(), allocator);
                loader = new VectorLoader(root_out);
                listener.setUseZeroCopy(false);
                listener.start(root_out);
                first = false;
            }
            if (transform) {
                System.out.println("relay producer - transform");
                // root_in.getFieldVectors().get(0).

                // long ffi_in_ptr = TransformInterface.convertVSR2FFI(root_in);
                // long ffi_out_ptr = TransformInterface.transformation(ffi_in_ptr, constVec);
                // root_out = TransformInterface.createOrUpdateVSR(ffi_out_ptr, root_out);
                // root_in = transformVectorSchemaRoot(root_in);
                root_in = WASMTransformVectorSchemaRoot(root_in);
                System.out.println("relay producer - after tranform");
           } else {
                System.out.println("relay producer - without transform");
                // long ffi_in_ptr = TransformInterface.convertVSR2FFI(root_in);
                // root_out = TransformInterface.createOrUpdateVSR(ffi_in_ptr, root_out);
           }



            unloader = new VectorUnloader(root_in);
            loader.load(unloader.getRecordBatch());

            listener.putNext();
            // System.out.println("relay producer - put next");
    }
        listener.completed();
        System.out.println("relay producer - completed");
        try {
            wasmAllocationFactory.close();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
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
