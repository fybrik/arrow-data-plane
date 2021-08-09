package org.m4d.adp.flight_server;

import com.google.common.collect.ImmutableList;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.perf.impl.PerfOuterClass;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;

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
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ArrowFileWriter writer = new ArrowFileWriter(v, null, Channels.newChannel(out));
            writer.start();
            writer.writeBatch();
            writer.end();

            byte[] recordBatchByteArray = out.toByteArray();
            byte[] transformedRecordBatchByteArray = recordBatchByteArray.clone();

            // send outByteArray and transformedBytes to WASM for transformation.
            // we fake it by simply copying the bytes

            ArrowStreamReader reader =
                    new ArrowStreamReader(new ByteArrayInputStream(transformedRecordBatchByteArray),
                            allocator);
            VectorSchemaRoot readBatch = reader.getVectorSchemaRoot();
            reader.loadNextBatch();
            return readBatch;
            // do we need to run reader.close() ? Maybe in the "final" clause?
        } catch (IOException e) {
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
                //root_in = transformVectorSchemaRoot(root_in);
                root_in = WASMTransformVectorSchemaRoot(root_in);
            }

            unloader = new VectorUnloader(root_in);
            loader.load(unloader.getRecordBatch());

            listener.putNext();
        }
        listener.completed();
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
