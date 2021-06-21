package ibm.com.example.server;

import com.google.common.collect.ImmutableList;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.perf.impl.PerfOuterClass;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ExampleProducer extends NoOpFlightProducer {
    private final Location location;
    private final BufferAllocator allocator;
    private final int RecordsPerBatch = 1024*1024;
    private final VectorSchemaRoot constVectorSchemaRoot;
    private boolean isNonBlocking = false;

    public ExampleProducer(Location location, BufferAllocator allocator) {
        this.location = location;
        this.allocator = allocator;
        this.constVectorSchemaRoot = getConstVectorSchemaRoot();
    }

    private VectorSchemaRoot getConstVectorSchemaRoot() {
        final Schema schema = new Schema(ImmutableList.of(
                Field.nullable("a", Types.MinorType.BIGINT.getType()),
                Field.nullable("b", Types.MinorType.BIGINT.getType()),
                Field.nullable("c", Types.MinorType.BIGINT.getType()),
                Field.nullable("d", Types.MinorType.BIGINT.getType())
        ));
        //Schema schema = Schema.deserialize(ByteBuffer.wrap(perf.getSchema().toByteArray()));
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        //root.allocateNew();
        BigIntVector a = (BigIntVector) root.getVector("a");
        BigIntVector b = (BigIntVector) root.getVector("b");
        BigIntVector c = (BigIntVector) root.getVector("c");
        BigIntVector d = (BigIntVector) root.getVector("d");

        for (int j = 0; j < this.RecordsPerBatch; j++) {
            a.setSafe(j, j);
            b.setSafe(j, j);
            c.setSafe(j, j);
            d.setSafe(j, j);
        }
        root.setRowCount(this.RecordsPerBatch);
        return root;
    }

    @Override
    public void getStream(FlightProducer.CallContext context, Ticket ticket,
                          FlightProducer.ServerStreamListener listener) {
        final Runnable loadData = () -> {
            listener.setUseZeroCopy(false);
            listener.start(this.constVectorSchemaRoot);
            for (int i=0; i<1000; i++) {
                //System.out.println("Example putNext()");
                listener.putNext();
            }
            listener.completed();
        };

        if (!isNonBlocking) {
            loadData.run();
        } else {
            final ExecutorService service = Executors.newSingleThreadExecutor();
            service.submit(loadData);
            service.shutdown();
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
