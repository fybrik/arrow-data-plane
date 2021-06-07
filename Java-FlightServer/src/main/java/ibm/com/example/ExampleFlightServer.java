package ibm.com.example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.common.collect.ImmutableList;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.perf.impl.PerfOuterClass.Token;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * An Example Flight Server that provides access to the InMemoryStore. Used for integration testing.
 */
public class ExampleFlightServer implements AutoCloseable {

    private final FlightServer flightServer;
    private final Location location;
    private final BufferAllocator allocator;
    private final boolean isNonBlocking;

    @Override
    public void close() throws Exception {
        AutoCloseables.close(flightServer, allocator);
    }

    public ExampleFlightServer(BufferAllocator incomingAllocator, Location location) {
        this(incomingAllocator, location, false);
    }

    public ExampleFlightServer(BufferAllocator incomingAllocator, Location location,
                                 boolean isNonBlocking) {
        this.allocator = incomingAllocator.newChildAllocator("flight-server", 0, Long.MAX_VALUE);
        this.location = location;
        Producer producer = new Producer();
        this.flightServer = FlightServer.builder(this.allocator, location, producer).build();
        this.isNonBlocking = isNonBlocking;
    }

    public Location getLocation() {
        return location;
    }

    public void start() throws IOException {
        flightServer.start();
    }

    public void awaitTermination() throws InterruptedException {
        flightServer.awaitTermination();
    }

    /**
     *  Main method starts the server listening to localhost:12233.
     */
    public static void main(String[] args) throws Exception {
        final BufferAllocator a = new RootAllocator(Long.MAX_VALUE);
        final ExampleFlightServer efs = new ExampleFlightServer(a, Location.forGrpcInsecure("localhost", 12233));
        efs.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                System.out.println("\nExiting...");
                AutoCloseables.close(efs, a);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }));
        efs.awaitTermination();
    }

    private final class Producer extends NoOpFlightProducer {
        private final int RecordsPerBatch = 4096;
        private final VectorSchemaRoot constVectorSchemaRoot = getConstVectorSchemaRoot();

        private VectorSchemaRoot getConstVectorSchemaRoot() {
            final Schema schema = new Schema(ImmutableList.of(
                    Field.nullable("a", MinorType.BIGINT.getType()),
                    Field.nullable("b", MinorType.BIGINT.getType()),
                    Field.nullable("c", MinorType.BIGINT.getType()),
                    Field.nullable("d", MinorType.BIGINT.getType())
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
        public void getStream(CallContext context, Ticket ticket,
                              ServerStreamListener listener) {
            final Runnable loadData = () -> {
                listener.setUseZeroCopy(true);
                listener.start(this.constVectorSchemaRoot);
                for (int i=0; i<10000; i++) {
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
        public FlightInfo getFlightInfo(CallContext context,
                                        FlightDescriptor descriptor) {
            Preconditions.checkArgument(descriptor.isCommand());
            //Perf exec = Perf.parseFrom(descriptor.getCommand());

            final Schema pojoSchema = new Schema(ImmutableList.of(
                    Field.nullable("a", MinorType.BIGINT.getType()),
                    Field.nullable("b", MinorType.BIGINT.getType()),
                    Field.nullable("c", MinorType.BIGINT.getType()),
                    Field.nullable("d", MinorType.BIGINT.getType())
            ));

            int streamCount = 1;
            Token token = Token.newBuilder()
                    .build();
            final Ticket ticket = new Ticket(token.toByteArray());

            List<FlightEndpoint> endpoints = new ArrayList<>();
            for (int i = 0; i < streamCount; i++) {
                endpoints.add(new FlightEndpoint(ticket, getLocation()));
            }

            return new FlightInfo(pojoSchema, descriptor, endpoints, -1, -1);
        }
    }
}