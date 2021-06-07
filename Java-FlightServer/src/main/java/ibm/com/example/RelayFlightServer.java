package ibm.com.example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.arrow.flight.*;
import org.apache.arrow.flight.perf.impl.PerfOuterClass.Token;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * An Example Flight Server that provides access to the InMemoryStore. Used for integration testing.
 */
public class RelayFlightServer implements AutoCloseable {

    private final FlightServer flightServer;
    private final Location location;
    private final BufferAllocator allocator;
    private final boolean isNonBlocking;
    private final FlightClient client;

    @Override
    public void close() throws Exception {
        AutoCloseables.close(flightServer, allocator);
    }

    public RelayFlightServer(BufferAllocator incomingAllocator, Location location) {
        this(incomingAllocator, location, false);
    }

    public RelayFlightServer(BufferAllocator incomingAllocator, Location location,
                             boolean isNonBlocking) {
        this.allocator = incomingAllocator.newChildAllocator("flight-server", 0, Long.MAX_VALUE);
        this.location = location;
        Producer producer = new Producer();
        this.flightServer = FlightServer.builder(this.allocator, location, producer).build();
        this.isNonBlocking = isNonBlocking;
        this.client = FlightClient.builder()
                .allocator(allocator)
                .location(Location.forGrpcInsecure("localhost", 12233))
                .build();
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
     * Main method starts the server listening to localhost:12232.
     */
    public static void main(String[] args) throws Exception {
        final BufferAllocator a = new RootAllocator(Long.MAX_VALUE);
        final RelayFlightServer efs = new RelayFlightServer(a, Location.forGrpcInsecure("localhost", 12232));
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
        @Override
        public void getStream(CallContext context, Ticket ticket,
                              ServerStreamListener listener) {
            FlightStream s = client.getStream(ticket);
            final Runnable loadData = () -> {
                VectorSchemaRoot root;
                listener.setUseZeroCopy(true);
                boolean first = true;
                while (s.next()) {
                    root = s.getRoot();
                    if (first) {
                        listener.start(root);
                        first = false;
                    }
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
            SchemaResult schemaResult = client.getSchema(descriptor);
            Preconditions.checkArgument(descriptor.isCommand());
            int streamCount = 1;
            Token token = Token.newBuilder().build();
            final Ticket ticket = new Ticket(token.toByteArray());

            List<FlightEndpoint> endpoints = new ArrayList<>();
            for (int i = 0; i < streamCount; i++) {
                endpoints.add(new FlightEndpoint(ticket, getLocation()));
            }

            return new FlightInfo(schemaResult.getSchema(), descriptor, endpoints, -1, -1);
        }
    }
}