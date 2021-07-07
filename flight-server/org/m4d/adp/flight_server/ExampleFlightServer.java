package org.m4d.adp.flight_server;

import java.io.IOException;

import org.apache.arrow.flight.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;

/**
 * An Example Flight Server that provides access to the InMemoryStore. Used for integration testing.
 */
public class ExampleFlightServer implements AutoCloseable {

    private final FlightServer flightServer;
    private final BufferAllocator allocator;

    @Override
    public void close() throws Exception {
        AutoCloseables.close(flightServer, allocator);
    }

    public ExampleFlightServer(BufferAllocator incomingAllocator, Location location, NoOpFlightProducer producer) {
        this.allocator = incomingAllocator.newChildAllocator("flight-server", 0, Long.MAX_VALUE);
        this.flightServer = FlightServer.builder(this.allocator, location, producer).build();
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
        if ((args.length != 3) && (args.length != 6)) {
            System.out.println("Arguments are either:");
            System.out.println("\texample host port");
            System.out.println("\trelay transformation(true/false) host port remote_host remote_port");
            System.exit(-1);
        }

        String host;
        int port;
        String remote_host = null;
        int remote_port = 0;

        if (!args[0].equals("example") && !args[0].equals("relay")) {
            System.out.println("Only acceptable arguments are 'direct' or 'relay'. got " + args[0]);
            System.exit(-1);
        }

        boolean relay = false;
        boolean transform = false;
        if (args[0].equals("relay")) {
            relay = true;
            transform = Boolean.valueOf(args[1]);
            host = args[2];
            port = Integer.valueOf(args[3]);
            remote_host = args[4];
            remote_port = Integer.valueOf(args[5]);
        } else {
            host = args[1];
            port = Integer.valueOf(args[2]);
        }

        final BufferAllocator a = new RootAllocator(Long.MAX_VALUE);
        final Location location;
        final NoOpFlightProducer producer;
        if (relay) {
            location = Location.forGrpcInsecure(host, port);
            Location remote_location = Location.forGrpcInsecure(remote_host, remote_port);
            producer = new RelayProducer(location, remote_location, a, transform);
        } else {
            location = Location.forGrpcInsecure(host, port);
            producer = new ExampleProducer(location, a);
        }

        final ExampleFlightServer efs = new ExampleFlightServer(a, location, producer);
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
}
