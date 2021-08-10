package org.m4d.adp.flight_server;

import java.io.IOException;

import org.apache.arrow.flight.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.memory.AllocationManager;
import org.m4d.adp.allocator.WasmAllocationManager;
import org.m4d.adp.allocator.WasmAllocationFactory;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

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

    private static BufferAllocator createWasmAllocator(AllocationManager.Factory factory) {
        return new RootAllocator(RootAllocator.configBuilder().allocationManagerFactory(factory)
            .build());
    }

    /**
     *  Main method starts the flight server.
     *  This server is either the memory server (using ExampleProducer) or
     *  a relay server (using RelayProducer). 
     */
    public static void main(String[] args) throws Exception {
        boolean relay = false;
        boolean transform = false;
        String host;
        int port;
        String remote_host = null;
        int remote_port = 0;
        BufferAllocator a;
        CommandLineParser parser = new DefaultParser();
        Options options = new Options();

        options.addOption("a", "alloc", true, "Allocation type");
        options.addOption("s", "server_type", true, "Server type");
        options.addOption("t", "transformation", true, "relay transformation(true/false)");
        options.addOption("h", "host", true, "Host");
        options.addOption("p", "port", true, "Port");
        options.addOption("rh", "remote_host", true, "Remote host");
        options.addOption("rp", "remote_port", true, "Remote port");

        CommandLine line = parser.parse( options, args );
        String allocator_type = line.getOptionValue("alloc", "Root");
        if(allocator_type.equals("wasm")) {
            WasmAllocationFactory wasmAllocationFactory = new WasmAllocationFactory();
            a = createWasmAllocator(wasmAllocationFactory);
        } else {
            a = new RootAllocator(Long.MAX_VALUE);
        }
        String server_type_arg = line.getOptionValue("server_type", "example");
        if (server_type_arg.equals("relay")) {
            relay = true;
            transform = Boolean.valueOf(line.getOptionValue("transformation", "false"));
            remote_host = line.getOptionValue("remote_host", "localhost");
            remote_port = Integer.valueOf(line.getOptionValue("remote_port", "12233"));
        }
        host = line.getOptionValue("host", "0.0.0.0");
        port = Integer.valueOf(line.getOptionValue("port", "12232"));
        
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
