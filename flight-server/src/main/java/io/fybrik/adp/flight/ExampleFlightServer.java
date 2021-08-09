package io.fybrik.adp.flight;

import java.io.IOException;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.memory.AllocationManager.Factory;
import org.apache.arrow.util.AutoCloseables;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;

import io.fybrik.adp.core.Instance;
import io.fybrik.adp.core.allocator.WasmAllocationFactory;
import io.fybrik.adp.core.transformer.NoOpTransformer;
import io.fybrik.adp.core.transformer.Transformer;
import io.fybrik.adp.core.transformer.WasmTransformer;

public class ExampleFlightServer implements AutoCloseable {
    private final FlightServer flightServer;
    private final BufferAllocator allocator;

    public void close() throws Exception {
        AutoCloseables.close(new AutoCloseable[]{this.flightServer, this.allocator});
    }

    public ExampleFlightServer(BufferAllocator incomingAllocator, Location location, FlightProducer producer) {
        this.allocator = incomingAllocator.newChildAllocator("flight-server", 0L, 9223372036854775807L);
        this.flightServer = FlightServer.builder(this.allocator, location, producer).build();
    }

    public void start() throws IOException {
        this.flightServer.start();
    }

    public void awaitTermination() throws InterruptedException {
        this.flightServer.awaitTermination();
    }

    public static void main(String[] args) throws Exception {
        Options options = new Options();
        options.addOption("h", "host", true, "Host");
        options.addOption("p", "port", true, "Port");
        options.addOption("t", "transform", true, "Transformation WASM module (optional)");
        CommandLineParser parser = new DefaultParser();
        CommandLine line = parser.parse(options, args);
        String host = line.getOptionValue("host", "localhost");
        int port = Integer.parseInt(line.getOptionValue("port", "49152"));
        String transform = line.getOptionValue("transform", "./corev2/src/main/rust/wasm/polars_transform/target/wasm32-unknown-unknown/release/polars_transform.wasm");
        System.out.println("transform");
        System.out.println(transform);
        Instance instance = null;
        RootAllocator allocator;
        Transformer transformer;
        if (transform != null && !transform.isEmpty()) {
            instance = new Instance(transform);
            Factory allocationFactory = new WasmAllocationFactory(instance);
            allocator = new RootAllocator(RootAllocator.configBuilder().allocationManagerFactory(allocationFactory).build());
            transformer = new WasmTransformer(allocator, instance);
        } else {
            allocator = new RootAllocator(Long.MAX_VALUE);
            transformer = new NoOpTransformer();
        }

        Location location = Location.forGrpcInsecure(host, port);
        ExampleProducer producer = new ExampleProducer(location, allocator, transformer);
        ExampleFlightServer server = new ExampleFlightServer(allocator, location, producer);
        server.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                System.out.println("\nExiting...");
                AutoCloseables.close(new AutoCloseable[]{server, producer, allocator});
            } catch (Exception var4) {
                var4.printStackTrace();
            }

        }));
        server.awaitTermination();
        if (instance != null) {
            instance.close();
        }

    }
}