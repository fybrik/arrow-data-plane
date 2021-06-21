package org.m4d.adp.flight_client;

import org.m4d.adp.flight_server.ExampleFlightServer;
import org.m4d.adp.flight_server.ExampleProducer;
import org.m4d.adp.flight_server.RelayProducer;
import org.apache.arrow.flight.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class RunBenchmark {
    static class MyServer implements Runnable {
        @Override
        public void run() {
            final BufferAllocator a = new RootAllocator(Long.MAX_VALUE);
            final Location location = Location.forGrpcInsecure("localhost", 12233);
            final NoOpFlightProducer producer = new ExampleProducer(location, a);
            final ExampleFlightServer efs = new ExampleFlightServer(a, location, producer);
            try {
                efs.start();
                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    try {
                        System.out.println("\nExiting...");
                        AutoCloseables.close(efs, a);
                    } catch (Exception e) {
                        return;
                    }
                }));
                efs.awaitTermination();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    static class MyRelay implements Runnable {
        @Override
        public void run() {
            final BufferAllocator a = new RootAllocator(Long.MAX_VALUE);
            final Location location = Location.forGrpcInsecure("localhost", 12232);
            final Location remote_location = Location.forGrpcInsecure("localhost", 12233);
            final NoOpFlightProducer producer = new RelayProducer(location, remote_location, a);
            final ExampleFlightServer efs = new ExampleFlightServer(a, location, producer);
            try {
                efs.start();
                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    try {
                        System.out.println("\nExiting...");
                        AutoCloseables.close(efs, a);
                    } catch (Exception e) {
                        return;
                    }
                }));
                efs.awaitTermination();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    static class MyClient implements Runnable {
        int port;

        public MyClient(int port) {
            this.port = port;
        }

        @Override
        public void run() {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            final BufferAllocator a = new RootAllocator(Long.MAX_VALUE);
            BufferAllocator allocator = a.newChildAllocator("flight-client", 0, Long.MAX_VALUE);
            final FlightClient client = FlightClient.builder()
                    .allocator(allocator)
                    .location(Location.forGrpcInsecure("localhost", this.port))
                    .build();

            final CallHeaders callHeaders = new FlightCallHeaders();
            final HeaderCallOption clientProperties = new HeaderCallOption(callHeaders);
            FlightDescriptor flightDescriptor = FlightDescriptor.command("123".getBytes(StandardCharsets.UTF_8));
            FlightInfo flightInfo = client.getInfo(flightDescriptor, clientProperties);
            System.out.println(flightInfo.getSchema());
            FlightEndpoint flightEndpoint = flightInfo.getEndpoints().get(0);
            Ticket ticket = flightEndpoint.getTicket();
            System.out.println(ticket);

            FlightStream s = client.getStream(ticket);
            System.out.println("About to traverse record-batches");
            int i = 0;
            long start = System.currentTimeMillis();
            while (s.next()) {
                i++;
                System.out.println(i);
                VectorSchemaRoot root = s.getRoot();
                //System.out.println(root.getVector(0));
                //System.out.println(root.getVector(1));
                //System.out.println(root.getVector(2));
                //System.out.println(root.getVector(3));
            }
            long finish = System.currentTimeMillis();
            long timeElapsed = finish - start;
            System.out.println("Time spent traversing dataset: " + timeElapsed/1000.0 + " seconds");
            System.out.println("Done traversing record-batches");
        }
    }

    public static void main(String[] args) throws InterruptedException {
        if (args.length != 1) {
            System.out.println("Need single argument: either 'direct' or 'relay'");
            System.exit(-1);
        }
        if (!args[0].equals("direct") && !args[0].equals("relay")) {
            System.out.println("Only acceptable arguments are 'direct' or 'relay'. got " + args[0]);
            System.exit(-1);
        }

        boolean direct = false;
        if (args[0].equals("direct")) {
            direct = true;
        }

        Thread t1, t2, t3;

        MyServer s = new MyServer();
        t1 = new Thread(s);
        t1.start();

        MyRelay r = new MyRelay();
        t2 = new Thread(r);
        t2.start();

        MyClient c = new MyClient(direct ? 12233 : 12232);
        t3 = new Thread(c);
        t3.start();
        t3.join();

        t1.interrupt();
        t2.interrupt();
    }
}
