package ibm.com.example;

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
            final ExampleFlightServer efs = new ExampleFlightServer(a, Location.forGrpcInsecure("localhost", 12233));
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
            final RelayFlightServer efs = new RelayFlightServer(a, Location.forGrpcInsecure("localhost", 12232));
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
                    .location(Location.forGrpcInsecure("localhost", 12232))
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
            while (s.next()) {
                i++;
                System.out.println(i);
                VectorSchemaRoot root = s.getRoot();
                //System.out.println(root.getVector(0));
                //System.out.println(root.getVector(1));
                //System.out.println(root.getVector(2));
                //System.out.println(root.getVector(3));
            }
            System.out.println("Done traversing record-batches");
        }
    }

    public static void main(String[] args) throws InterruptedException {
        MyServer s = new MyServer();
        MyRelay r = new MyRelay();
        MyClient c = new MyClient();

        Thread t1 =new Thread(s);
        Thread t2 =new Thread(r);
        Thread t3 =new Thread(c);

        t1.start();
        t2.start();
        t3.start();

        t3.join();
        t1.interrupt();
        t2.interrupt();
    }
}
