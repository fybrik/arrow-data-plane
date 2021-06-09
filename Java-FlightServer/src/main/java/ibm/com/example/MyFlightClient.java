package ibm.com.example;

import org.apache.arrow.flight.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.nio.charset.StandardCharsets;

public class MyFlightClient {
    public static void main(String[] args) {
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
	int i = 0;
        while (s.next()) {
            VectorSchemaRoot root = s.getRoot();
	    i++;
            System.out.println(i);
            //System.out.println(root.getVector(0));
            //System.out.println(root.getVector(1));
            //System.out.println(root.getVector(2));
            //System.out.println(root.getVector(3));
        }
    }
}
