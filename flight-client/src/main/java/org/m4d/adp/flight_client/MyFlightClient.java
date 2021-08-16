package org.m4d.adp.flight_client;

import org.apache.arrow.flight.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.nio.charset.StandardCharsets;

public class MyFlightClient {
    /** Accepts the hostname and port of the flight server.
     *  Reads the record-batches one at a time.
     *  measures time and computes throughput. */
    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("Usage: MyFlightClient host port");
            System.exit(-1);
        }
        String host = args[0];
        String port = args[1];

        final BufferAllocator a = new RootAllocator(Long.MAX_VALUE);
        BufferAllocator allocator = a.newChildAllocator("flight-client", 0, Long.MAX_VALUE);
        final FlightClient client = FlightClient.builder()
                .allocator(allocator)
                .location(Location.forGrpcInsecure(host, Integer.valueOf(port)))
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
        VectorSchemaRoot root = null;

        long start = System.currentTimeMillis();
        while (s.next()) {
            root = s.getRoot();
            //i++;
            // System.out.println(i);
            // System.out.println("client");
            //System.out.println(root.getVector(0));
            //System.out.println(root.getVector(1));
            //System.out.println(root.getVector(2));
            //System.out.println(root.getVector(3));
            //System.out.println(root.getVector(4));
            //System.out.println(root.getVector(5));
            //System.out.println(root.getVector(6));
        }
        long finish = System.currentTimeMillis();

        VarCharVector names = (VarCharVector) root.getVector(0);
        BigIntVector ages = (BigIntVector) root.getVector(1);
        BigIntVector building_numbers = (BigIntVector) root.getVector(2);
        VarCharVector streets = (VarCharVector) root.getVector(3);
        VarCharVector cities = (VarCharVector) root.getVector(4);
        VarCharVector countries = (VarCharVector) root.getVector(5);
        BigIntVector postcodes = (BigIntVector) root.getVector(6);

        for (int j = 0; j < root.getRowCount(); j++) {
            System.out.print(names.get(j) + ",");
            System.out.print(ages.get(j) + ",");
            System.out.print(building_numbers.get(j) + ",");
            System.out.print(streets.get(j) + ",");
            System.out.print(cities.get(j) + ",");
            System.out.print(countries.get(j) + ",");
            System.out.println(postcodes.get(j));
        }


        System.out.println("\nTime spent traversing dataset: " + (finish - start)/1000.0 + " seconds");
        double throughput = (double)i * root.getRowCount() * 4 * 4 / ((finish - start) / 1000.0);
        System.out.println("Throughput: " + String.format("%.2f", throughput / (1024*1024)) + "MB/sec");
        try {
            s.close();
            client.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        a.close();
    }
}
