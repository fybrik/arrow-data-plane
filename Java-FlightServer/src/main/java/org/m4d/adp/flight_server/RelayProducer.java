package org.m4d.adp.flight_server;

import com.google.common.collect.ImmutableList;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.perf.impl.PerfOuterClass;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RelayProducer extends NoOpFlightProducer {
    private final BackpressureStrategy bpStrategy;
    private boolean isNonBlocking = false;
    private final FlightClient client;
    private final Location location;

    private RelayProducer(BackpressureStrategy bpStrategy, Location location, Location remote_location, BufferAllocator allocator) {
        this.bpStrategy = bpStrategy;
        this.location = location;
        this.client = FlightClient.builder()
                .allocator(allocator)
                .location(remote_location)
                .build();
    }

    public RelayProducer(Location location, Location remote_location, BufferAllocator allocator) {
        this(new BackpressureStrategy() {
            private FlightProducer.ServerStreamListener listener;

            @Override
            public void register(FlightProducer.ServerStreamListener listener) {
                this.listener = listener;
            }

            @Override
            public WaitResult waitForListener(long timeout) {
                while (!listener.isReady() && !listener.isCancelled()) {
                    // busy wait
                }
                return WaitResult.READY;
            }
        }, location, remote_location, allocator);
    }

    @Override
    public void getStream(CallContext context, Ticket ticket,
                          ServerStreamListener listener) {
        bpStrategy.register(listener);
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
                bpStrategy.waitForListener(0);
                //System.out.println("Relay putNext()");
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
