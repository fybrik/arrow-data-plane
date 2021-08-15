package org.m4d.adp.flight_server;

import com.google.common.collect.ImmutableList;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.perf.impl.PerfOuterClass;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** Flight Producer for flight server that returns a single dataset.
 *  The dataset consists of a 1000 identical record batches. The record
 *  batch is in memory, has 4 integer columns of size 1024*1024.
 */
public class ExampleProducer extends NoOpFlightProducer {
    private final Location location;
    private final BufferAllocator allocator;
    private final VectorSchemaRoot constVectorSchemaRoot;
    private boolean isNonBlocking = false;

    public ExampleProducer(Location location, BufferAllocator allocator) {
        this.location = location;
        this.allocator = allocator;
        this.constVectorSchemaRoot = getConstVectorSchemaRoot();
    }

    private VectorSchemaRoot getConstVectorSchemaRoot() {
        final Schema schema = new Schema(ImmutableList.of(
                Field.nullable("name", Types.MinorType.VARCHAR.getType()),
                Field.nullable("age", Types.MinorType.BIGINT.getType()),
                Field.nullable("street_number", Types.MinorType.BIGINT.getType()),
                Field.nullable("street", Types.MinorType.VARCHAR.getType()),
                Field.nullable("city", Types.MinorType.VARCHAR.getType()),
                Field.nullable("country", Types.MinorType.VARCHAR.getType()),
                Field.nullable("postcode", Types.MinorType.BIGINT.getType())
        ));
        //Schema schema = Schema.deserialize(ByteBuffer.wrap(perf.getSchema().toByteArray()));
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);

        VarCharVector name = (VarCharVector) root.getVector("name");
        BigIntVector age = (BigIntVector) root.getVector("age");
        BigIntVector street_number = (BigIntVector) root.getVector("street_number");
        VarCharVector street = (VarCharVector) root.getVector("street");
        VarCharVector city = (VarCharVector) root.getVector("city");
        VarCharVector country = (VarCharVector) root.getVector("country");
        BigIntVector postcode = (BigIntVector) root.getVector("postcode");

        int counter = 0;
        try {
            BufferedReader csvReader = new BufferedReader(new FileReader("fake.csv"));
            String row;
            while ((row = csvReader.readLine()) != null) {
                String[] data = row.split(",");
                name.setSafe(counter, data[0].getBytes());
                age.setSafe(counter, Integer.parseInt(data[1]));
                street_number.setSafe(counter, Integer.parseInt(data[2]));
                street.setSafe(counter, data[3].getBytes());
                city.setSafe(counter, data[4].getBytes());
                country.setSafe(counter, data[5].getBytes());
                postcode.setSafe(counter, Integer.parseInt(data[6]));
                counter++;
            }
            csvReader.close();
        } catch (FileNotFoundException e) {
            System.out.println("cannot read input CSV file");
            System.exit(-1);
        } catch (IOException e) {
            System.out.println("error parsing CSV file");
            System.exit(-2);
        }

        root.setRowCount(counter);
        return root;
    }

    @Override
    public void getStream(FlightProducer.CallContext context, Ticket ticket,
                          FlightProducer.ServerStreamListener listener) {
        final Runnable loadData = () -> {
            listener.setUseZeroCopy(true);
            listener.start(this.constVectorSchemaRoot);
            for (int i=0; i<1000; i++) {
                //System.out.println("Example putNext()");
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
                Field.nullable("name", Types.MinorType.VARCHAR.getType()),
                Field.nullable("age", Types.MinorType.BIGINT.getType()),
                Field.nullable("street_number", Types.MinorType.BIGINT.getType()),
                Field.nullable("street", Types.MinorType.VARCHAR.getType()),
                Field.nullable("city", Types.MinorType.VARCHAR.getType()),
                Field.nullable("country", Types.MinorType.VARCHAR.getType()),
                Field.nullable("postcode", Types.MinorType.BIGINT.getType())
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
