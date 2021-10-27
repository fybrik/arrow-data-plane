package org.m4d.adp.flight_server;

import org.apache.arrow.flight.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.util.MemoryUtil;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.m4d.adp.allocator.AllocatorInterface;
import org.m4d.adp.allocator.WasmAllocationFactory;
import org.m4d.adp.transform.TransformInterface;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.HashMap;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.JSONPObject;
// import com.google.gson.Gson;

/**
 * Flight Producer for flight server that serves as relay to another flight
 * server (e.g. the flight server with ExampleProducer). Depending on the
 * transform boolean, this server may transform the first column with a constant
 * column.
 */
public class RelayProducer extends NoOpFlightProducer implements AutoCloseable {
    BufferAllocator allocator;
    private final boolean zeroCopy;
    private final boolean transform;
    private final FlightClient client;
    private final Location location;
    String configuration;
    String wasmImage;
    String transformConf;
    String datasetName;

    public RelayProducer(Location location, Location remote_location, BufferAllocator allocator, boolean transform,
            boolean zeroCopy) {
        this.transform = transform;
        this.zeroCopy = zeroCopy;
        this.allocator = allocator;
        this.location = location;
        this.client = FlightClient.builder().allocator(allocator).location(remote_location).build();
        this.configuration = "";
    }

    public RelayProducer(Location location, Location remote_location, BufferAllocator allocator, boolean transform,
            boolean zeroCopy, String configuration) throws Exception {
        this.transform = transform;
        this.zeroCopy = zeroCopy;
        this.allocator = allocator;
        this.location = location;
        this.client = FlightClient.builder().allocator(allocator).location(remote_location).build();
        this.configuration = configuration;
        //// Parsing the configuration
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> conf = mapper.readValue(configuration, new TypeReference<Map<String, Object>>(){});
        // The desired action exists in a "data" list.
        List<Map<String, Object>> data = (List<Map<String, Object>>) conf.get("data");
        // The mapping from actions to images exists in a list of "transformations"
        List<Map<String, Object>> imageMappings = (List<Map<String, Object>>) conf.get("transformations");
        Map<String, Object> data1 = data.get(0);
        this.datasetName = (String) data1.get("name");

        // List<Map<String, String>> transformations = (List<Map<String, String>>) data1.get("transformations");

        System.out.println("transforms[0] = ");

        List<Map<String, Object>> transformationsJson = (List<Map<String, Object>>) data1.get("transformations");
        System.out.println("transforms[0] = " + transformationsJson);
        // Map<String, String> transformationsMap = mapper.readValue(transformationsJson.get(0), new TypeReference<Map<String, String>>(){});
        String actionName = (String) transformationsJson.get(0).get("name");
        Map<String, Object> argsJsonMap = (Map<String, Object>) transformationsJson.get(0).get(actionName);
        String argsJsonStr = mapper.writeValueAsString(argsJsonMap);
        System.out.println("transform map = " + transformationsJson + " action name = " + actionName + " args = " + argsJsonStr);
        this.transformConf = argsJsonStr;



        // // String transformation = (String) data1.get("transformation");
        // // String actionId = transformations.get(0).get("id");
        // String actionName = transformations.get(0).get("name");
        // String actionArgs = transformations.get(0).get("args");
        
        // System.out.println("transformations = " + transformations +  " name = " + actionName + " args = " + actionArgs);
        // // transformation.split(" ");
        // Map<String, String> argsMap = Arrays.stream(actionArgs.split(" "))
        // .map(entry -> entry.split(":"))
        // .collect(Collectors.toMap(entry -> entry[0].replace("map", "").replace("[", "").replace("]", ""), entry -> entry[1].replace("map", "").replace("[", "").replace("]", "")));
        // // map.keySet().iterator().    
        // ObjectMapper jsonWriter = new ObjectMapper();
        // String jsonStr =  jsonWriter.writeValueAsString(argsMap);
        // this.transformConf = jsonStr;
        // System.out.println("map = " + argsMap + " map str = " + this.transformConf + " json str = " + jsonStr);

        // // Map<String, Object> transformation = (Map<String, Object>) transformations.get("transformation");
        // // String id = (String) transformation.get("id");
        // // System.out.println("id = " + id);
        // // "[map[args:map[column_name:age] id:redact-ID level:2 name:redact]]"

        // // Now we want to get the desired action
        // // String action = (String) transformations.get("action");

        
        String action = actionName;
        System.out.println("wanted action: " + action);
        // Now we want to get the oci image that related to the action
        for (int i = 0; i < imageMappings.size(); i++) {
            Map<String, Object> actionConf = imageMappings.get(i);
            String ociActionName = (String) actionConf.get("name");
            if (ociActionName.equalsIgnoreCase(action)) {
                this.wasmImage = (String) actionConf.get("wasm_image");
                // this.transformConf = (String) actionConf.get("configuration");
            }
            System.out.println("action name: " + ociActionName + " wasm image: " + this.wasmImage);
        }
        System.out.println("wasm image: " + this.wasmImage + " dataset name: " + this.datasetName + " conf: " + this.transformConf);
    }

    private byte[] WASMTransformByteArray(long instance_ptr, byte[] recordBatchByteArray, int size) {
        // Allocate a block in wasm memory and copy the byte array to this block
        long allocatedAddress = AllocatorInterface.wasmAlloc(instance_ptr, size);
        long wasm_mem_address = AllocatorInterface.wasmMemPtr(instance_ptr);
        ByteBuffer buffer = MemoryUtil.directBuffer(allocatedAddress + wasm_mem_address, size);
        buffer.put(recordBatchByteArray, 0, size);

        // Allocate a block in wasm memory and copy the configuration to this block
        byte[] confBytes = this.transformConf.getBytes();
        int confSize = confBytes.length;
        long confAllocatedAddress = AllocatorInterface.wasmAlloc(instance_ptr, confSize);
        wasm_mem_address = AllocatorInterface.wasmMemPtr(instance_ptr);
        buffer = MemoryUtil.directBuffer(confAllocatedAddress + wasm_mem_address, confSize);
        buffer.put(confBytes, 0, confSize);
        // Transform the vector schema root that is represented as a byte array in
        // `allocatedAddress` memory address with length `size`
        // The function returns a tuple of `(address, lenght)` of as byte array that
        // represents the transformed vector schema root
        long transformed_bytes_tuple = TransformInterface.TransformationIPC(instance_ptr, allocatedAddress, size, confAllocatedAddress, confSize); 
        // To add a code for copying the configuration to the memory of the module 
        // (define a possible struct) and send a pointer to it to the function 
        // (or we can do this in Rust side before calling wasm)

        // Get the byte array from the memory address
        long transformed_bytes_address = TransformInterface.GetFirstElemOfTuple(instance_ptr, transformed_bytes_tuple);
        long transformed_bytes_len = TransformInterface.GetSecondElemOfTuple(instance_ptr, transformed_bytes_tuple);
        wasm_mem_address = AllocatorInterface.wasmMemPtr(instance_ptr);
        ByteBuffer transformed_buffer = MemoryUtil.directBuffer(transformed_bytes_address + wasm_mem_address,
                (int) transformed_bytes_len);
        byte[] transformedRecordBatchByteArray = new byte[(int) transformed_bytes_len];
        transformed_buffer.get(transformedRecordBatchByteArray);
        // Deallocate transformed bytes
        TransformInterface.DropTuple(instance_ptr, transformed_bytes_tuple);
        return transformedRecordBatchByteArray;
    }

    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
        try {
            if (transform) {
                getStreamTransform(context, ticket, listener);
            } else {
                getStreamNoTransform(context, ticket, listener);
            }
            System.out.println("relay producer - completed");
        } catch (Exception e) {
            listener.error(e);
            e.printStackTrace();
            System.out.println("relay producer - failed");
        }
    }

    public void getStreamTransform(CallContext context, Ticket ticket, ServerStreamListener listener) throws Exception {
        // We need to parse the json string of the configuration to get the oci image for the desired action
        // String wasmImage = "";
        // ObjectMapper mapper = new ObjectMapper();
        // Map<String, Object> conf = mapper.readValue(configuration, new TypeReference<Map<String, Object>>(){});
        // // The desired action exists in a "data" list.
        // List<Map<String, Object>> data = (List<Map<String, Object>>) conf.get("data");
        // // The mapping from actions to images exists in a list of "transformations"
        // List<Map<String, Object>> imageMappings = (List<Map<String, Object>>) conf.get("transformations");
        // Map<String, Object> data1 = data.get(0);
        // // System.out.println(conf);

        // List<Map<String, Object>> transformations = (List<Map<String, Object>>) data1.get("transformations");
        // // Now we want to get the desired action
        // String action = (String) transformations.get(0).get("action");
        // System.out.println("wanted action: " + action);
        // // Now we want to get the oci image that related to the action
        // for (int i = 0; i < imageMappings.size(); i++) {
        //     Map<String, Object> actionConf = imageMappings.get(i);
        //     String actionName = (String) actionConf.get("name");
        //     if (actionName.equals(action)) {
        //         wasmImage = (String) actionConf.get("wasm_image");
        //     }
        //     System.out.println("action name: " + actionName + " wasm image: " + wasmImage);
        // }
        // System.out.println("wasm image: " + wasmImage);
        // System.out.println("assetId: " + dataset.get("name") + " transformations: " + transformations + " oci image: " + wasmImage);

    
        // String wasmImage = "ghcr.io/the-mesh-for-data/alloc-transform:v1";
        //// get oci image ////

        System.out.println("get stream transform");
        try (FlightStream stream = client.getStream(ticket);
                WasmAllocationFactory wasmAllocationFactory = new WasmAllocationFactory(wasmImage)) {
            long instance_ptr = wasmAllocationFactory.wasmInstancePtr();
            VectorLoader loader = null;
            while (stream.next()) {
                try (NoCopyByteArrayOutputStream out = new NoCopyByteArrayOutputStream()) {
                    // Write the input vector schema root to the byte array output stream in IPC
                    // format
                    try (ArrowStreamWriter writer = new ArrowStreamWriter(stream.getRoot(), null,
                            Channels.newChannel(out))) {
                        writer.start();
                        writer.writeBatch();
                        writer.end();
                    }

                    // Transform the original record batch
                    byte[] transformedRecordBatchByteArray = WASMTransformByteArray(instance_ptr, out.getBuffer(),
                            out.size());

                    // Read the byte array to get the transformed batch
                    try (ArrowStreamReader reader = new ArrowStreamReader(
                            new ByteArrayInputStream(transformedRecordBatchByteArray), allocator)) {
                        VectorSchemaRoot transformedVSR = reader.getVectorSchemaRoot();
                        reader.loadNextBatch();
                        VectorUnloader unloader = new VectorUnloader(transformedVSR);

                        // First time initialization
                        if (loader == null) {
                            VectorSchemaRoot root_out = VectorSchemaRoot.create(transformedVSR.getSchema(), allocator);
                            loader = new VectorLoader(root_out);
                            listener.setUseZeroCopy(zeroCopy);
                            listener.start(root_out);
                        }
                        loader.load(unloader.getRecordBatch());
                        listener.putNext();
                    }
                }
            }
            listener.completed();
        }
    }

    public void getStreamNoTransform(CallContext context, Ticket ticket, ServerStreamListener listener)
            throws Exception {
        VectorSchemaRoot root_out = null;
        VectorUnloader unloader = null;
        VectorLoader loader = null;
        FlightStream stream = client.getStream(ticket);
        try {
            while (stream.next()) {
                // First time initialization
                if (unloader == null) {
                    VectorSchemaRoot root = stream.getRoot();
                    unloader = new VectorUnloader(root);
                    root_out = VectorSchemaRoot.create(root.getSchema(), allocator);
                    loader = new VectorLoader(root_out);
                    listener.setUseZeroCopy(zeroCopy);
                    listener.start(root_out);
                }

                loader.load(unloader.getRecordBatch());
                listener.putNext();
            }
            listener.completed();
        } finally {
            stream.close();
            if (root_out != null) {
                root_out.close();
            }
        }
    }


    @Override
    public FlightInfo getFlightInfo(FlightProducer.CallContext context,
                                    FlightDescriptor descriptor) {
        final CallHeaders callHeaders = new FlightCallHeaders();
        final HeaderCallOption clientProperties = new HeaderCallOption(callHeaders);
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> request = new HashMap<String, Object>();
        System.out.println("dataset name = " + datasetName);
        request.put("asset", datasetName);
        List<String> list = new ArrayList<String>();
        list.add("name");
        list.add("age");
        list.add("street");
        list.add("country");
        request.put("columns", list);


        // Gson json = new Gson();
        String jsonStr = "";
        try {
            jsonStr = mapper.writeValueAsString(request);
        } catch (JsonProcessingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }//json.toJson(request);
        FlightDescriptor flightDescriptor = FlightDescriptor.command(jsonStr.getBytes(StandardCharsets.UTF_8));
        FlightInfo flightInfo = client.getInfo(flightDescriptor, clientProperties);
        System.out.println(flightInfo.getSchema());
        return flightInfo;
    }



    // @Override
    // public FlightInfo getFlightInfo(FlightProducer.CallContext context, FlightDescriptor descriptor) {
    //     Preconditions.checkArgument(descriptor.isCommand());
    //     // Perf exec = Perf.parseFrom(descriptor.getCommand());

    //     final Schema pojoSchema = new Schema(ImmutableList.of(Field.nullable("a", Types.MinorType.BIGINT.getType()),
    //             Field.nullable("b", Types.MinorType.BIGINT.getType()),
    //             Field.nullable("c", Types.MinorType.BIGINT.getType()),
    //             Field.nullable("d", Types.MinorType.BIGINT.getType())));

    //     int streamCount = 1;
    //     PerfOuterClass.Token token = PerfOuterClass.Token.newBuilder().build();
    //     final Ticket ticket = new Ticket(token.toByteArray());

    //     List<FlightEndpoint> endpoints = new ArrayList<>();
    //     for (int i = 0; i < streamCount; i++) {
    //         endpoints.add(new FlightEndpoint(ticket, location));
    //     }

    //     return new FlightInfo(pojoSchema, descriptor, endpoints, -1, -1);
    // }

    @Override
    public void close() throws Exception {
        this.client.close();
    }
}
