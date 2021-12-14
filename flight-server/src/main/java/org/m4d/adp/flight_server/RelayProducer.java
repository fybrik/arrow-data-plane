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
import java.util.List;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.HashMap;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

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
    ArrayList<String> wasmImages;
    ArrayList<String> transformConfs;
    String datasetName;

    public RelayProducer(Location location, Location remote_location, BufferAllocator allocator, boolean transform,
            boolean zeroCopy) {
        this.transform = transform;
        this.zeroCopy = zeroCopy;
        this.allocator = allocator;
        this.location = location;
        this.client = FlightClient.builder().allocator(allocator).location(remote_location).build();
        this.configuration = null;
    }

    // Constructor with a configuration parameter
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
        // Read the configuration String as a map from String to Object
        Map<String, Object> conf = mapper.readValue(configuration, new TypeReference<Map<String, Object>>() {
        });
        // Getting the dataset we want to read and transform (Here we assume there is
        // only one dataset)
        List<Map<String, Object>> data = (List<Map<String, Object>>) conf.get("data");
        Map<String, Object> data1 = data.get(0);
        this.datasetName = (String) data1.get("name");
        // Get the actions that should be performed
        // TODO: Generalize to more than one transformation
        this.transformConfs = new ArrayList<String>();
        ArrayList<String> actionNames = new ArrayList<String>();
        List<Map<String, Object>> transformationsJson = (List<Map<String, Object>>) data1.get("transformations");
        for (Map<String, Object> transformationJson : transformationsJson) {
            String actionName = (String) transformationJson.get("name");
            actionNames.add(actionName);
            // Get the arguments of the transformation in order to move them to the Wasm
            // side
            Map<String, Object> argsJsonMap = (Map<String, Object>) transformationJson.get(actionName);
            String argsJsonStr = mapper.writeValueAsString(argsJsonMap);
            System.out.println("transform map = " + transformationsJson + " action name = " + actionName + " args = "
                    + argsJsonStr);
            this.transformConfs.add(argsJsonStr);
        }
        System.out.println("action names = " + actionNames);

        // Now we want to get the oci image that related to the action
        // The mapping from actions to images exists in a list of "transformations"
        this.wasmImages = new ArrayList<String>();
        List<Map<String, Object>> imageMappings = (List<Map<String, Object>>) conf.get("transformations");
        for (String actionName : actionNames) {
            for (Map<String, Object> imageMapping : imageMappings) {
                String ociActionName = (String) imageMapping.get("name");
                if (ociActionName.equalsIgnoreCase(actionName)) {
                    this.wasmImages.add((String) imageMapping.get("wasm_image"));
                    System.out.println("wasm images = " + this.wasmImages);
                    break;
                }
            }
        }
        System.out.println("wasm images: " + this.wasmImages + " dataset name: " + this.datasetName + " conf: "
                + this.transformConfs);
    }

    private byte[] WASMTransformByteArray(long instance_ptr_alloc, ArrayList<Long> wasmInstances,
            byte[] recordBatchByteArray, int size) {
        ///////////////////////////////////
        // Try more than one transformation
        byte[] transformedRecordBatchByteArray = null;
        long wasm_time_module = instance_ptr_alloc;
        long instance_ptr = wasm_time_module;
        long transformed_bytes_tuple = 0;
        long bytes_size = size;
        System.out.println("transformations loop record batch1 = " + recordBatchByteArray.length + " size = " + size);
        // Allocate a block in wasm memory and copy the byte array to this block
        long allocatedAddress = AllocatorInterface.wasmAlloc(instance_ptr, size);
        System.out.println("after alloc");
        long wasm_mem_address = AllocatorInterface.wasmMemPtr(instance_ptr);
        System.out.println("direct buffer");
        ByteBuffer buffer = MemoryUtil.directBuffer(allocatedAddress + wasm_mem_address, size);
        System.out.println("put record batch = " + recordBatchByteArray);
        buffer.put(recordBatchByteArray, 0, size);
        long transformed_bytes_address = 0;
        long transformed_bytes_len = 0;

        System.out.println("transformations loop, allocated address = " + allocatedAddress + " size " + size);
        // Allocate a block in wasm memory and copy the configuration to this block
        for (int i = this.transformConfs.size() - 1; i >= 0; i--) {
            byte[] confBytes = this.transformConfs.get(i).getBytes();
            int confSize = confBytes.length;
            long confAllocatedAddress = AllocatorInterface.wasmAlloc(instance_ptr, confSize);
            wasm_mem_address = AllocatorInterface.wasmMemPtr(instance_ptr);
            buffer = MemoryUtil.directBuffer(confAllocatedAddress + wasm_mem_address, confSize);
            buffer.put(confBytes, 0, confSize);
            // Transform the vector schema root that is represented as a byte array in
            // `allocatedAddress` memory address with length `size`
            // The function returns a tuple of `(address, length)` of as byte array that
            // represents the transformed vector schema root
            transformed_bytes_tuple = TransformInterface.TransformationIPC(instance_ptr, allocatedAddress, size,
                    confAllocatedAddress, confSize, i);
            // Get the byte array from the memory address
            transformed_bytes_address = TransformInterface.GetFirstElemOfTuple(instance_ptr, transformed_bytes_tuple);
            transformed_bytes_len = TransformInterface.GetSecondElemOfTuple(instance_ptr, transformed_bytes_tuple);
            // allocatedAddress = transformed_bytes_address;
            // size = (int) transformed_bytes_len;
            // TransformInterface.DropTuple(instance_ptr_transform,
            // transformed_bytes_tuple);
            // allocatedAddress = transformed_bytes_tuple;
            allocatedAddress = transformed_bytes_address;
            size = (int) transformed_bytes_len;
            AllocatorInterface.wasmDealloc(instance_ptr, confAllocatedAddress, confSize);
        }
        // transformed_bytes_address =
        // TransformInterface.GetFirstElemOfTuple(instance_ptr,
        // transformed_bytes_tuple);
        // transformed_bytes_len = TransformInterface.GetSecondElemOfTuple(instance_ptr,
        // transformed_bytes_tuple);
        wasm_mem_address = AllocatorInterface.wasmMemPtr(instance_ptr);
        System.out.println("WASMTransformByteArray3 " + wasm_mem_address);
        ByteBuffer transformed_buffer = MemoryUtil.directBuffer(transformed_bytes_address + wasm_mem_address,
                (int) transformed_bytes_len);
        transformedRecordBatchByteArray = new byte[(int) transformed_bytes_len];
        transformed_buffer.get(transformedRecordBatchByteArray);
        // recordBatchByteArray = transformedRecordBatchByteArray;
        // size = recordBatchByteArray.length;
        // Deallocate transformed bytes
        TransformInterface.DropTuple(instance_ptr, transformed_bytes_tuple);
        // AllocatorInterface.wasmDealloc(instance_ptr, allocatedAddress, bytes_size);
        // AllocatorInterface.wasmDealloc(instance_ptr, transformed_bytes_address,
        // transformed_bytes_len);
        // Try more than one transformation
        ///////////////////////////////////
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
        // Get instance of the Wasm module that related to the requested action
        try (FlightStream stream = client.getStream(ticket);
                WasmAllocationFactory wasmAllocationFactory = new WasmAllocationFactory(this.wasmImages);) {
            long instance_ptr_alloc = wasmAllocationFactory.wasmInstancePtr();

            ArrayList<Long> wasmInstances = new ArrayList<Long>();
            System.out.println("get stream transform");
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
                    byte[] transformedRecordBatchByteArray = WASMTransformByteArray(instance_ptr_alloc, wasmInstances,
                            out.getBuffer(),
                            out.size());

                    System.out.println("getStreamTransform 1");

                    // Read the byte array to get the transformed batch
                    try (ArrowStreamReader reader = new ArrowStreamReader(
                            new ByteArrayInputStream(transformedRecordBatchByteArray), allocator)) {
                        VectorSchemaRoot transformedVSR = reader.getVectorSchemaRoot();
                        // System.out.println("getStreamTransform transformed vsr = " +
                        // transformedVSR.contentToTSVString());
                        reader.loadNextBatch();
                        VectorUnloader unloader = new VectorUnloader(transformedVSR);
                        System.out.println("getStreamTransform 2");

                        // First time initialization
                        if (loader == null) {
                            VectorSchemaRoot root_out = VectorSchemaRoot.create(transformedVSR.getSchema(), allocator);
                            loader = new VectorLoader(root_out);
                            listener.setUseZeroCopy(zeroCopy);
                            listener.start(root_out);
                        }
                        loader.load(unloader.getRecordBatch());
                        listener.putNext();
                        System.out.println("getStreamTransform 1");

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
        // Create a request of the dataset for the arrow-flight server
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

        // Convert the request to Json
        String jsonStr = "";
        try {
            jsonStr = mapper.writeValueAsString(request);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        FlightDescriptor flightDescriptor = FlightDescriptor.command(jsonStr.getBytes(StandardCharsets.UTF_8));
        FlightInfo flightInfo = client.getInfo(flightDescriptor, clientProperties);
        System.out.println(flightInfo.getSchema());
        return flightInfo;
    }

    @Override
    public void close() throws Exception {
        this.client.close();
    }
}
