package org.apache.arrow.memory;
import org.apache.arrow.memory.util.MemoryUtil;
import org.wasmer.Instance;
import org.wasmer.Memory;

import java.io.IOException;
import java.lang.StringBuilder;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

class GreetExample {
    public static void main(String[] args) throws IOException {
        // Instantiates the module.
        byte[] bytes = Files.readAllBytes(Paths.get("lib/greet.wasm"));
        Instance instance = new Instance(bytes);
        Memory memory = instance.exports.getMemory("memory");

        // Set the subject to greet.
        byte[] subject = "Wasmer".getBytes(StandardCharsets.UTF_8);
        Long memAddr = (long) 0;


        try {
            Field memPtr = Memory.class.getDeclaredField("memoryPointer");
            memPtr.setAccessible(true);
            Long fieldVal = (Long) memPtr.get(memory);
            memAddr = fieldVal;
            System.out.println(fieldVal);
          } catch (Exception e) {
            e.printStackTrace();
          }
          
        // Allocate memory for the subject, and get a pointer to it.
        Integer input_pointer = (Integer) instance.exports.getFunction("allocate").apply(1024)[0];
        System.out.println(input_pointer);
        System.out.println(subject.length);

        {
            ByteBuffer memoryBuffer = memory.buffer();
            System.out.println(memoryBuffer);
            memoryBuffer.position(input_pointer);
            memoryBuffer.put(subject);
        }


        MemoryUtil.UNSAFE.setMemory(memAddr+input_pointer-10000, 32, (byte) 5);
        System.out.println("setMemory2   :" + MemoryUtil.UNSAFE.getByte(memAddr+input_pointer-10000));
        // Write the subject into the memory.
        
        // Run the `greet` function. Give the pointer to the subject.
        Integer output_pointer = (Integer) instance.exports.getFunction("greet").apply(input_pointer)[0];
        System.out.println("gg");
        // Read the result of the `greet` function.
        String result;

        {
            StringBuilder output = new StringBuilder();
            ByteBuffer memoryBuffer = memory.buffer();
            System.out.println("gg");

            for (Integer i = output_pointer, max = memoryBuffer.limit(); i < max; ++i) {
                byte[] b = new byte[1];
                memoryBuffer.position(i);
                memoryBuffer.get(b);
                System.out.println("byte = " + b);
                if (b[0] == 0) {
                    break;
                }

                output.appendCodePoint(b[0]);
            }

            result = output.toString();
        }

        assert result.equals("Hello, Wasmer!");

        instance.close();
    }
}
