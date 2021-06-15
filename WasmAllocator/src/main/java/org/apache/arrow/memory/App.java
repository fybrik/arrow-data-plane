//package com.example;
package org.apache.arrow.memory;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import org.apache.arrow.vector.BaseValueVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;


/**
 * Hello world!
 *
 */
public class App 
{
    private static BaseAllocator createWasmAllocator() {
        return new RootAllocator(BaseAllocator.configBuilder().allocationManagerFactory(WasmAllocationManager.FACTORY)
            .build());
    }

    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
        //UnsafeAllocationManager allocator = new UnsafeAllocationManager(new RootAllocator(Integer.MAX_VALUE), Integer.MAX_VALUE);
        //BaseAllocator allocator = createUnsafeAllocator();
        BaseAllocator allocator = createWasmAllocator();
        System.out.println( "Hello World!2" );
        BitVector bitVector = new BitVector("boolean", allocator);
        System.out.println( "Hello World!3" );
        VarCharVector varCharVector = new VarCharVector("varchar", allocator);
        System.out.println( "Hello World!4" );
        bitVector.allocateNew();
        System.out.println( "Hello World!5" );
        varCharVector.allocateNew();
        System.out.println( "Hello World!6" );
        for (int i = 0; i < 10; i++) {
            bitVector.setSafe(i, i % 2 == 0 ? 0 : 1);
            varCharVector.setSafe(i, ("test" + i).getBytes(StandardCharsets.UTF_8));
        }
        System.out.println( "Hello World!7" );
        bitVector.setValueCount(10);
        varCharVector.setValueCount(10);

        List<org.apache.arrow.vector.types.pojo.Field> fields = Arrays.asList(bitVector.getField(), varCharVector.getField());
        List<? extends BaseValueVector> vectors = Arrays.asList(bitVector, varCharVector);
        VectorSchemaRoot vectorSchemaRoot = new VectorSchemaRoot(fields, (List<FieldVector>) vectors);
        System.out.println(fields);
        FieldVector tmp = vectorSchemaRoot.getVector("varchar");
        System.out.println(tmp);
        // int x = bitVector.get(3);
        // System.out.println("gg " + x);
        vectorSchemaRoot.close();
        //bitVector.close();
        //varCharVector.close();
    }
}
