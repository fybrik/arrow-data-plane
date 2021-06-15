// This is the interface to the JVM that we'll call the majority of our
// methods on.
use jni::JNIEnv;

// These objects are what you should use as arguments to your native
// function. They carry extra lifetime information to prevent them escaping
// this context and getting used after being GC'd.
use jni::objects::{JClass, JString, JObject, JValue};

// This is just a pointer. We'll be returning it from our function. We
// can't return one of the objects with lifetime information because the
// lifetime checker won't let us.
use jni::sys::{jlong, jarray, jobject, jint};

use arrow::{
    buffer::Buffer,
    datatypes::{Schema, Field, DataType},
    array::{ArrayData, ArrayDataBuilder, StructArray, ArrayRef, Array},
    util::pretty::print_batches,
    record_batch::RecordBatch
};

use serde_json::Value;
use std::sync::Arc;

// Get a rust arrow-schema, and returns a java Schema
// Convert the schema to a json representation, and then create the Java schema from the json
pub fn from_rust_schema_to_java_schema<'a>(env: JNIEnv, r_schema: Schema) -> jobject {
    let json = r_schema.to_json();
    let json_string = serde_json::to_string(&json).unwrap();
    let j_string = JValue::from(env.new_string(json_string).unwrap());
    env.call_static_method(
                                      "org/apache/arrow/vector/types/pojo/Schema",
                                     "fromJSON",
                                     "(Ljava/lang/String;)org/apache/arrow/vector/types/pojo/Schema;",
                                      &[j_string])
        .unwrap()
        .l()
        .unwrap().into_inner()
}


// Create a reference manager using the memory allocator
pub fn create_reference_manager(env: JNIEnv, jmem_allocator: JObject) -> jobject {
    let jv_buffer_alocator = env.call_method(jmem_allocator, "getBufferAllocator", "()Lorg/apache/arrow/memory/BufferAllocator;", &[])
        .unwrap();
    let args = [jv_buffer_alocator, JValue::Object(jmem_allocator)];
    env.new_object("Lorg/apache/arrow/memory/BufferLedger", "(Lorg/apache/arrow/memory/BufferAllocator;Lorg/apache/arrow/memory/AllocationManager;)V", &args)
        .unwrap()
        .into_inner()

}

// Populate a java vector with buffers from rust
pub fn populate_java_vector(env: JNIEnv, r_arr : Arc<dyn Array>, jmem_allocator: JObject, j_field_vector: JObject) {
    let list_buffers = env.new_object("Ljava/util/ArrayList", "()V", &[]).unwrap();
    let r_buffers = r_arr.data().buffers();

    // Create ArrowFieldNode
    let length = r_arr.len() as i64;
    let null_count = r_arr.null_count() as i64;
    let j_arrow_field_node = env.new_object("Lorg/apache/arrow/vector/ipc/message/ArrowFieldNode", "(JJ)V",
                                            &[JValue::Long(length), JValue::Long(null_count)])
        .unwrap();

    for i in 0..r_buffers.len() {
        let buf = &r_buffers[i];
        let len = buf.len() as i64;
        // Address of the memory
        let ptr = buf.as_ptr() as i64;

        // Creating the reference manager that will be accounting this buffer
        let j_reference_manager = JObject::from(create_reference_manager(env, jmem_allocator));

        let mut args = [JValue::Object(JObject::from(j_reference_manager)), JValue::Object(JObject::null()), JValue::Long(len), JValue::Long(ptr)];

        // Create the ArrowBuf
        let j_arrow_buf = env.new_object("Lorg/apache/arrow/memory/ArrowBuf", "(Lorg/apache/arrow/memory/ReferenceManager;Lorg/apache/arrow/memory/BufferManager;JJ)V", &args)
            .unwrap();

        // Increment by 1 the number of references to the buffer in the buffer manager
        env.call_method(j_reference_manager, "retain", "()V", &[]);

        // Add the buffer to the list of buffers
        env.call_method(list_buffers, "add", "(Lorg/apache/arrow/memory/ArrowBuf;)V", &[JValue::Object(j_arrow_buf)]);
    }
    let args = [JValue::Object(j_arrow_field_node), JValue::Object(list_buffers)];

    // Load the buffers to the vector
    env.call_method(j_field_vector, "loadFieldBuffers", "(org/apache/arrow/vector/ipc/message/ArrowFieldNode;Ljava/util/List;)V", &args);

}

pub fn load_record_batch_to_vsr(env: JNIEnv, vsr: JObject, record: RecordBatch, jmem_allocator: JObject) {
    let j_list_vectors = env.call_method(vsr, "getFieldVectors", "()V",&[])
        .unwrap()
        .l()
        .unwrap();
    for i in 0..record.columns().len() {
        let column = record.columns()[i].clone();
        let j_field_vector = env.call_method(j_list_vectors, "get",
                                             "(I)Lorg/apache/arrow/vector/FieldVector;",
                                             &[JValue::Int(i as i32)])
            .unwrap()
            .l()
            .unwrap();
        populate_java_vector(env, column, jmem_allocator, j_field_vector);
    }
}


// Get a rust arrow-array, and returns a java FieldVector
pub fn from_rust_arr_to_java_vector<'a>(env: JNIEnv, r_arr : &Array) -> JObject<'a>{
    JObject::null()
}

// Get a rust columns, and returns a java List of FieldVector
pub fn from_rust_colums_to_java_vectors<'a>(env: JNIEnv, columns: Vec<Arc<Array>>) -> JObject<'a> {
    JObject::null()
}

// Get a rust record-batch, and returns a java VectorSchemaRoot
pub fn from_rust_record_batch_to_java_vsr<'a>(env: JNIEnv, record: RecordBatch) -> JObject<'a> {
    JObject::null()
}


