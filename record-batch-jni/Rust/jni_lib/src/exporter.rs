// This is the interface to the JVM that we'll call the majority of our
// methods on.
use jni::JNIEnv;

// These objects are what you should use as arguments to your native
// function. They carry extra lifetime information to prevent them escaping
// this context and getting used after being GC'd.
use jni::objects::{JClass, JString, JObject, JValue, JList};

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

pub fn create_arrow_buf_from_rust_buf(env: JNIEnv, buf: &Buffer) -> jobject {
    let len = buf.len() as i64;
    // Address of the memory
    let ptr = buf.as_ptr() as i64;
    // Creating the reference manager that will be accounting this buffer
    //let j_reference_manager = JObject::from(create_reference_manager(env, jmem_allocator));
    let jo_no_op_rm = env.get_static_field("org/apache/arrow/memory/ReferenceManager", "NO_OP", "Lorg/apache/arrow/memory/ReferenceManager;")
        .unwrap()
        .l()
        .unwrap();
    // Create the ArrowBuf
    let mut args = [JValue::Object(JObject::from(jo_no_op_rm)), JValue::Object(JObject::null()), JValue::Long(len), JValue::Long(ptr)];
    env.new_object("org/apache/arrow/memory/ArrowBuf", "(Lorg/apache/arrow/memory/ReferenceManager;Lorg/apache/arrow/memory/BufferManager;JJ)V", &args)
        .unwrap()
        .into_inner()
}

// Populate a java vector with buffers from rust
pub fn populate_java_vector(env: JNIEnv, r_arr : Arc<dyn Array>, j_field_vector: JObject) {
    let list_buffers = JList::from_env(&env, env.new_object("java/util/ArrayList", "(I)V", &[JValue::Int(1 as i32)]).unwrap()).unwrap();
    let r_buffers = [&r_arr.data().null_buffer().unwrap(), &r_arr.data().buffers()[0]];

    // Create ArrowFieldNode
    let length = r_arr.len() as i64;
    let null_count = r_arr.null_count() as i64;
    let j_arrow_field_node = env.new_object("org/apache/arrow/vector/ipc/message/ArrowFieldNode", "(JJ)V",
                                            &[JValue::Long(length), JValue::Long(null_count)])
        .unwrap();

    for i in 0..r_buffers.len() {
        let r_buf = r_buffers[i];
        let j_arrow_buf = JObject::from(create_arrow_buf_from_rust_buf(env, r_buf));

        // Add the buffer to the list of buffers
        list_buffers.add(j_arrow_buf);
    }
    // Load the buffers to the vector
    let args = [JValue::Object(j_arrow_field_node), JValue::Object(list_buffers.into())];
    env.call_method(j_field_vector, "loadFieldBuffers", "(Lorg/apache/arrow/vector/ipc/message/ArrowFieldNode;Ljava/util/List;)V", &args).unwrap();
}

pub fn load_record_batch_to_vsr(env: JNIEnv, vsr: JObject, record: &RecordBatch) {
    for i in 0..record.columns().len() {
        let column = record.columns()[i].clone();
        let j_field_vector = env.call_method(vsr, "getVector",
                                             "(I)Lorg/apache/arrow/vector/FieldVector;",
                                             &[JValue::Int(i as i32)])
            .unwrap()
            .l()
            .unwrap();
        populate_java_vector(env, column, j_field_vector);
    }
    let row_count = record.num_rows();
    env.call_method(vsr, "setRowCount", "(I)V", &[JValue::Int(row_count as i32)]);
}
