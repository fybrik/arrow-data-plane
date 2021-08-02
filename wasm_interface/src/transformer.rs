use crate::types::{WasmModule, Pointer, jptr};

use libc::c_void;
use jni::JNIEnv;
use jni::objects::{JString, JClass, JObject, JValue, JList};

// This is just a pointer. We'll be returning it from our function. We
// can't return one of the objects with lifetime information because the
// lifetime checker won't let us.
use jni::sys::{jlong, jarray, jobjectArray, jint, jobject};

use arrow::{
    buffer::Buffer,
    datatypes::{Schema, Field, DataType},
    array::{ArrayData, ArrayDataBuilder, StructArray, ArrayRef},
    util::pretty::print_batches,
    record_batch::RecordBatch
};

use arrow::ffi::FFI_ArrowArray;
use arrow::ffi::FFI_ArrowSchema;
use arrow::ffi::ArrowArray;
use arrow::ffi::ArrowArrayRef;

use std::ptr::NonNull;
use std::sync::Arc;
use serde_json::Value;
use std::mem;
use arrow::array::{Array, make_array_from_raw};
use arrow::datatypes::SchemaRef;

pub fn get_int_by_getter(env: JNIEnv, object: JObject, getter: &str) -> i32 {
    let jv_size = env.call_method(object, getter, "()I", &[]).unwrap();
    let r_size= jv_size.i().unwrap();
    r_size
}

pub fn get_vectors(env: JNIEnv, jv_vectors_list: JObject, fields: Vec<Field>) -> Vec<ArrayData> {
    let mut vec = Vec::new();
    let r_size = get_int_by_getter(env, jv_vectors_list, "size") as usize;
    for i in 0..r_size {
        let jv_i = [JValue::Int(i as jint)];
        let jv_vector = env.call_method(jv_vectors_list, "get", "(I)Ljava/lang/Object;", &jv_i).unwrap();
        let jo_vector = jv_vector.l().unwrap();
        let field = &fields[i];
        let r_array = create_data_array(env, jo_vector, field.data_type().clone());
        vec.push(r_array);
    }
    vec
}

pub fn arrow_buf_to_rust_buffer(env: JNIEnv, jo_arrow_buffer: JObject) -> Buffer {
    let jv_memory_address = env.call_method(jo_arrow_buffer, "memoryAddress", "()J", &[]).unwrap();
    let r_memory_address = jv_memory_address.j().unwrap();
    let casted_mptr = r_memory_address as  *mut u8;
    let non_null_ptr = NonNull::new(casted_mptr).unwrap();
    let jv_size = env.call_method(jo_arrow_buffer, "capacity", "()J", &[]).unwrap();
    let r_size = jv_size.j().unwrap();
    let buffer: Buffer = unsafe { Buffer::from_raw_parts(non_null_ptr, r_size as usize, r_size as usize)  };

    buffer
}

pub fn get_buffer_by_getter(env: JNIEnv, jo_value_vector: JObject, getter: &str) -> Buffer {
    let jv_arrow_buffer = env.call_method(jo_value_vector, getter, "()Lorg/apache/arrow/memory/ArrowBuf;", &[]).unwrap();
    let jo_arrow_buffer = jv_arrow_buffer.l().unwrap();
    arrow_buf_to_rust_buffer(env, jo_arrow_buffer)
}

pub fn get_buffers(env: JNIEnv, jo_vector: JObject) -> Vec<Buffer> {
    let mut buffers = Vec::new();
    let buf = get_buffer_by_getter(env, jo_vector, "getDataBuffer");
    buffers.push(buf);
    buffers
}

pub fn create_data_array(env: JNIEnv, jo_vector: JObject, datatype: DataType) -> ArrayData {
    let mut builder = ArrayDataBuilder::new(datatype);

    // Get buffers
    let buffers = get_buffers(env, jo_vector);
    builder = builder.buffers(buffers);

    // Get null buffer
    let null_buffer = get_buffer_by_getter(env, jo_vector, "getValidityBuffer");
    builder = builder.null_bit_buffer(null_buffer);

    // Get number of elements
    let len = get_int_by_getter(env, jo_vector, "getValueCount") as usize;
    builder = builder.len(len);

    builder.build()
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

pub fn from_rust_schema_to_java_schema<'a>(env: JNIEnv, r_schema: SchemaRef) -> jobject {
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
        .unwrap()
        .into_inner()
}

pub fn c_data_interface_to_record_batch(raw_ptr: jptr) -> RecordBatch {
    let tuple_ptr: Pointer<(*const FFI_ArrowArray, *const FFI_ArrowSchema)> = Pointer::from(raw_ptr);
    let PFFIAA = unsafe { (*tuple_ptr).0 as *const FFI_ArrowArray };
    let PFFIAS =  unsafe { (*tuple_ptr).1 as *const FFI_ArrowSchema };
    let arrow_array = unsafe{ ArrowArray::try_from_raw(PFFIAA, PFFIAS).unwrap() };
    let array_data = arrow_array.to_data().unwrap();
    let struct_array : StructArray = array_data.into();
    RecordBatch::from(&struct_array)
}
