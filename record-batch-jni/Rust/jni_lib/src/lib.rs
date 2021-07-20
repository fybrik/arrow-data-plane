mod exporter;

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
use jni::sys::{jlong, jarray, jobjectArray, jint};

use arrow::{
    buffer::Buffer,
    datatypes::{Schema, Field, DataType},
    array::{ArrayData, ArrayDataBuilder, StructArray, ArrayRef},
    util::pretty::print_batches,
    record_batch::RecordBatch
};

use std::ptr::NonNull;
use std::sync::Arc;
use serde_json::Value;
use std::mem;


// This keeps Rust from "mangling" the name and making it unique for this
// crate.
#[no_mangle]
pub extern "system" fn Java_com_ibm_arrowconverter_ArrowJNIAdapter_Convert2CdataInterface(env: JNIEnv,
// This is the class that owns our static method. It's not going to be used,
// but still must be present to match the expected signature of a static
// native method.
                                                                   _class: JClass,
                                                                   vsr_org: JObject,
                                                                    vsr_target: JObject)
                                                                   {
    // Schema as JValue
    let jo_schema = env.call_method(vsr_org, "getSchema","()Lorg/apache/arrow/vector/types/pojo/Schema;",&[])
        .unwrap()
        .l()
        .unwrap();
    let jo_json = env.call_method(jo_schema, "toJson","()Ljava/lang/String;",&[])
        .unwrap()
        .l()
        .unwrap();
    let js_json = JString::from(jo_json);
    let rs_json: String = env.get_string(js_json)
        .unwrap()
        .into();
    let rv_json : Value = serde_json::from_str(&rs_json)
        .unwrap();
    let r_schema = Schema::from(&rv_json)
        .unwrap();
    let fields = r_schema
        .fields()
        .clone();

    // Get vectors
    let jo_vectors_list = env.call_method(vsr_org, "getFieldVectors","()Ljava/util/List;",&[])
        .unwrap()
        .l()
        .unwrap();

    let vectors = get_vectors(env, jo_vectors_list, r_schema.fields().clone());
    let arrays: Vec<_> = vectors.iter().map(|x| StructArray::from(x.clone())).collect();

    let mut pairs: Vec<(Field, ArrayRef)> = Vec::new();
    for i in 0..arrays.len() {
        pairs.push((fields[i].clone(), arrow::array::make_array(vectors[i].clone())));
    }
    let struct_array = StructArray::from(pairs);
    let record = RecordBatch::from(&struct_array);

    let _ = print_batches(&[record.clone()]);

    // Load the record batch to the target vsr
    exporter::load_record_batch_to_vsr(env, vsr_target, &record);

    // Forget the record batch so we won't get seg fault
    mem::forget(record);
}

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