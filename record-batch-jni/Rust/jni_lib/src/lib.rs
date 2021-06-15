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
                                                                   vsr: JObject)
                                                                   {
    // Schema as JValue
    let jv_schema = env.call_method(vsr, "getSchema","()Lorg/apache/arrow/vector/types/pojo/Schema;",&[]).unwrap();
    // Schema aa JObject
    let jo_schema = jv_schema.l().unwrap();

    let jv_json = env.call_method(jo_schema, "toJson","()Ljava/lang/String;",&[]).unwrap();
    let jo_json = jv_json.l().unwrap();
    let js_json = JString::from(jo_json);
    let rs_json: String = env.get_string(js_json).unwrap().into();
    println!("json schema: {}", rs_json);

    let rv_json : Value = serde_json::from_str(&rs_json).unwrap();
    //let r_field: DataType = rv_json.into();
    //println!("{}", r_field);

    let r_schema = Schema::from(&rv_json).unwrap();

    //let r_field = Field::from(&rv_json).unwrap();

    println!("schema object: {}", r_schema);

    let fields = r_schema.fields().clone();
    //let r_datatype = DataType::Struct(fields);

    //println!("data-type object: {}", r_datatype);

    // Get vectors
    let jv_vectors_list = env.call_method(vsr, "getFieldVectors","()Ljava/util/List;",&[]).unwrap();
    let jo_vectors_list = jv_vectors_list.l().unwrap();
    let vectors = get_vectors(env, jo_vectors_list, r_schema.fields().clone());
    println!("got all vectors!");
    let arrays: Vec<_> = vectors.iter().map(|x| StructArray::from(x.clone())).collect();

    let mut pairs: Vec<(Field, ArrayRef)> = Vec::new();
    for i in 0..arrays.len() {
        pairs.push((fields[i].clone(), arrow::array::make_array(vectors[i].clone())));
    }

    let struct_array = StructArray::from(pairs);
    let record = RecordBatch::from(&struct_array);
    //println!("array: {:?}", record.columns()[0].data());
    println!("managed to convert to a RecordBatch!");
    check_batch(&record);
    let _ = print_batches(&[record.clone()]);
    println!("passed assertion");
    mem::forget(record);
}

fn check_batch(record_batch: &RecordBatch) {
    assert_eq!(5, record_batch.num_rows());
    assert_eq!(4, record_batch.num_columns());
    assert_eq!(5, record_batch.column(0).data().len());
    assert_eq!(5, record_batch.column(1).data().len());
    assert_eq!(5, record_batch.column(2).data().len());
    assert_eq!(5, record_batch.column(3).data().len());

    assert_eq!(&DataType::Int32, record_batch.schema().field(0).data_type());
    assert_eq!(&DataType::Int32, record_batch.schema().field(1).data_type());
    assert_eq!(&DataType::Int32, record_batch.schema().field(2).data_type());
    assert_eq!(&DataType::Int32, record_batch.schema().field(3).data_type());


}

pub fn get_int_by_getter(env: JNIEnv, object: JObject, getter: &str) -> i32 {
    println!("get_int_by_getter");
    let jv_size = env.call_method(object, getter, "()I", &[]).unwrap();
    let r_size= jv_size.i().unwrap();
    r_size
}

pub fn get_vectors(env: JNIEnv, jv_vectors_list: JObject, fields: Vec<Field>) -> Vec<ArrayData> {
    println!("get_vectors");
    let mut vec = Vec::new();

    let r_size = get_int_by_getter(env, jv_vectors_list, "size") as usize;
    //println!("number of vectors: {}", r_size);

    for i in 0..r_size {
        println!("i = {}", i);

        let jv_i = [JValue::Int(i as jint)];
        let jv_vector = env.call_method(jv_vectors_list, "get", "(I)Ljava/lang/Object;", &jv_i).unwrap();
        let jo_vector = jv_vector.l().unwrap();
        let field = &fields[i];
        let r_array = create_data_array(env, jo_vector, field.data_type().clone());
        vec.push(r_array);
    }

    vec
}

/*
pub fn get_children<'a>(env: JNIEnv<'a>, jo_parent_vector: JObject) -> Vec<JObject<'a>> {
    let mut vec: Vec<JObject<'a>> = Vec::new();
    let jv_children_list = env.call_method(jo_parent_vector, "getChildrenFromFields", "()Ljava/util/List;", &[]).unwrap();
    let jo_children_list = jv_children_list.l().unwrap();

    let r_size = get_int_by_getter(env, jo_children_list, "size");

    for i in 0..r_size {
        let jv_i = JValue::from(i as jint);
        let jv_child_vector = env.call_method(jo_children_list, "get", "(I)Lorg/apache/arrow/vector/FieldVector;", &[jv_i]).unwrap();
        let jo_child_vector = jv_child_vector.l().unwrap();
        vec.push(jo_child_vector);
    }
    vec
}

*/

/*
pub fn get_bull_count(env: JNIEnv, j_value_vector: JObject) -> usize {
    let jv_null_count = env.call_method(j_value_vector, "getNullCount", "()I;", &[]).unwrap();
    let r_null_count : i32 = jv_null_count.i().unwrap().into();
    r_null_count as usize
}
*/


pub fn arrow_buf_to_rust_buffer(env: JNIEnv, jo_arrow_buffer: JObject) -> Buffer {
    println!("arrow_buf_to_rust_buffer");
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
    println!("get_buffer_by_getter");
    let jv_arrow_buffer = env.call_method(jo_value_vector, getter, "()Lorg/apache/arrow/memory/ArrowBuf;", &[]).unwrap();
    let jo_arrow_buffer = jv_arrow_buffer.l().unwrap();
    arrow_buf_to_rust_buffer(env, jo_arrow_buffer)
}

pub fn get_buffers(env: JNIEnv, jo_vector: JObject) -> Vec<Buffer> {
    println!("get_buffers");
    let mut buffers = Vec::new();
    let arg = [JValue::from(false)];
    let jv_buffers = env.call_method(jo_vector, "getBuffers", "(Z)[Lorg/apache/arrow/memory/ArrowBuf;", &arg).unwrap();
    let jo_buffers = jv_buffers.l().unwrap();

    let j_array = jo_buffers.into_inner() as jarray;
    let array_length = env.get_array_length(j_array).unwrap();
    //println!("array_length: {}", array_length);

    let j_array_object = j_array as jobjectArray;
    /*
    for i in 0..array_length {

        let jo_buffer = env.get_object_array_element(j_array_object, i).unwrap();
        let buffer = arrow_buf_to_rust_buffer(env, jo_buffer);
        buffers.push(buffer);
    }

    */
    let buf = get_buffer_by_getter(env, jo_vector, "getDataBuffer");
    buffers.push(buf);

    buffers
}

pub fn create_data_array(env: JNIEnv, jo_vector: JObject, datatype: DataType) -> ArrayData {
    println!("create_data_array");
    //let datatype = DataType::Struct(r_schema.fields().clone());
    let mut builder = ArrayDataBuilder::new(datatype);

    // We currently assume we don't have nesting - no children
    /*
    let children = get_children(env, jvalue_vector);

    // have children, recurse
    if children.len() != 0 {
        for i in..children.len() {

        }

    }

    */
    // Get buffers
    let buffers = get_buffers(env, jo_vector);
    builder = builder.buffers(buffers);

    // Get null buffer
    let null_buffer = get_buffer_by_getter(env, jo_vector, "getValidityBuffer");
    builder = builder.null_bit_buffer(null_buffer);
    /*
    // Get data buffer
    let data_buffer = get_buffers(env, jvalue_vector);
    builder = builder.buffers(data_buffer);
    */

    // Get number of elements
    let len = get_int_by_getter(env, jo_vector, "getValueCount") as usize;
    println!("len of column: {}", len);
    builder = builder.len(len);

    builder.build()
}