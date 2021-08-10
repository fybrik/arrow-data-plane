use crate::types::{Pointer, Tuple, WasmModule, jptr};

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
    // util::pretty::print_batches,
    record_batch::RecordBatch
};

use arrow::ffi::FFI_ArrowArray;
use arrow::ffi::FFI_ArrowSchema;
use arrow::ffi::ArrowArray;
use arrow::ffi::ArrowArrayRef;
use wasmer::{Cranelift, Instance, Module, NativeFunc, Store, Universal, imports};

use std::ptr::NonNull;
use std::sync::Arc;
use serde_json::Value;
use std::mem;
use arrow::array::{Array, Int16Array, Int32Array, Int64Array, make_array_from_raw};
use arrow::datatypes::SchemaRef;

pub fn get_int_by_getter(env: JNIEnv, object: JObject, getter: &str) -> i32 {
    let jv_size = env.call_method(object, getter, "()I", &[]).unwrap();
    let r_size= jv_size.i().unwrap();
    r_size
}

pub fn get_vectors(env: JNIEnv, jv_vectors_list: JObject, fields: Vec<Field>) -> Vec<(Field, ArrayRef)> {
    // let mut vec = Vec::new();
    let r_size = get_int_by_getter(env, jv_vectors_list, "size") as usize;
    let mut pairs: Vec<(Field, ArrayRef)> = Vec::new();

    for i in 0..r_size {
        let jv_i = [JValue::Int(i as jint)];
        let jv_vector = env.call_method(jv_vectors_list, "get", "(I)Ljava/lang/Object;", &jv_i).unwrap();
        let jo_vector = jv_vector.l().unwrap();
        let field = &fields[i];
        let r_array = create_data_array(env, jo_vector, field.data_type().clone());
        // vec.push(r_array);
        pairs.push((fields[i].clone(), arrow::array::make_array(r_array)));
    }
    // vec
    pairs
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
    env.call_method(vsr, "setRowCount", "(I)V", &[JValue::Int(row_count as i32)]).unwrap();
    println!("load_record_batch22");
}

pub fn from_rust_schema_to_java_schema<'a>(env: JNIEnv, r_schema: SchemaRef) -> jobject {
    let json = r_schema.to_json();
    let json_string = serde_json::to_string(&json).unwrap();
    let j_string = JValue::from(env.new_string(json_string).unwrap());
    // env.call_static_method("org/m4d/adp/transform/TransformInterface", "tmpFunc", "(Ljava/lang/String;)V", &[j_string]).unwrap();
    // println!("after calling tmpFunc");
    
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

pub fn c_data_interface_to_record_batch(ffi_ptr: jptr, wasm_module_ptr: jptr) -> RecordBatch {
    let wasm_module = Into::<Pointer<WasmModule>>::into(wasm_module_ptr).borrow();
    let instance = &wasm_module.instance;
    let get_ffiaa_wasm = instance.exports.get_function("get_ffiaa").unwrap().native::<(i64),i64>().unwrap();
    let get_ffias_wasm = instance.exports.get_function("get_ffias").unwrap().native::<(i64),i64>().unwrap();
    let memory = instance.exports.get_memory("memory").unwrap();
    let mem_ptr = memory.data_ptr();
    let pffiaa = (get_ffiaa_wasm.call(ffi_ptr).unwrap() + (mem_ptr as i64)) as *const FFI_ArrowArray;
    let pffias = (get_ffias_wasm.call(ffi_ptr).unwrap() + (mem_ptr as i64)) as *const FFI_ArrowSchema;
println!("c to record ffiaa = {:?},, ffias = {:?}", pffiaa, pffias);
    // let ffi_tuple = Into::<Pointer<Tuple>>::into(ffi_ptr).borrow();
    // let pffiaa = ((*ffi_tuple).0) as *const FFI_ArrowArray;
    // let pffias = ((*ffi_tuple).1) as *const FFI_ArrowSchema;
    // Build a record batch from the FFI pointers
    let arrow_array = unsafe{ArrowArray::try_from_raw(pffiaa, pffias).unwrap()};
    let array_data = arrow_array.to_data().unwrap();
    let struct_array : StructArray = array_data.into();
    println!("c to record struct array = {:?}", struct_array);
    let record = RecordBatch::from(&struct_array);
    mem::forget(arrow_array);
    record
}

pub fn transform_record_batch(record_in: RecordBatch, java_vec: JObject, env: JNIEnv) -> RecordBatch {
    // let num_cols = record_in.num_columns();
    // let num_rows = record_in.num_rows();
    // let data_type = record_in.schema().field(0).data_type().clone();
    // println!("transform record batch data type = {:?},, rows = {:?}", data_type, num_rows);
    // let mut array_builder = ArrayDataBuilder::new(data_type);
    // let buffer = Buffer::from(vec![0; num_rows]);
    // array_builder = array_builder.buffers(vec![buffer]);
    // // Get null buffer
    // let null_buffer = record_in.column(num_cols-1).data().null_buffer().unwrap();
    // array_builder = array_builder.null_bit_buffer(null_buffer.clone());

    // // Get number of elements
    // array_builder = array_builder.len(num_rows);
    
    // let array_data = array_builder.build();

    let array_data = create_data_array(env, java_vec, record_in.schema().field(0).data_type().clone());
    let struct_array = Int64Array::from(array_data);
    let new_column = Arc::new(struct_array);

    let columns: &[ArrayRef] = record_in.columns();
    let num_cols = record_in.num_columns();
    let first_columns = columns[0..num_cols-1].to_vec();
    let new_array = [first_columns, vec![new_column]].concat();
    let record_out = RecordBatch::try_new(
        record_in.schema(),
        new_array
    ).unwrap();
    return record_out;
}

pub fn fill_ffi_arrow(src_addr: i64, dst_addr: i64, num_of_fields: i64, write_field_wasm: NativeFunc<(i64, i64), ()>) {
    for i in 0..num_of_fields {
        let src_field = src_addr + 8*i;
        let dst_field = dst_addr + 8*i;
        let read_field = unsafe{std::ptr::read(src_field as *const i64)};
        write_field_wasm.call(dst_field, read_field).unwrap();
    }
}


pub fn try_ipc() {
    let wasm_bytes_file = std::fs::read("../wasm-modules/allocator/target/wasm32-unknown-unknown/release/alloc.wasm").unwrap();
    let store = Store::new(&Universal::new(Cranelift::default()).engine());
    // Compiling the Wasm module.
    let module = Module::new(&store, wasm_bytes_file).unwrap();
    let import_object = imports! {
        "env" => {}
    };
    let instance = Instance::new(&module, &import_object).unwrap();
    let write_batch = instance.exports.get_native_function::<(), i64>("write_batch_example").unwrap();
    let read_batch = instance.exports.get_native_function::<i64, ()>("read_from_bytes").unwrap();
    let bytes_ptr_len = write_batch.call().unwrap();
    read_batch.call(bytes_ptr_len).unwrap();
}