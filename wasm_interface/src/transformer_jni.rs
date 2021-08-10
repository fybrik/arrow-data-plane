use crate::types::{Pointer, Tuple, WasmModule, jptr};

use arrow::error::ArrowError;
use arrow::ipc;
use arrow::ipc::reader::StreamReader;
use libc::c_void;
use jni::JNIEnv;
use jni::objects::{JByteBuffer, JClass, JList, JObject, JString, JValue};

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
use wasmer::{Cranelift, Instance, Module, Store, Universal, imports};

use std::convert::TryFrom;
use std::io::{BufReader, Cursor, Read};
use std::ptr::NonNull;
use std::sync::Arc;
use serde_json::Value;
use std::mem;
use arrow::array::{Array, Int32Array, Int64Array, make_array, make_array_from_raw};

use crate::transformer;

#[no_mangle]
pub extern "system" fn Java_org_m4d_adp_transform_TransformInterface_GetFFIArrowArray(_env: JNIEnv,_class: JClass, ffi_ptr: jptr) -> jptr {
    let ffi_tuple = Into::<Pointer<Tuple>>::into(ffi_ptr).borrow();
    let pffiaa = ((*ffi_tuple).0) as *const FFI_ArrowArray;
    let ret_ffiaa_ptr: i64 = Pointer::new(pffiaa).into();
    ret_ffiaa_ptr
}
#[no_mangle]
pub extern "system" fn Java_org_m4d_adp_transform_TransformInterface_GetFFIArrowSchema(_env: JNIEnv,_class: JClass, ffi_ptr: jptr) -> jptr {
    let ffi_tuple = Into::<Pointer<Tuple>>::into(ffi_ptr).borrow();
    let pffias = ((*ffi_tuple).1) as *const FFI_ArrowSchema;
    let ret_ffias_ptr: i64 = Pointer::new(pffias).into();
    ret_ffias_ptr
}


#[no_mangle]
pub extern "system" fn Java_org_m4d_adp_transform_TransformInterface_convertVSR2FFI(env: JNIEnv,
                                                                                    _class: JClass,
                                                                                    vsr_org: JObject, wasm_module_ptr: jptr)
                                                                                    -> jptr {

unsafe{println!("jo_vectors_list = {:?}", vsr_org.as_ref().unwrap() as *const _ as i64)};
transformer::try_ipc();    
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
    // println!("rust rs_json = {:?}", rs_json);    
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

    // let vectors = transformer::get_vectors(env, jo_vectors_list, r_schema.fields().clone());
    // let arrays: Vec<_> = vectors.iter().map(|x| StructArray::from(x.clone())).collect();

    // let mut pairs: Vec<(Field, ArrayRef)> = Vec::new();
    let pairs = transformer::get_vectors(env, jo_vectors_list, r_schema.fields().clone());
    // for i in 0..arrays.len() {
    //     pairs.push((fields[i].clone(), arrow::array::make_array(vectors[i].clone())));
    // }
    // create struct array
    let struct_array_org = StructArray::from(pairs);
    let (ffiaa, ffias) =  struct_array_org.to_raw().unwrap();


   
let tmp = unsafe{&(*ffiaa)};
let tmp_ffias = unsafe{&(*ffias)};
println!("deref ffiaa = {:?}\n,, ffias = {:?}", tmp, tmp_ffias);
println!("format = {:?}", tmp_ffias.format().chars().next().unwrap() as i64);


    let wasm_module = Into::<Pointer<WasmModule>>::into(wasm_module_ptr).borrow();
    let instance = &wasm_module.instance;
    let create_tuple_ptr_wasm = instance.exports.get_function("create_tuple_ptr").unwrap().native::<(i64, i64), i64>().unwrap();
    let create_empty_ffi_arrow_wasm = instance.exports.get_function("create_empty_ffi_arrow").unwrap().native::<(),i64>().unwrap();
    let get_ffiaa_wasm = instance.exports.get_function("get_ffiaa").unwrap().native::<(i64),i64>().unwrap();
    let get_ffias_wasm = instance.exports.get_function("get_ffias").unwrap().native::<(i64),i64>().unwrap();
    let transform_wasm = instance.exports.get_function("transform2").unwrap().native::<(i64, i64), i64>().unwrap();
    let write_field_wasm = instance.exports.get_function("write_field").unwrap().native::<(i64, i64), ()>().unwrap();
    let empty_ffi_ptr = create_empty_ffi_arrow_wasm.call().unwrap();
    let empty_ffiaa_ptr = get_ffiaa_wasm.call(empty_ffi_ptr).unwrap();
    let empty_ffias_ptr = get_ffias_wasm.call(empty_ffi_ptr).unwrap();
    println!("convert vsr to ffi empty, ffiaa = {:?},, ffias = {:?}", empty_ffiaa_ptr, empty_ffias_ptr);
// let empty_ffiaa = unsafe{&(*(empty_ffiaa_ptr as *const FFI_ArrowArray))};

let memory = instance.exports.get_memory("memory").unwrap();
let mem_ptr = memory.data_ptr();
// println!("transform call wasm,, mem ptr = {:?}", (mem_ptr as i64 + empty_ffiaa_ptr));
// let add_buff = unsafe{std::ptr::read(((ffiaa as i64) + 8*6) as *const i64)};
// unsafe{println!("before writing = {:?},, schema = ", std::ptr::read(((ffiaa as i64) + 8*6) as *const i64))};
// unsafe{println!("before writing = {:?},, schema = ", std::ptr::read(add_buff as *mut *const ::std::os::raw::c_void))};


////////////////////////////////////////////////////////////////////////
let addr = empty_ffiaa_ptr as i64;
let addr_src = ffiaa as i64;

transformer::fill_ffi_arrow(addr_src, addr, 10, write_field_wasm.clone());


let addr = empty_ffias_ptr as i64;
let addr_src = ffias as i64;
transformer::fill_ffi_arrow(addr_src, addr, 9, write_field_wasm.clone());



// unsafe{println!("transform call wasm,, mem ptr = {:?},, ffias = {:?}", std::ptr::read((mem_ptr as i64 + empty_ffiaa_ptr) as *const FFI_ArrowArray), std::ptr::read((mem_ptr as i64 + empty_ffias_ptr) as *const FFI_ArrowSchema))};

// write_field_wasm.call(addr as i64, ffiaa as i64).unwrap();
// println!("write field call");




let ffi_transformed_ptr = transform_wasm.call(empty_ffiaa_ptr as i64, empty_ffias_ptr as i64).unwrap();
println!("transform after call len {:?}", ffi_transformed_ptr);

    let ret_tuple_ptr = create_tuple_ptr_wasm.call(empty_ffiaa_ptr as i64, empty_ffias_ptr as i64).unwrap();
    println!("convert vsr to ffi tuple ptr = {:?}, ffiaa = {:?},, ffias = {:?}", ret_tuple_ptr, ffiaa, ffias);
    // let ret_tuple = Tuple(ffiaa as i64, ffias as i64);
    // let ret_tuple_ptr = Pointer::new(ret_tuple).into();


    ret_tuple_ptr
}

#[no_mangle]
pub extern "system" fn Java_org_m4d_adp_transform_TransformInterface_createOrUpdateVSR(env: JNIEnv,
                                                                                    _class: JClass,
                                                                                    ffi_ptr: jptr,
                                                                                    vsr_target: JObject, wasm_module_ptr: jptr)
                                                                                    -> jobject {
    let record = transformer::c_data_interface_to_record_batch(ffi_ptr, wasm_module_ptr);
    println!("create or update record = {:?}", record);
    let vsr : JObject;
    if vsr_target.is_null() {
        //not working!!
        // println!("create or update schema = {:?}", record.schema());
        let schema = JObject::from(transformer::from_rust_schema_to_java_schema(env, record.schema()));
        // For now use the null allocator
        let null_allocator = JObject::null();
        vsr = env.call_static_method(
            "org/apache/arrow/vector/VectorSchemaRoot",
            "create",
             "(org/apache/arrow/vector/types/pojo/Schema;Ljava/lang/Object;)org/apache/arrow/vector/VectorSchemaRoot",
                 &[JValue::Object(schema), JValue::Object(null_allocator)]
        )
            .unwrap()
            .l()
            .unwrap();
    } else {
        println!("create or update inside else1");
        vsr = vsr_target;
    }
    transformer::load_record_batch_to_vsr(env, vsr, &record);
        println!("create or update 2");
        vsr.into_inner()
}


#[no_mangle]
pub extern "system" fn Java_org_m4d_adp_transform_TransformInterface_transformation2(_env: JNIEnv,_class: JClass, ffi_ptr: jptr, java_vec: JObject) -> jptr {
    let ffi_tuple = Into::<Pointer<Tuple>>::into(ffi_ptr).borrow();
    let pffiaa = ((*ffi_tuple).0) as *const FFI_ArrowArray;
    let pffias = ((*ffi_tuple).1) as *const FFI_ArrowSchema;
    let arrow_array = unsafe{ArrowArray::try_from_raw(pffiaa, pffias).unwrap()};
    let array_data = arrow_array.to_data().unwrap();
    // let struct_array: StructArray = StructArray::from(array_data);
    let struct_array : StructArray = array_data.into();
    let record = RecordBatch::from(&struct_array);
    // Transformation
    let transformed_record = transformer::transform_record_batch(record, java_vec, _env);
    // let transformed_record = record;
    // Transformation
    let struct_array2: StructArray = transformed_record.into();
    let (ffiaa, ffias) = struct_array2.to_raw().unwrap();
    let ret_tuple = Tuple(ffiaa as i64, ffias as i64);
    let ret_tuple_ptr: i64 = Pointer::new(ret_tuple).into();
    mem::forget(arrow_array);
    ret_tuple_ptr
}

#[no_mangle]
pub extern "system" fn Java_org_m4d_adp_transform_TransformInterface_transformation(_env: JNIEnv,_class: JClass, ffi_ptr: jptr, wasm_module_ptr: jptr, java_vec: JObject) -> jptr {
    println!("transform");
    // let wasm_bytes = std::fs::read("../wasm-modules/transform/target/wasm32-unknown-unknown/release/wasm_transform.wasm").unwrap();
    // let store = Store::new(&Universal::new(Cranelift::default()).engine());
    // println!("Compiling module...");
    // // Let's compile the Wasm module.
    // let module = Module::new(&store, wasm_bytes).unwrap();
    // let import_object = imports! {
        //   "env" => {
            //   }
    // };
            // let instance = Instance::new(&module, &import_object).unwrap();
            

// let tmp = unsafe{*(ffi_ptr as *const i64)};
// let tmp = unsafe{std::ptr::read(ffi_ptr as *const i64)};
// let tmp = unsafe{std::ptr::read(tmp as *const i64)};
let f = Field::new("name", DataType::Int32, true);
let int_array = Int32Array::from(vec![6,7,8,9,10]);
    let int_array_data = make_array(int_array.data().clone());
    let int_array2 = Int32Array::from(vec![6,7,8,9,10]);
    let int_array_data2 = make_array(int_array2.data().clone());
    let int_array3 = Int32Array::from(vec![6,7,8,9,10]);
    let int_array_data3 = make_array(int_array3.data().clone());
    let int_array4 = Int32Array::from(vec![6,7,8,9,10]);
    let int_array_data4 = make_array(int_array4.data().clone());
    // println!("arr");
    let array = StructArray::try_from(vec![(f.clone(), int_array_data), (f.clone(), int_array_data2), (f.clone(), int_array_data3), (f.clone(), int_array_data4)]).unwrap();
    let (new_ffiaa, new_ffias) =  array.to_raw().unwrap();
let new_addr = new_ffiaa as i64;
let new_addr = new_addr + 8*6;    
let tmp6 = unsafe{std::ptr::read(new_addr as *const i64)};
let new_addr = new_addr + 8*5;    
let tmp5 = unsafe{std::ptr::read(new_addr as *const i64)};
// println!("new array = {:?}", array);
mem::forget(array);



// println!("tmp = {:?}", tmp);
    // let ffi_tuple = Into::<Pointer<Tuple>>::into(ffi_ptr).borrow();
    // let pffiaa = ((*ffi_tuple).0) as *const FFI_ArrowArray;
    // let pffias = ((*ffi_tuple).1) as *const FFI_ArrowSchema;
    // println!("transformation ffiaa = {:?},, ffias = {:?}", pffiaa, pffias);
    let wasm_module = Into::<Pointer<WasmModule>>::into(wasm_module_ptr).borrow();
    println!("transform wasm bytes");
    let instance = &wasm_module.instance;
    // let create_ffi_arrow_wasm = instance.exports.get_function("create_empty_ffi_arrow").unwrap().native::<(),i64>().unwrap();
    let transform_wasm = instance.exports.get_function("transform").unwrap().native::<(i64, i64, i64), i64>().unwrap();
    // let transform_wasm = instance.exports.get_function("transform2").unwrap().native::<(i64, i64), i64>().unwrap();
    let memory = instance.exports.get_memory("memory").unwrap();
    let mem_ptr = memory.data_ptr();
    // let ffi_ptr2 = create_ffi_arrow_wasm.call().unwrap();
    println!("transform call wasm,, mem ptr = {:?},, ffi_ptr = {:?}", mem_ptr, ffi_ptr);
    let ffi_transformed_ptr = transform_wasm.call(ffi_ptr as i64, tmp5, tmp6).unwrap();
    println!("transform after call {:?}", ffi_transformed_ptr);
    ffi_transformed_ptr
    // ffi_ptr
}


#[no_mangle]
pub extern "system" fn Java_org_m4d_adp_transform_TransformInterface_writeByteBuffer(_env: JNIEnv,_class: JClass, byte_array: JObject, wasm_module_ptr: jptr, address: jlong, size: jlong) {
    let wasm_module = Into::<Pointer<WasmModule>>::into(wasm_module_ptr).borrow();
    let instance = &wasm_module.instance;
    let memory = instance.exports.get_memory("memory").unwrap();
    let wasm_mem_address = memory.data_ptr();

    println!("write buffer addr = {:?},, wasm mem = {:?}", address, wasm_mem_address);
    let addr = wasm_mem_address as i64 + address;
    // let j_buffer = JByteBuffer::from(byte_array);
    let vec = _env.convert_byte_array(byte_array.into_inner()).unwrap();
    // println!("j_buffer = {:?}", vec);
    // unsafe{std::ptr::write(addr as *mut Vec<u8>, vec)};
    let write_field_wasm = instance.exports.get_function("write_field_u8").unwrap().native::<(i64, u8), ()>().unwrap();
    // println!("vec[0] = {:?}", 6);
    for (i, v) in vec.iter().enumerate() {
        let dst_addr = address + i as i64;
        write_field_wasm.call(dst_addr, *v).unwrap();
    }

    // let bytes_array = unsafe{std::ptr::read(addr as *mut Vec<u8>)};
    // // let bytes_array = unsafe{ Vec::from_raw_parts(addr as *mut u8, size as usize, size as usize) };
    // println!("from raw parts = {:?}", bytes_array);
    println!("written");
}

#[no_mangle]
pub extern "system" fn Java_org_m4d_adp_transform_TransformInterface_transformationIPC(_env: JNIEnv,_class: JClass, wasm_module_ptr: jptr, wasm_mem_address2: jlong, address: jlong, size: jlong) -> jptr {
    println!("\ntransform IPC,, size = {:?}", size);
    let wasm_module = Into::<Pointer<WasmModule>>::into(wasm_module_ptr).borrow();
    let instance = &wasm_module.instance;
    // let create_tuple_wasm = instance.exports.get_function("create_tuple_ptr").unwrap().native::<(i64, i64), i64>().unwrap();
    let read_bytes_wasm = instance.exports.get_function("read_from_bytes").unwrap().native::<(i64, i64), i64>().unwrap();
    let get_first_of_tuple = instance.exports.get_function("get_first_of_tuple").unwrap().native::<i64, i64>().unwrap();
    let get_second_of_tuple = instance.exports.get_function("get_second_of_tuple").unwrap().native::<i64, i64>().unwrap();
    
    println!("tuple");
    // let bytes_tuple_ptr = create_tuple_wasm.call(address as i64, size as i64).unwrap();
    // println!("tuple = {:?},, address = {:?},, mem addr = {:?}", bytes_tuple_ptr, address, wasm_mem_address2);
    let memory = instance.exports.get_memory("memory").unwrap();
    let wasm_mem_address = memory.data_ptr();
    println!("transform wasm mem = {:?}, address = {:?}", wasm_mem_address, address);
    // let bytes_array: Vec<u8> = unsafe{ Vec::from_raw_parts((address + wasm_mem_address2 as i64) as *mut _, size as usize, size as usize) };
    // println!("size rust = {:?}", bytes_array.len());

    // ////
    // // let mut bytes_array = Vec::new();
    // // for i in 0..100 {
    // //     print!("{:?}, ", bytes_array[i]);
    // //     // let src_addr = address + wasm_mem_address as i64 + i as i64;
    // //     // print!("{:?}, ", (unsafe{std::ptr::read(src_addr as *mut u8)}));
    // // }
    // println!("tuple {:?}", bytes_array.len());
    // let cursor = Cursor::new(bytes_array);
    // println!("cursor = {:?}", cursor);


    // // let reader = StreamReader::try_new(cursor).unwrap();
    // println!("tuple");
    ////
    // let mut ret_ptr = 0;
    // reader.for_each(|batch| {
    //     let batch = batch.unwrap();
    //     println!("reader = {:?}", batch);
    // });
    let transformed_tuple = read_bytes_wasm.call(address as i64, size as i64).unwrap();
    // println!("u8 = {:?}", transformed_tuple);

    // let transformed_ptr_len = Into::<Pointer<Tuple>>::into(transformed_tuple).borrow();
    let transformed_ptr = get_first_of_tuple.call(transformed_tuple).unwrap();
    let transformed_len = get_second_of_tuple.call(transformed_tuple).unwrap();
    let memory = instance.exports.get_memory("memory").unwrap();
    let wasm_mem_address = memory.data_ptr();
    println!("transformed ptr = {:?},, transformed len = {:?}", transformed_ptr, transformed_len);
    // let transformed_bytes_array: Vec<u8> = unsafe{ Vec::from_raw_parts((transformed_ptr + wasm_mem_address as i64) as *mut _, transformed_len as usize, transformed_len as usize) };
    // println!("transformed array = {:?}", transformed_bytes_array);
    // mem::forget(transformed_bytes_array);
    let ret_tuple = Tuple(transformed_ptr + wasm_mem_address as i64, transformed_len as i64);
    let ret_tuple_ptr = Pointer::new(ret_tuple).into();
    ret_tuple_ptr
    // transformed_ptr + wasm_mem_address as i64

    // // // let mut transformed_bytes_array = Vec::new();
    // // // for i in 0..size {
    // // //     let src_addr = transformed_addr + wasm_mem_address as i64 + i;
    // // //     transformed_bytes_array.push(unsafe{std::ptr::read(src_addr as *mut u8)});
    // // // }
    // // println!("transformed bytes array = {:?}", transformed_bytes_array);
    // // let cursor = Cursor::new(transformed_bytes_array);
    // // let reader = StreamReader::try_new(cursor).unwrap();
    // // reader.for_each(|batch| {
    // //     let batch = batch.unwrap();
    // //     println!("reader = {:?}", batch);
    // // });

}

#[no_mangle]
pub extern "system" fn Java_org_m4d_adp_transform_TransformInterface_GetFirstElemOfTuple(_env: JNIEnv,_class: JClass, tuple_ptr: jptr) -> jptr {
    let tuple = Into::<Pointer<Tuple>>::into(tuple_ptr).borrow();
    (*tuple).0
}

#[no_mangle]
pub extern "system" fn Java_org_m4d_adp_transform_TransformInterface_GetSecondElemOfTuple(_env: JNIEnv,_class: JClass, tuple_ptr: jptr) -> jptr {
    let tuple = Into::<Pointer<Tuple>>::into(tuple_ptr).borrow();
    (*tuple).1
}
