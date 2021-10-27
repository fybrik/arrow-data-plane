use std::mem;

use crate::types::{Pointer, WasmModule, jptr};

use jni::JNIEnv;
use jni::objects::JClass;

// This is just a pointer. We'll be returning it from our function. We
// can't return one of the objects with lifetime information because the
// lifetime checker won't let us.
use jni::sys::jlong;
use serde_json::Value;


#[no_mangle]
pub extern "system" fn Java_org_m4d_adp_transform_TransformInterface_TransformationIPC(_env: JNIEnv,_class: JClass, wasm_module_ptr: jptr, address: jlong, length: jlong, confAddress: jlong, confSize: jlong) -> jptr {
    // Get the wasm module instance and the functions we want to use
    let wasm_module = Into::<Pointer<WasmModule>>::into(wasm_module_ptr).borrow();
    let instance = &wasm_module.instance;
    let read_transform_write_from_bytes_wasm = instance.exports.get_function("read_transform_write_from_bytes").unwrap().native::<(i64, i64, i64, i64), i64>().unwrap();


    ////conf////
    let memory = instance.exports.get_memory("memory").unwrap();
    let mem_ptr = memory.data_ptr();
    let conf_bytes_array: Vec<u8> = unsafe{ Vec::from_raw_parts(((mem_ptr as jlong) + confAddress) as *mut _, confSize as usize, confSize as usize) };
    // let json_str = match std::str::from_utf8(&conf_bytes_array) {
    //     Ok(v) => v,
    //     Err(e) => panic!("Invalid UTF-8 sequence: {}", e),
    // };
    let json_str = std::str::from_utf8(&conf_bytes_array).unwrap();
    // println!("conf str = {:?}", json_str);
    let json: Value = serde_json::from_str(json_str).unwrap();
    println!("json = {:?}", json);
    // Object({"columns": Array([String("age")])})
    // let x = json["value"].as_str().unwrap().parse::<i64>().unwrap();
    let x = json["value"].as_i64().unwrap();
    println!("json = {:?}", x);
    // println!("json = {:?}", json["data"][0]["transformations"][0]["action"]);
    mem::forget(conf_bytes_array);
    ////conf////
    

    
    // Call the function that read the bytes in the `address` parameter and getting the appropriate record batch
    // Then, it makes a transformation, writes back the transformed record batch, and returns a tuple of `(address, len)` of the transformed batch
    let transformed_tuple = read_transform_write_from_bytes_wasm.call(address as i64, length as i64, confAddress as i64, confSize as i64).unwrap();
    transformed_tuple
}

#[no_mangle]
pub extern "system" fn Java_org_m4d_adp_transform_TransformInterface_GetFirstElemOfTuple(_env: JNIEnv,_class: JClass, wasm_module_ptr: jptr, tuple_ptr: jptr) -> jptr {
    // Get the wasm module instance and the functions we want to use
    let wasm_module = Into::<Pointer<WasmModule>>::into(wasm_module_ptr).borrow();
    let instance = &wasm_module.instance;
    let get_first_of_tuple = instance.exports.get_function("get_first_of_tuple").unwrap().native::<i64, i64>().unwrap();
    get_first_of_tuple.call(tuple_ptr).unwrap()
}

#[no_mangle]
pub extern "system" fn Java_org_m4d_adp_transform_TransformInterface_GetSecondElemOfTuple(_env: JNIEnv,_class: JClass, wasm_module_ptr: jptr, tuple_ptr: jptr) -> jptr {
    // Get the wasm module instance and the functions we want to use
    let wasm_module = Into::<Pointer<WasmModule>>::into(wasm_module_ptr).borrow();
    let instance = &wasm_module.instance;
    let get_second_of_tuple = instance.exports.get_function("get_second_of_tuple").unwrap().native::<i64, i64>().unwrap();
    get_second_of_tuple.call(tuple_ptr).unwrap()
}

#[no_mangle]
pub extern "system" fn Java_org_m4d_adp_transform_TransformInterface_DropTuple(_env: JNIEnv,_class: JClass, wasm_module_ptr: jptr, tuple_ptr: jptr) {
    // Get the wasm module instance and the functions we want to use
    let wasm_module = Into::<Pointer<WasmModule>>::into(wasm_module_ptr).borrow();
    let instance = &wasm_module.instance;
    let drop_tuple_wasm = instance.exports.get_function("drop_tuple").unwrap().native::<i64, ()>().unwrap();
    drop_tuple_wasm.call(tuple_ptr).unwrap();
}
