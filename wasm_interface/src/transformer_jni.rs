use crate::types::{Pointer, Tuple, WasmModule, jptr};

use jni::JNIEnv;
use jni::objects::JClass;

// This is just a pointer. We'll be returning it from our function. We
// can't return one of the objects with lifetime information because the
// lifetime checker won't let us.
use jni::sys::jlong;


#[no_mangle]
pub extern "system" fn Java_org_m4d_adp_transform_TransformInterface_transformationIPC(_env: JNIEnv,_class: JClass, wasm_module_ptr: jptr, address: jlong, size: jlong) -> jptr {
    // Get the wasm module instance and the functions we want to use
    let wasm_module = Into::<Pointer<WasmModule>>::into(wasm_module_ptr).borrow();
    let instance = &wasm_module.instance;
    let read_transform_write_from_bytes_wasm = instance.exports.get_function("read_transform_write_from_bytes").unwrap().native::<(i64, i64), i64>().unwrap();
    let get_first_of_tuple = instance.exports.get_function("get_first_of_tuple").unwrap().native::<i64, i64>().unwrap();
    let get_second_of_tuple = instance.exports.get_function("get_second_of_tuple").unwrap().native::<i64, i64>().unwrap();
    
    // Call the function that read the bytes in the `address` parameter and getting the appropriate record batch
    // Then, it makes a transformation, writes back the transformed record batch, and returns a tuple of `(address, len)` of the transformed batch
    let transformed_tuple = read_transform_write_from_bytes_wasm.call(address as i64, size as i64).unwrap();

    let transformed_ptr = get_first_of_tuple.call(transformed_tuple).unwrap();
    let transformed_len = get_second_of_tuple.call(transformed_tuple).unwrap();
    // Get updated wasm memory address
    let memory = instance.exports.get_memory("memory").unwrap();
    let wasm_mem_address = memory.data_ptr();
    // Return to Java a tuple with address to the transformed record batch and its length
    let ret_tuple = Tuple(transformed_ptr + wasm_mem_address as i64, transformed_len as i64);
    let ret_tuple_ptr = Pointer::new(ret_tuple).into();
    ret_tuple_ptr
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
