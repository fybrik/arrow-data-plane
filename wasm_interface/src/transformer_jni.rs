use crate::types::{Pointer, WasmModule, jptr};

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
    
    // Call the function that read the bytes in the `address` parameter and getting the appropriate record batch
    // Then, it makes a transformation, writes back the transformed record batch, and returns a tuple of `(address, len)` of the transformed batch
    let transformed_tuple = read_transform_write_from_bytes_wasm.call(address as i64, size as i64).unwrap();
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
