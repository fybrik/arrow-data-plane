use crate::{
    allocator::{wasm_alloc, wasm_dealloc, wasm_drop, wasm_instance, wasm_mem_ptr},
    types::jptr,
};
use jni::objects::{JObject, JString};
use jni::sys::jlong;
use jni::JNIEnv;

// Wrapping the functions of `allocator.rs` with JNI to export them to JAVA.

#[no_mangle]
pub extern "C" fn Java_io_fybrik_adp_core_jni_JniWrapper_wasmInstance(
    jre: JNIEnv,
    _object: JObject,
    path: JString,
) -> jlong {
    let input: String = jre
        .get_string(path)
        .expect("Couldn't get java string!")
        .into();
    wasm_instance(input)
}

#[no_mangle]
pub extern "C" fn Java_io_fybrik_adp_core_jni_JniWrapper_wasmDrop(
    _jre: JNIEnv,
    _object: JObject,
    wasm_module_ptr: jptr,
) {
    wasm_drop(wasm_module_ptr);
}

#[no_mangle]
pub extern "C" fn Java_io_fybrik_adp_core_jni_JniWrapper_wasmAlloc(
    _jre: JNIEnv,
    _object: JObject,
    wasm_module_ptr: jptr,
    size: jlong,
) -> jlong {
    wasm_alloc(wasm_module_ptr, size)
}

#[no_mangle]
pub extern "C" fn Java_io_fybrik_adp_core_jni_JniWrapper_wasmMemPtr(
    _jre: JNIEnv,
    _object: JObject,
    wasm_module_ptr: jptr,
) -> jlong {
    wasm_mem_ptr(wasm_module_ptr)
}

#[no_mangle]
pub extern "C" fn Java_io_fybrik_adp_core_jni_JniWrapper_wasmDealloc(
    _jre: JNIEnv,
    _object: JObject,
    wasm_module_ptr: jptr,
    offset: jlong,
    size: jlong,
) {
    wasm_dealloc(wasm_module_ptr, offset, size);
}
