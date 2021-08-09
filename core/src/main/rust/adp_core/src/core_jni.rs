use crate::{
    types::jptr,
};
use crate::types::{Pointer};
use jni::objects::JObject;
use jni::sys::{jlong, jbyteArray};
use jni::JNIEnv;

#[no_mangle]
pub extern "C" fn Java_io_fybrik_adp_core_jni_JniWrapper_newInstance(
    jre: JNIEnv,
    _object: JObject,
    module_bytes: jbyteArray,
) -> jptr {
    let instance = CoreInstance{}
    Pointer::new(instance).into();
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
