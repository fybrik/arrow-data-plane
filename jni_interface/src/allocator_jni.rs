use crate::{
    allocator::{wasmAlloc, wasmDealloc, wasmDrop, wasmInstance, wasmMemPtr}, 
    types::{jptr}
};
use libc::{c_void};
use jni::JNIEnv;
use jni::objects::{JString};
use jni::sys::{jlong};



#[no_mangle]
pub extern fn Java_ibm_com_example_Adder_wasmInstance(jre: JNIEnv, _class: *const c_void, path: JString) -> jlong{
    let input: String = jre.get_string(path).expect("Couldn't get java string!").into();
    wasmInstance(input)
}

#[no_mangle]
pub extern fn Java_ibm_com_example_Adder_wasmDrop(_jre: JNIEnv, _class: *const c_void, instance_ptr: jptr){
    wasmDrop(instance_ptr);
}


#[no_mangle]
pub extern fn Java_ibm_com_example_Adder_wasmAlloc(_jre: JNIEnv, _class: *const c_void, instance_ptr: jptr, size: jlong) -> jlong {
    wasmAlloc(instance_ptr, size)
}

#[no_mangle]
pub extern fn Java_ibm_com_example_Adder_wasmMemPtr(_jre: JNIEnv, _class: *const c_void, instance_ptr: jptr) -> jlong {
    wasmMemPtr(instance_ptr)
}


#[no_mangle]
pub extern fn Java_ibm_com_example_Adder_wasmDealloc(_jre: JNIEnv, _class: *const c_void, instance_ptr: jptr, offset: jlong, size: jlong){
    wasmDealloc(instance_ptr, offset, size);
}