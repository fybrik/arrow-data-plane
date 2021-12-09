// Wrapping the functions of `allocator.rs` with JNI to export them to JAVA. 

use crate::{
    allocator::{wasmAlloc, wasmDealloc, wasmDrop, wasmTimeInstance, wasmMemPtr}, 
    types::jptr
};
use libc::c_void;
use jni::{JNIEnv, objects::JObject};
use jni::objects::JString;
use jni::sys::jlong;



#[no_mangle]
pub extern fn Java_org_m4d_adp_allocator_AllocatorInterface_wasmTimeInstance(jre: JNIEnv, _class: *const c_void, wasm_images: JObject) -> jlong {
    let mut images = Vec::new();
    // let wasm_image1 = jre.call_method(wasm_images, "get", "()Ljava/util/ArrayList;", &[jni::objects::JValue::Int(0)]).unwrap().l().unwrap();
    let wasm_images_list = jre.get_list(wasm_images).unwrap();
    for i in 0..wasm_images_list.size().unwrap() {
        let wasm_image = wasm_images_list.get(i).unwrap().unwrap();
        let wasm_image = JString::from(wasm_image);
        let wasm_image_str: String = jre.get_string(wasm_image).expect("Couldn't get java string!").into();
        images.push(wasm_image_str);
    }
    // println!("wasm time instance, input 1 = {:?}", input1);
    // let input2: String = jre.get_string(path2).expect("Couldn't get java string!").into();
    // wasmTimeInstance(vec!["alloc.wasm", "transformation.wasm"])
    wasmTimeInstance(images)
}

// #[no_mangle]
// pub extern fn Java_org_m4d_adp_allocator_AllocatorInterface_wasmInstance(jre: JNIEnv, _class: *const c_void, path: JString) -> jlong{
//     let input: String = jre.get_string(path).expect("Couldn't get java string!").into();
//     wasmInstance(input)
// }

#[no_mangle]
pub extern fn Java_org_m4d_adp_allocator_AllocatorInterface_wasmDrop(_jre: JNIEnv, _class: *const c_void, wasm_module_ptr: jptr) {
    wasmDrop(wasm_module_ptr);
}

#[no_mangle]
pub extern fn Java_org_m4d_adp_allocator_AllocatorInterface_wasmAlloc(_jre: JNIEnv, _class: *const c_void, wasm_module_ptr: jptr, size: jlong) -> jlong {
    wasmAlloc(wasm_module_ptr, size)
}

#[no_mangle]
pub extern fn Java_org_m4d_adp_allocator_AllocatorInterface_wasmMemPtr(_jre: JNIEnv, _class: *const c_void, wasm_module_ptr: jptr) -> jlong {
    wasmMemPtr(wasm_module_ptr)
}

#[no_mangle]
pub extern fn Java_org_m4d_adp_allocator_AllocatorInterface_wasmDealloc(_jre: JNIEnv, _class: *const c_void, wasm_module_ptr: jptr, offset: jlong, size: jlong) {
    wasmDealloc(wasm_module_ptr, offset, size);
}