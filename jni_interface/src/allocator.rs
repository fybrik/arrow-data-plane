use crate::{types::{Pointer, jptr}};
use libc::{c_void};
use jni::JNIEnv;
use jni::objects::{JString};
use wasmer::{Cranelift, Instance, JIT, Module, Store, imports};
use jni::sys::{jlong};



#[no_mangle]
pub extern fn Java_ibm_com_example_Adder_wasmInstance(_jre: JNIEnv, _class: *const c_void, path: JString) -> jlong{
    let input: String = _jre.get_string(path).expect("Couldn't get java string!").into();
    let wasm_bytes_file = std::fs::read(input).unwrap();
    let store = Store::new(&JIT::new(Cranelift::default()).engine());

    // Compiling the Wasm module.
    let module_file = Module::new(&store, wasm_bytes_file).unwrap();
    let import_object = imports! {
        "env" => {}
    };
    let instance_file = Instance::new(&module_file, &import_object).unwrap();
    let instance = Pointer::new(instance_file).into();
    instance
}

#[no_mangle]
pub extern fn Java_ibm_com_example_Adder_wasmDrop(_jre: JNIEnv, _class: *const c_void, instance_ptr: jptr){
    let _: Pointer<Instance> = instance_ptr.into();
}


#[no_mangle]
pub extern fn Java_ibm_com_example_Adder_wasmAlloc(_jre: JNIEnv, _class: *const c_void, instance_ptr: jptr, size: jlong) -> jlong {
    let instance_file = Into::<Pointer<Instance>>::into(instance_ptr).borrow();
    let alloc = (instance_file).exports.get_function("alloc").unwrap().native::<i64, i32>().unwrap();
    let result = alloc.call(size as i64).unwrap();
    result as i64
}

#[no_mangle]
pub extern fn Java_ibm_com_example_Adder_wasmMemPtr(_jre: JNIEnv, _class: *const c_void, instance_ptr: jptr) -> jlong {
    let instance_file = Into::<Pointer<Instance>>::into(instance_ptr).borrow();
    let memory = (instance_file).exports.get_memory("memory").unwrap();
    let ptr = memory.data_ptr();
    ptr as i64
}


#[no_mangle]
pub extern fn Java_ibm_com_example_Adder_wasmDealloc(_jre: JNIEnv, _class: *const c_void, instance_ptr: jptr, offset: jlong, size: jlong){
    let instance_file = Into::<Pointer<Instance>>::into(instance_ptr).borrow();
    let dealloc = (instance_file).exports.get_function("dealloc").unwrap().native::<(i64,i64),()>().unwrap();
    dealloc.call(offset, size as i64).unwrap();
}