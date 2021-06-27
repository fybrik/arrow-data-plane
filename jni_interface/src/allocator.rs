use wasmer::{Cranelift, Instance, Module, Store, Universal, imports};
use crate::{types::{Pointer}};
// use libc::{c_void};


#[no_mangle]
pub fn wasmInstance(path: String) -> i64{
    let wasm_bytes_file = std::fs::read(path).unwrap();
    let store = Store::new(&Universal::new(Cranelift::default()).engine());

    // Compiling the Wasm module.
    let module = Module::new(&store, wasm_bytes_file).unwrap();
    let import_object = imports! {
        "env" => {}
    };
    let instance = Instance::new(&module, &import_object).unwrap();
    let instance_ptr = Pointer::new(instance).into();
    instance_ptr
}

#[no_mangle]
pub fn wasmDrop(instance_ptr: i64){
    let _: Pointer<Instance> = instance_ptr.into();
}

#[no_mangle]
pub extern fn wasmAlloc(instance_ptr: i64, size: i64) -> i64 {
    let instance = Into::<Pointer<Instance>>::into(instance_ptr).borrow();
    let alloc = instance.exports.get_native_function::<i64,i32>("alloc").unwrap();
    let result = alloc.call(size as i64).unwrap();
    result as i64
}

#[no_mangle]
pub extern fn wasmMemPtr(instance_ptr: i64) -> i64 {
    let instance = Into::<Pointer<Instance>>::into(instance_ptr).borrow();
    let memory = instance.exports.get_memory("memory").unwrap();
    let mem_ptr = memory.data_ptr();
    mem_ptr as i64
}

#[no_mangle]
pub extern fn wasmDealloc(instance_ptr: i64, offset: i64, size: i64){
    let instance = Into::<Pointer<Instance>>::into(instance_ptr).borrow();
    let dealloc = instance.exports.get_native_function::<(i64,i64),()>("dealloc").unwrap();
    dealloc.call(offset, size as i64).unwrap();
}