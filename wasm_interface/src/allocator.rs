use wasmer::{Cranelift, Instance, Module, Store, Universal, imports};
use crate::types::{WasmModule, Pointer};


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
    let alloc = instance.exports.get_native_function::<i64,i32>("alloc").unwrap();
    let dealloc = instance.exports.get_native_function::<(i64,i64),()>("dealloc").unwrap();
    let instance_ptr = Pointer::new(instance).into();
    let java_module = WasmModule{instance: instance_ptr, alloc_func: alloc, dealloc_func: dealloc};
    let to_return = Pointer::new(java_module).into();
    to_return
}

#[no_mangle]
pub fn wasmDrop(wasm_module_ptr: i64){
    let wasm_module: Pointer<WasmModule> = wasm_module_ptr.into();
    let _: Pointer<Instance> = wasm_module.instance;
}

#[no_mangle]
pub extern fn wasmAlloc(wasm_module_ptr: i64, size: i64) -> i64 {
    let wasm_module = Into::<Pointer<WasmModule>>::into(wasm_module_ptr).borrow();
    let alloc_func = &wasm_module.alloc_func;
    let result = alloc_func.call(size as i64).unwrap();
    result as i64
}

#[no_mangle]
pub extern fn wasmMemPtr(wasm_module_ptr: i64) -> i64 {
    let wasm_module = Into::<Pointer<WasmModule>>::into(wasm_module_ptr).borrow();
    let instance_ptr = &wasm_module.instance;
    let memory = instance_ptr.exports.get_memory("memory").unwrap();
    let mem_ptr = memory.data_ptr();
    mem_ptr as i64
}

#[no_mangle]
pub extern fn wasmDealloc(wasm_module_ptr: i64, offset: i64, size: i64){
    let wasm_module = Into::<Pointer<WasmModule>>::into(wasm_module_ptr).borrow();
    let dealloc_func = &wasm_module.dealloc_func;
    dealloc_func.call(offset, size as i64).unwrap();
}