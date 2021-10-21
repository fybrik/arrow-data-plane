use std::ptr;

use wasmer::{Cranelift, Instance, Memory, MemoryType, Module, Store, Universal, imports};
use wasmer_wasi::WasiState;
use crate::{oci_cache, types::{WasmModule, Pointer}};



#[no_mangle]
pub fn wasmInstance(path: String) -> i64 {
    println!("gg");
    // let wasm_bytes_file = std::fs::read(path).unwrap();
    let wasm_bytes_file = std::fs::read("alloc.wasm").unwrap();
    ////oci////
    // let wasm_bytes_file = oci_cache::cached_pull_wasm_module(None, None, path).unwrap();
    ////oci////
    let store = Store::new(&Universal::new(Cranelift::default()).engine());
    // Compiling the Wasm module.
    let module = Module::new(&store, wasm_bytes_file).unwrap();
    let mut wasi_env = WasiState::new("transformer").finalize().unwrap();
    let import_object = wasi_env.import_object(&module).unwrap();
    let instance = Instance::new(&module, &import_object).unwrap();
    let alloc = instance.exports.get_native_function::<i64,i32>("alloc").unwrap();
    let dealloc = instance.exports.get_native_function::<(i64,i64),()>("dealloc").unwrap();
    let instance_ptr = Pointer::new(instance).into();
    // Fill a WasmModule object with module's instance, alloc function, and dealloc function. 
    let wasm_module = WasmModule{instance: instance_ptr, alloc_func: alloc, dealloc_func: dealloc};
    let to_return = Pointer::new(wasm_module).into();
    to_return
}



// // A function that takes a path to `.wasm` file and returns a Pointer of type WasmModule.
// #[no_mangle]
// pub fn wasmInstance(path: String) -> i64{
//     let wasm_bytes_file = std::fs::read(path).unwrap();
//     let store = Store::new(&Universal::new(Cranelift::default()).engine());
//     // Compiling the Wasm module.
//     let module = Module::new(&store, wasm_bytes_file).unwrap();
//     let mut wasi_env = WasiState::new("transformer").finalize().unwrap();
//     let import_object = wasi_env.import_object(&module).unwrap();
//     let instance = Instance::new(&module, &import_object).unwrap();
//     let alloc = instance.exports.get_native_function::<i64,i32>("alloc").unwrap();
//     let dealloc = instance.exports.get_native_function::<(i64,i64),()>("dealloc").unwrap();
//     let alloc_mem = instance.exports.get_memory("memory").unwrap();
//     let instance_ptr = Pointer::new(instance).into();
//     let host_mem = Memory::new(&Store::default(),MemoryType::new(17, Some(17), false)).unwrap();
//     let import_object = imports! {
//         "env" => {
//             // "sum" => sum_func,
//             "memory" => alloc_mem.clone(),
//         }
//     };
//     let mem_ptr = alloc_mem.data_ptr();
//     let alloc_ptr = alloc.call(100).unwrap();
//     let mem_addr = mem_ptr as i64 + alloc_ptr as i64;
//     unsafe { ptr::write(mem_addr as *mut i32, 12) };






//     // Fill a WasmModule object with module's instance, alloc function, and dealloc function. 
//     let wasm_module = WasmModule{instance: instance_ptr, alloc_func: alloc, dealloc_func: dealloc};
//     let to_return = Pointer::new(wasm_module).into();
//     to_return
// }

// A function to release the WasmModule object that was allocated in `wasmInstance`.
#[no_mangle]
pub fn wasmDrop(wasm_module_ptr: i64) {
    let wasm_module: Pointer<WasmModule> = wasm_module_ptr.into();
    let _: Pointer<Instance> = wasm_module.instance;
}

// A function to allocate a memory chunk of a given size using the allocation function from the WASM module.
#[no_mangle]
pub extern fn wasmAlloc(wasm_module_ptr: i64, size: i64) -> i64 {
    let wasm_module = Into::<Pointer<WasmModule>>::into(wasm_module_ptr).borrow();
    let alloc_func = &wasm_module.alloc_func;
    let result = alloc_func.call(size as i64).unwrap();
    result as i64
}

// A function that returns the memory address of the memory of the WASM module.
#[no_mangle]
pub extern fn wasmMemPtr(wasm_module_ptr: i64) -> i64 {
    let wasm_module = Into::<Pointer<WasmModule>>::into(wasm_module_ptr).borrow();
    let instance_ptr = &wasm_module.instance;
    let memory = instance_ptr.exports.get_memory("memory").unwrap();
    let mem_ptr = memory.data_ptr();
    mem_ptr as i64
}

// A function to deallocate the memory chunk that starts at the given offset with the given size 
// using the deallocation function from the Wasm Module.
#[no_mangle]
pub extern fn wasmDealloc(wasm_module_ptr: i64, offset: i64, size: i64){
    let wasm_module = Into::<Pointer<WasmModule>>::into(wasm_module_ptr).borrow();
    let dealloc_func = &wasm_module.dealloc_func;
    dealloc_func.call(offset, size as i64).unwrap();
}