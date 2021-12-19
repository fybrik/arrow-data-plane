use std::ops::Add;

use crate::{
    oci_cache,
    types::{Pointer, WasmTimeData},
};
use wasmtime::*;
use wasmtime_wasi::sync::WasiCtxBuilder;

// wasmtime
// A function that takes a path to OCI image and returns a Pointer of type WasmModule.
#[no_mangle]
pub fn wasmTimeData(wasm_images: Vec<String>) -> i64 {
    // let wasm_bytes_file = oci_cache::cached_pull_wasm_module(None, None, path).unwrap();
    let wasm_images = ["alloc.wasm", "filter.wasm", "filter.wasm"];
    let len = wasm_images.len();
    if len == 0 {
        println!("Error: number of wasm images must be larger than 0");
    }
    let mut wasm_transform_instances = Vec::new();
    // An engine stores and configures global compilation settings like
    // optimization level, enabled wasm features, etc.
    let engine = Engine::default();
    let mut linker = Linker::new(&engine);
    wasmtime_wasi::add_to_linker(&mut linker, |s| s).unwrap();
    // The first wasm module is responsible of allocations, it doesn't import memory
    let path1 = wasm_images.get(0).unwrap();
    let module1 = Module::from_file(&engine, path1).unwrap();
    let wasi = WasiCtxBuilder::new()
        .inherit_stdio()
        .inherit_args()
        .unwrap()
        .build();

    let mut store = Store::new(&engine, wasi);
    let wasm_alloc_instance = linker.instantiate(&mut store, &module1).unwrap();
    linker.instance(&mut store, "env", wasm_alloc_instance).unwrap();

    // Get alloc function
    let alloc_func = wasm_alloc_instance
        .get_func(&mut store, "alloc")
        .expect("`alloc` was not an exported function")
        .typed::<i64, i32, _>(&store)
        .unwrap();

    // Get dealloc function
    let dealloc_func = wasm_alloc_instance
        .get_func(&mut store, "dealloc")
        .expect("`dealloc` was not an exported function")
        .typed::<(i64, i64), (), _>(&store)
        .unwrap();

    let wasm_alloc_instance: Pointer<Instance> = Pointer::new(wasm_alloc_instance).into();
    // Create instances from the transformation wasm modules.
    // The transformation modules import the same memory as the allocation module
    for i in 1..len {
        let path = wasm_images.get(i).unwrap();
        let module = Module::from_file(&engine, path).unwrap();
        let instance = linker.instantiate(&mut store, &module).unwrap();
        let instance_ptr: Pointer<Instance> = Pointer::new(instance).into();
        wasm_transform_instances.push(instance_ptr);
    }
    
    let wasm_module = WasmTimeData {
        wasm_alloc_instance,
        wasm_transform_instances,
        alloc_func,
        dealloc_func,
        store,
    };
    let to_return = Pointer::new(wasm_module).into();
    to_return
}

// A function to release the WasmModule object that was allocated in `wasmInstance`.
#[no_mangle]
pub fn wasmDrop(wasm_data_ptr: i64) {
    let wasm_data: Pointer<WasmTimeData> = wasm_data_ptr.into();
    let _ = wasm_data.wasm_alloc_instance;
    let wasm_instances = &wasm_data.wasm_transform_instances;
    for _ in wasm_instances {}
}

// A function to allocate a memory chunk of a given size using the allocation function from the WASM module.
#[no_mangle]
pub extern "C" fn wasmAlloc(wasm_data_ptr: i64, size: i64) -> i64 {
    let wasm_data = Into::<Pointer<WasmTimeData>>::into(wasm_data_ptr).borrow();
    let alloc_func = &wasm_data.alloc_func;
    let store = &mut wasm_data.store;
    let result = alloc_func.call(store, size as i64);
    let result = result.unwrap();
    result as i64
}

// A function that returns the memory address of the memory of the WASM module.
#[no_mangle]
pub extern "C" fn wasmMemPtr(wasm_data_ptr: i64) -> i64 {
    let wasm_data = Into::<Pointer<WasmTimeData>>::into(wasm_data_ptr).borrow();
    let instance_ptr = &wasm_data.wasm_alloc_instance;
    let memory = instance_ptr
        .get_memory(&mut wasm_data.store, "memory")
        .unwrap();
    let mem_ptr = memory.data_ptr(&mut wasm_data.store);
    mem_ptr as i64
}

// A function to deallocate the memory chunk that starts at the given offset with the given size
// using the deallocation function from the Wasm Module.
#[no_mangle]
pub extern "C" fn wasmDealloc(wasm_data_ptr: i64, offset: i64, size: i64) {
    let wasm_data = Into::<Pointer<WasmTimeData>>::into(wasm_data_ptr).borrow();
    let dealloc_func = &wasm_data.dealloc_func;
    dealloc_func
        .call(&mut wasm_data.store, (offset, size as i64))
        .unwrap();
}
