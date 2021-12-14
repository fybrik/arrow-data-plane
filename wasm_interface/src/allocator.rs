use std::ops::Add;

// use wasmer::{Cranelift, Instance, Module, Store, Universal, imports};
// use wasmer_wasi::WasiState;
use crate::{oci_cache, types::{Pointer, WasmTimeModule}};
use wasmtime::*;
use wasmtime_wasi::sync::WasiCtxBuilder;

// // A function that takes a path to OCI image and returns a Pointer of type WasmModule.
// #[no_mangle]
// pub fn wasmInstance(path: String) -> i64 {
//     // Get the Wasm image from the OCI registry according to the path input
//     // let wasm_bytes_file = oci_cache::cached_pull_wasm_module(None, None, path).unwrap();
//     let wasm_bytes_file = std::fs::read("alloc.wasm").unwrap();
//     let store = Store::new(&Universal::new(Cranelift::default()).engine());
//     // Compiling the Wasm module.
//     let module = Module::new(&store, wasm_bytes_file).unwrap();
//     let import_object = imports! {
//         "env" => {}
//     };
//     // let mut wasi_env = WasiState::new("transformer").finalize().unwrap();
//     // let import_object = wasi_env.import_object(&module).unwrap();
//     let instance = Instance::new(&module, &import_object).unwrap();
//     let alloc = instance.exports.get_native_function::<i64,i32>("alloc").unwrap();
//     let dealloc = instance.exports.get_native_function::<(i64,i64),()>("dealloc").unwrap();
//     let instance_ptr = Pointer::new(instance).into();
//     // Fill a WasmModule object with module's instance, alloc function, and dealloc function. 
//     let wasm_module = WasmModule{instance: instance_ptr, alloc_func: alloc, dealloc_func: dealloc};
//     let to_return = Pointer::new(wasm_module).into();
//     to_return
// }

// wasmtime
// A function that takes a path to OCI image and returns a Pointer of type WasmModule.
#[no_mangle]
pub fn wasmTimeInstance(wasm_images: Vec<String>) -> i64 {
    println!("wasm time instance, wasm images = {:?}", wasm_images);
    let wasm_images = ["alloc.wasm", "transformation.wasm"];
    // let path1 = "alloc.wasm";
    // let path2 = "transformation.wasm";
    let len = wasm_images.len();
    if len == 0 {
        println!("Error: number of wasm images must be larger than 0");
    }
    let mut wasm_instances = Vec::new();
    // An engine stores and configures global compilation settings like
    // optimization level, enabled wasm features, etc.
    let engine = Engine::default();
    let mut linker = Linker::new(&engine);
    wasmtime_wasi::add_to_linker(&mut linker, |s| s).unwrap();
    // We start off by creating a `Module` which represents a compiled form
    // of our input wasm module. In this case it'll be JIT-compiled after
    // we parse the text format.
    let path1 = wasm_images.get(0).unwrap();
    let module1 = Module::from_file(&engine, path1).unwrap();
    // let module2 = Module::from_file(&engine, path2).unwrap();
    let wasi = WasiCtxBuilder::new()
        .inherit_stdio()
        .inherit_args().unwrap()
        .build();

    let mut store = Store::new(&engine, wasi);
    let instance1 = linker.instantiate(&mut store, &module1).unwrap();
    linker.instance(&mut store, "env", instance1).unwrap();

    
    // let instance0 = linker.instantiate(&mut store, &module1).unwrap();
    // let instance = Instance::new(&mut store, &module, &[]).unwrap();

    // let memory1 = instance1
    //     .get_memory(&mut store, "memory").unwrap();
    let memory1 = instance1
        .get_memory(&mut store, "memory").unwrap();
    println!("memory = {:?}", memory1.size(&store));
    let mem_ptr = memory1.data_ptr(&store);
    // Get alloc function
    let alloc_func = instance1.get_func(&mut store, "alloc")
    .expect("`alloc` was not an exported function").typed::<i64, i32, _>(&store).unwrap();
    println!("alloc func instance = {:?}", &alloc_func as *const _ as u64);

    // Get dealloc function
    let dealloc_func = instance1.get_func(&mut store, "dealloc")
    .expect("`dealloc` was not an exported function").typed::<(i64, i64), (), _>(&store).unwrap();

    
    let instance_ptr1: Pointer<Instance> = Pointer::new(instance1).into();
    wasm_instances.push(instance_ptr1);
    // With a compiled `Module` we can then instantiate it, creating
    // an `Instance` which we can actually poke at functions on.
    for i in 1..len {
        let path = wasm_images.get(i).unwrap();
        let module = Module::from_file(&engine, path).unwrap();
        let instance = linker.instantiate(&mut store, &module).unwrap();
        let instance_ptr: Pointer<Instance> = Pointer::new(instance).into();
        wasm_instances.push(instance_ptr);
    }
    // let instance_ptr2: Pointer<Instance> = Pointer::new(wasm_instances.get(0).unwrap().clone()).into();
    // Fill a WasmModule object with module's instance, alloc function, and dealloc function. 
    // let x = wasm_instances[1];
    let wasm_module = WasmTimeModule{wasm_instances, alloc_func, dealloc_func, store};
    let to_return = Pointer::new(wasm_module).into();
    to_return
}

// A function to release the WasmModule object that was allocated in `wasmInstance`.
#[no_mangle]
pub fn wasmDrop(wasm_module_ptr: i64) {
    let wasm_module: Pointer<WasmTimeModule> = wasm_module_ptr.into();
    let wasm_instances = &wasm_module.wasm_instances;
    // let iterator: &wasm_instances.iter();
    for _ in wasm_instances {
    }
    // let _: Pointer<Instance> = wasm_module.instance_ptr1;
    // let _: Pointer<Instance> = wasm_module.instance_ptr2;
}

// A function to allocate a memory chunk of a given size using the allocation function from the WASM module.
#[no_mangle]
pub extern fn wasmAlloc(wasm_module_ptr: i64, size: i64) -> i64 {
    let wasm_module = Into::<Pointer<WasmTimeModule>>::into(wasm_module_ptr).borrow();
    let alloc_func = &wasm_module.alloc_func;
    let store = &mut wasm_module.store;
    let result = alloc_func.call(store, size as i64);
    let result = result.unwrap();
    result as i64
}

// A function that returns the memory address of the memory of the WASM module.
#[no_mangle]
pub extern fn wasmMemPtr(wasm_module_ptr: i64) -> i64 {
    let wasm_module = Into::<Pointer<WasmTimeModule>>::into(wasm_module_ptr).borrow();
    let instance_ptr = wasm_module.wasm_instances.get(0).unwrap();
    let memory = instance_ptr.get_memory(&mut wasm_module.store, "memory").unwrap();
    let mem_ptr = memory.data_ptr(&mut wasm_module.store);
    mem_ptr as i64
}

// A function to deallocate the memory chunk that starts at the given offset with the given size 
// using the deallocation function from the Wasm Module.
#[no_mangle]
pub extern fn wasmDealloc(wasm_module_ptr: i64, offset: i64, size: i64){
    let wasm_module = Into::<Pointer<WasmTimeModule>>::into(wasm_module_ptr).borrow();
    let dealloc_func = &wasm_module.dealloc_func;
    dealloc_func.call(&mut wasm_module.store, (offset, size as i64)).unwrap();
    println!("wasm dealloc, ptr = {:?}, size = {:?}", offset, size);
}