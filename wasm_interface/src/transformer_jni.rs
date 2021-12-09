use std::ptr::null;

use crate::types::{Pointer, WasmTimeModule, jptr};

use jni::JNIEnv;
use jni::objects::{JClass, JString};
use libc::c_void;
// use wasmer::{Cranelift, Instance, Memory, MemoryType, Module, NativeFunc, Store, Universal, imports};

// This is just a pointer. We'll be returning it from our function. We
// can't return one of the objects with lifetime information because the
// lifetime checker won't let us.
use jni::sys::jlong;

// pub(crate) static mut ALLOC_FUNC: Option<NativeFunc<i64, i32>> = None;

#[no_mangle]
pub extern "system" fn Java_org_m4d_adp_transform_TransformInterface_TransformationIPC(_env: JNIEnv, _class: JClass, wasm_module_ptr: jptr, address: jlong, length: jlong, conf_address: jlong, conf_size: jlong, i: jlong) -> jptr {
    // Get the wasm module instance and the functions we want to use
    let wasm_module = Into::<Pointer<WasmTimeModule>>::into(wasm_module_ptr).borrow();
    let instance = wasm_module.wasm_instances.get(i as usize).unwrap();;
    // if i == 0 {
    //     instance = wasm_module.wasm_instances.get(0).unwrap();
    // } else {
    //     instance = wasm_module.wasm_instances.get(1).unwrap();
    //     // tuple
    //     // let tuple = address;
    // }

    let transform_func = instance.get_func(&mut wasm_module.store, "read_transform_write_from_bytes")
    .expect("`transform` was not an exported function").typed::<(i64, i64, i64, i64), i64, _>(&wasm_module.store).unwrap();
    // let read_transform_write_from_bytes_wasm = instance.exports.get_function("read_transform_write_from_bytes").unwrap().native::<(i64, i64, i64, i64), i64>().unwrap();
    
    // Call the function that read the bytes in the `address` parameter and getting the appropriate record batch
    // Then, it makes a transformation, writes back the transformed record batch, and returns a tuple of `(address, len)` of the transformed batch
    println!("transformation IPC");
    // let transformed_tuple = read_transform_write_from_bytes_wasm.call(address as i64, length as i64, conf_address as i64, conf_size as i64).unwrap();
    let transformed_tuple = transform_func.call(&mut wasm_module.store, (address as i64, length as i64, conf_address as i64, conf_size as i64)).unwrap();
    transformed_tuple
}

#[no_mangle]
pub extern "system" fn Java_org_m4d_adp_transform_TransformInterface_GetFirstElemOfTuple(_env: JNIEnv, _class: JClass, wasm_module_ptr: jptr, tuple_ptr: jptr) -> jptr {
    // Get the wasm module instance and the functions we want to use
    let wasm_module = Into::<Pointer<WasmTimeModule>>::into(wasm_module_ptr).borrow();
    // let instance = &wasm_module.instance_ptr1;
    let instance = wasm_module.wasm_instances.get(0).unwrap();
    // let get_first_of_tuple = instance.exports.get_function("get_first_of_tuple").unwrap().native::<i64, i64>().unwrap();
    let get_first_of_tuple = instance.get_func(&mut wasm_module.store, "get_first_of_tuple")
    .expect("`first` was not an exported function").typed::<i64, i64, _>(&wasm_module.store).unwrap();
    // get_first_of_tuple.call(tuple_ptr).unwrap()
    get_first_of_tuple.call(&mut wasm_module.store, tuple_ptr).unwrap()
}

#[no_mangle]
pub extern "system" fn Java_org_m4d_adp_transform_TransformInterface_GetSecondElemOfTuple(_env: JNIEnv, _class: JClass, wasm_module_ptr: jptr, tuple_ptr: jptr) -> jptr {
    // Get the wasm module instance and the functions we want to use
    // let wasm_module = Into::<Pointer<WasmModule>>::into(wasm_module_ptr).borrow();
    // let instance = &wasm_module.instance;
    // let get_second_of_tuple = instance.exports.get_function("get_second_of_tuple").unwrap().native::<i64, i64>().unwrap();
    // get_second_of_tuple.call(tuple_ptr).unwrap()

    let wasm_module = Into::<Pointer<WasmTimeModule>>::into(wasm_module_ptr).borrow();
    let instance = wasm_module.wasm_instances.get(0).unwrap();
    // let get_first_of_tuple = instance.exports.get_function("get_first_of_tuple").unwrap().native::<i64, i64>().unwrap();
    let get_second_of_tuple = instance.get_func(&mut wasm_module.store, "get_second_of_tuple")
    .expect("`second` was not an exported function").typed::<i64, i64, _>(&wasm_module.store).unwrap();
    // get_first_of_tuple.call(tuple_ptr).unwrap()
    get_second_of_tuple.call(&mut wasm_module.store, tuple_ptr).unwrap()
}

#[no_mangle]
pub extern "system" fn Java_org_m4d_adp_transform_TransformInterface_DropTuple(_env: JNIEnv, _class: JClass, wasm_module_ptr: jptr, tuple_ptr: jptr) {
    // Get the wasm module instance and the functions we want to use
    let wasm_module = Into::<Pointer<WasmTimeModule>>::into(wasm_module_ptr).borrow();
    let instance = wasm_module.wasm_instances.get(0).unwrap();
    // let drop_tuple_wasm = instance.exports.get_function("drop_tuple").unwrap().native::<i64, ()>().unwrap();
    // drop_tuple_wasm.call(tuple_ptr).unwrap();
    let drop_tuple = instance.get_func(&mut wasm_module.store, "drop_tuple")
    .expect("`drop` was not an exported function").typed::<i64, (), _>(&wasm_module.store).unwrap();
    // get_first_of_tuple.call(tuple_ptr).unwrap()
    drop_tuple.call(&mut wasm_module.store, tuple_ptr).unwrap();
}


// #[no_mangle]
// pub extern "system" fn Java_org_m4d_adp_transform_TransformInterface_wasmInstancePtr(jre: JNIEnv, _class: JClass, wasm_module_ptr1: jlong, wasm_image: JString) -> jlong {
//     println!("wasm instance for transform interface");
//     // let wasm_module1: Pointer<WasmModule> = wasm_module_ptr1.into();
//     // let instance1 = &wasm_module1.instance;
//     let wasm_module1 = Into::<Pointer<WasmModule>>::into(wasm_module_ptr1).borrow();
//     let instance1 = &wasm_module1.instance;
//     let wasm_image_str: String = jre.get_string(wasm_image).expect("Couldn't get java string!").into();
//     //////////////////////////////////////
//     let memory1 = instance1.exports.get_memory("memory").unwrap();
//     println!("memory1 = {:?}", memory1.data_ptr());
//     // let wasm_bytes_file2 = std::fs::read(wasm_image_str).unwrap();
//     let wasm_bytes_file2 = std::fs::read("transformation.wasm").unwrap();
//     let store2 = Store::new(&Universal::new(Cranelift::default()).engine());
//     // Compiling the Wasm module.
//     let module2 = Module::new(&store2, wasm_bytes_file2).unwrap();
//     // let mut wasi_env2 = WasiState::new("transformer").finalize().unwrap();
//     // let host_mem = Memory::new(&Store::default(),MemoryType::new(18, None, false)).unwrap();
//     let import_object2 = imports! {
//         "env" => {
//             // "sum" => sum_func,
//             "memory" => memory1.clone(),
//             // "memory" => host_mem,
//         }
//     };
//     let instance2 = Instance::new(&module2, &import_object2).unwrap();
//     // let memory2 = instance2.exports.get_memory("memory").unwrap();
//     // println!("memory2 = {:?}", memory2.data_ptr());
//     let alloc = instance2.exports.get_native_function::<i64,i32>("alloc").unwrap();
    
//     // let read_wasm = instance.exports.get_native_function::<i64,i32>("read_from_addr").unwrap();
//     let dealloc = instance2.exports.get_native_function::<(i64,i64),()>("dealloc").unwrap();
//     // let mem_ptr = memory1.data_ptr();
//     // let alloc_ptr = alloc.call(100).unwrap();
//     // let alloc_mem = instance.exports.get_memory("memory").unwrap();
//     // println!("alloc_mem = {:?}", alloc_mem);
//     /////////////////////////////////////////
//     let instance_ptr2 = Pointer::new(instance2).into();
//     // Fill a WasmModule object with module's instance, alloc function, and dealloc function. 
//     let wasm_module2 = WasmModule{instance: instance_ptr2, alloc_func: alloc.clone(), dealloc_func: dealloc.clone()};
//     let to_return = Pointer::new(wasm_module2).into();
//     unsafe { ALLOC_FUNC = Some(alloc.clone()); 
//         let alloc_ptr = ALLOC_FUNC.clone().unwrap().call(100).unwrap();
//         println!("alloc ptr = {:?}", alloc_ptr);
//     }
//     to_return
// }

// #[no_mangle]
// pub extern "system" fn Java_org_m4d_adp_transform_TransformInterface_wasmAlloc(_jre: JNIEnv, _class: *const c_void, wasm_module_ptr: jptr, size: jlong) -> jlong {
//     let wasm_module = Into::<Pointer<WasmTimeModule>>::into(wasm_module_ptr).borrow();
//     // let alloc_func = &wasm_module.alloc_func;
//     unsafe { let alloc_func = ALLOC_FUNC.clone().unwrap();
//     let result = alloc_func.call(size as i64).unwrap();
//     result as i64 }
// }