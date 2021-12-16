use crate::types::{jptr, Pointer, WasmTimeData};
use jni::objects::JClass;
use jni::JNIEnv;
// This is just a pointer. We'll be returning it from our function. We
// can't return one of the objects with lifetime information because the
// lifetime checker won't let us.
use jni::sys::jlong;

#[no_mangle]
pub extern "system" fn Java_org_m4d_adp_transform_TransformInterface_TransformationIPC(
    _env: JNIEnv,
    _class: JClass,
    wasm_module_ptr: jptr,
    address: jlong,
    length: jlong,
    conf_address: jlong,
    conf_size: jlong,
    i: jlong,
) -> jptr {
    // Get the wasm module instance and the functions we want to use
    let wasm_data = Into::<Pointer<WasmTimeData>>::into(wasm_module_ptr).borrow();
    let instance = wasm_data.wasm_transform_instances.get(i as usize).unwrap();

    let transform_func = instance
        .get_func(&mut wasm_data.store, "read_transform_write_from_bytes")
        .expect("`transform` was not an exported function")
        .typed::<(i64, i64, i64, i64), i64, _>(&wasm_data.store)
        .unwrap();

    // Call the function that read the bytes in the `address` parameter and getting the appropriate record batch
    // Then, it makes a transformation, writes back the transformed record batch, and returns a tuple of `(address, len)` of the transformed batch
    let transformed_tuple = transform_func
        .call(
            &mut wasm_data.store,
            (
                address as i64,
                length as i64,
                conf_address as i64,
                conf_size as i64,
            ),
        )
        .unwrap();
    transformed_tuple
}

#[no_mangle]
pub extern "system" fn Java_org_m4d_adp_transform_TransformInterface_GetFirstElemOfTuple(
    _env: JNIEnv,
    _class: JClass,
    wasm_module_ptr: jptr,
    tuple_ptr: jptr,
) -> jptr {
    // Get the wasm module instance and the functions we want to use
    let wasm_data = Into::<Pointer<WasmTimeData>>::into(wasm_module_ptr).borrow();
    let instance = &wasm_data.wasm_alloc_instance;
    let get_first_of_tuple = instance
        .get_func(&mut wasm_data.store, "get_first_of_tuple")
        .expect("`first` was not an exported function")
        .typed::<i64, i64, _>(&wasm_data.store)
        .unwrap();
    get_first_of_tuple
        .call(&mut wasm_data.store, tuple_ptr)
        .unwrap()
}

#[no_mangle]
pub extern "system" fn Java_org_m4d_adp_transform_TransformInterface_GetSecondElemOfTuple(
    _env: JNIEnv,
    _class: JClass,
    wasm_module_ptr: jptr,
    tuple_ptr: jptr,
) -> jptr {
    // Get the wasm module instance and the functions we want to use
    let wasm_data = Into::<Pointer<WasmTimeData>>::into(wasm_module_ptr).borrow();
    let instance = &wasm_data.wasm_alloc_instance;
    let get_second_of_tuple = instance
        .get_func(&mut wasm_data.store, "get_second_of_tuple")
        .expect("`second` was not an exported function")
        .typed::<i64, i64, _>(&wasm_data.store)
        .unwrap();
    get_second_of_tuple
        .call(&mut wasm_data.store, tuple_ptr)
        .unwrap()
}

#[no_mangle]
pub extern "system" fn Java_org_m4d_adp_transform_TransformInterface_DropTuple(
    _env: JNIEnv,
    _class: JClass,
    wasm_module_ptr: jptr,
    tuple_ptr: jptr,
) {
    // Get the wasm module instance and the functions we want to use
    let wasm_data = Into::<Pointer<WasmTimeData>>::into(wasm_module_ptr).borrow();
    let instance = wasm_data.wasm_transform_instances.get(0 as usize).unwrap();
    let drop_tuple = instance
        .get_func(&mut wasm_data.store, "drop_tuple")
        .expect("`drop` was not an exported function")
        .typed::<i64, (), _>(&wasm_data.store)
        .unwrap();
    drop_tuple.call(&mut wasm_data.store, tuple_ptr).unwrap();
}

