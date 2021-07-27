use crate::types::{WasmModule, Pointer, jptr};

use libc::c_void;
use jni::JNIEnv;
use jni::objects::{JString, JClass, JObject, JValue, JList};

// This is just a pointer. We'll be returning it from our function. We
// can't return one of the objects with lifetime information because the
// lifetime checker won't let us.
use jni::sys::{jlong, jarray, jobjectArray, jint, jobject};

use arrow::{
    buffer::Buffer,
    datatypes::{Schema, Field, DataType},
    array::{ArrayData, ArrayDataBuilder, StructArray, ArrayRef},
    util::pretty::print_batches,
    record_batch::RecordBatch
};

use arrow::ffi::FFI_ArrowArray;
use arrow::ffi::FFI_ArrowSchema;
use arrow::ffi::ArrowArray;
use arrow::ffi::ArrowArrayRef;

use std::ptr::NonNull;
use std::sync::Arc;
use serde_json::Value;
use std::mem;
use arrow::array::{Array, make_array_from_raw};

use crate::transformer;

#[no_mangle]
pub extern "system" fn Java_org_m4d_adp_transform_TransformInterface_GetFFIArrowArray(_env: JNIEnv,_class: JClass, ffi_ptr: jptr) -> jlong {
    let ffi_tuple = Into::<Pointer<Tuple>>::into(ffi_ptr).borrow();
    let PFFIAA = ((*ffi_tuple).0) as *const FFI_ArrowArray;
    unsafe{println!("****FFIAA = {:?}", (PFFIAA))};
    let ret_ffiaa_ptr: i64 = Pointer::new(PFFIAA).into();
    ret_ffiaa_ptr
}
#[no_mangle]
pub extern "system" fn Java_org_m4d_adp_transform_TransformInterface_GetFFIArrowSchema(_env: JNIEnv,_class: JClass, ffi_ptr: jptr) -> jlong{
    let ffi_tuple = Into::<Pointer<Tuple>>::into(ffi_ptr).borrow();
    let PFFIAS = ((*ffi_tuple).1) as *const FFI_ArrowSchema;
    unsafe{println!("****FFIAS = {:?}", (PFFIAS))};
    let ret_ffias_ptr: i64 = Pointer::new(PFFIAS).into();
    ret_ffias_ptr
}


#[no_mangle]
pub extern "system" fn Java_org_m4d_adp_transform_TransformInterface_convertVSR2FFI(env: JNIEnv,
                                                                                    _class: JClass,
                                                                                    vsr_org: JObject)
                                                                                    -> jlong {
    let jo_schema = env.call_method(vsr_org, "getSchema","()Lorg/apache/arrow/vector/types/pojo/Schema;",&[])
        .unwrap()
        .l()
        .unwrap();
    let jo_json = env.call_method(jo_schema, "toJson","()Ljava/lang/String;",&[])
        .unwrap()
        .l()
        .unwrap();
    let js_json = JString::from(jo_json);
    let rs_json: String = env.get_string(js_json)
        .unwrap()
        .into();
    let rv_json : Value = serde_json::from_str(&rs_json)
        .unwrap();
    let r_schema = Schema::from(&rv_json)
        .unwrap();
    let fields = r_schema
        .fields()
        .clone();

    // Get vectors
    let jo_vectors_list = env.call_method(vsr_org, "getFieldVectors","()Ljava/util/List;",&[])
        .unwrap()
        .l()
        .unwrap();

    let vectors = transformer::get_vectors(env, jo_vectors_list, r_schema.fields().clone());
    let arrays: Vec<_> = vectors.iter().map(|x| StructArray::from(x.clone())).collect();

    let mut pairs: Vec<(Field, ArrayRef)> = Vec::new();
    for i in 0..arrays.len() {
        pairs.push((fields[i].clone(), arrow::array::make_array(vectors[i].clone())));
    }
    // create struct array
    let struct_array_org = StructArray::from(pairs);
    let tuple =  struct_array_org.to_raw().unwrap() ;

    Pointer::new(PFFIAS).into(tuple);
}

#[no_mangle]
pub extern "system" fn Java_org_m4d_adp_transform_TransformInterface_createOrUpdateVSR(env: JNIEnv,
                                                                                    _class: JClass,
                                                                                    c_data_interface: jptr,
                                                                                    vsr_target: JObject)
                                                                                    -> jptr {
    let record = transformer::c_data_interface_to_record_batch(c_data_interface);
    let vsr : JObject;
    if vsr_target.is_null() {
        let schema = JObject::from(transformer::from_rust_schema_to_java_schema(env, record.schema()));
        // For now use the null allocator
        let null_allocator = JObject::null();
        vsr = env.call_static_method(
            "org/apache/arrow/vector/VectorSchemaRoot",
            "create",
             "(org/apache/arrow/vector/types/pojo/Schema;Ljava/lang/Object;)org/apache/arrow/vector/VectorSchemaRoot",
                 &[JValue::Object(schema), JValue::Object(null_allocator)]
        )
            .unwrap()
            .l()
            .unwrap();
    } else {
        vsr = vsr_target;
    }
    transformer::load_record_batch_to_vsr(env, vsr, &record);
    vsr.into_inner() as jptr
}