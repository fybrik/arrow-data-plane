use std::convert::TryInto;
use arrow::ffi::FFI_ArrowSchema;

use std::{
    os::raw::{c_char, c_void},
    sync::Arc,
};

#[repr(C)]
#[derive(Debug)]
struct FFI32_ArrowSchema {
    format: u32,
    name: u32,
    metadata: u32,
    flags: i64,
    n_children: i64,
    children: u32,
    dictionary: u32,
    release: Option<u32>,
    private_data: u32,
}

#[repr(C)]
#[derive(Debug)]
struct FFI64_ArrowSchema {
    format: u64,
    name: u64,
    metadata: u64,
    flags: i64,
    n_children: i64,
    children: u64,
    dictionary: u64,
    release: Option<u64>,
    private_data: u64,
}

#[repr(C)]
struct TransformContext {
    in_schema: *const FFI64_ArrowSchema, 
    out_schema: *const FFI64_ArrowSchema, 
}

#[no_mangle]
pub extern "C" fn prepare_transform() -> u32 {
    let ctx = TransformContext{

    };
    Box::into_raw(Box::new(ctx)) as u32
}

#[no_mangle]
pub unsafe extern "C" fn finalize_tansform(ctx: u32) {
    let ctx = ctx as *mut TransformContext;
    let ctx =  Box::from_raw(ctx);
    Arc::from_raw(ctx.in_schema);
}

#[no_mangle]
pub extern "C" fn transform(ctx: u32) {
    let ctx = ctx as *mut TransformContext;
    let ctx = unsafe{ &*ctx };
    // TODO
}
