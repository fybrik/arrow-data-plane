// use std::convert::TryInto;
// use arrow::ffi::FFI_ArrowSchema;

use std::convert::TryFrom;
use std::sync::Arc;

use arrow::array::{ArrayRef, make_array_from_raw};
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};

use crate::array::{FFI32_ArrowArray, FFI64_ArrowArray};
use crate::schema::{FFI32_ArrowSchema, FFI64_ArrowSchema};

#[repr(C)]
pub(crate) struct TransformContext {
    in_schema: *const FFI64_ArrowSchema,
    in_array: *const FFI64_ArrowArray,
    out_schema: *const FFI64_ArrowSchema,
    out_array: *const FFI64_ArrowArray,
}

impl TransformContext {
    pub fn input(&self) -> ArrayRef {
        let schema = unsafe {&*self.in_schema};
        let schema = FFI32_ArrowSchema::try_from(schema).unwrap();
        let schema = Arc::into_raw(Arc::new(schema)) as *const FFI_ArrowSchema;
        
        let array = unsafe {&*self.in_array};
        let array = FFI32_ArrowArray::try_from(array).unwrap();
        let array = Arc::into_raw(Arc::new(array)) as *const FFI_ArrowArray;
        
        let result = unsafe {make_array_from_raw(array, schema)};
        result.unwrap()
    }
}

#[no_mangle]
pub extern "C" fn prepare_transform() -> u32 {
    let ctx = TransformContext {
        in_schema: Arc::into_raw(Arc::new(FFI64_ArrowSchema::empty())),
        in_array: Arc::into_raw(Arc::new(FFI64_ArrowArray::empty())),
        out_schema: std::ptr::null_mut(),
        out_array: std::ptr::null_mut(),
    };
    Box::into_raw(Box::new(ctx)) as u32
}

#[no_mangle]
pub unsafe extern "C" fn finalize_tansform(ctx: u32) {
    let ctx = ctx as *mut TransformContext;
    let ctx = Box::from_raw(ctx);
    Arc::from_raw(ctx.in_schema);
    Arc::from_raw(ctx.in_array);
}
