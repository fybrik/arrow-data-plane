// use std::convert::TryInto;
// use arrow::ffi::FFI_ArrowSchema;

use std::convert::TryFrom;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int32Array, StructArray, make_array_from_raw};
use arrow::datatypes::Schema;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow::record_batch::RecordBatch;

use crate::array::{FFI32_ArrowArray, FFI64_ArrowArray};
use crate::schema::{FFI32_ArrowSchema, FFI64_ArrowSchema};

#[repr(C)]
pub(crate) struct TransformContext {
    base: u64,
    in_schema: *mut FFI64_ArrowSchema,
    in_array: *mut FFI64_ArrowArray,
    out_schema: *const FFI64_ArrowSchema,
    out_array: *const FFI64_ArrowArray,
}

impl TransformContext {
    pub fn input_schema(&self) -> Option<Schema> {
        let schema = unsafe {&mut*self.in_schema};
        let schema = FFI32_ArrowSchema::from(self.base, schema);
        let schema = Arc::into_raw(Arc::new(schema)) as *const FFI_ArrowSchema;
        let schema = unsafe{&*schema};
        println!("format {}", schema.format());
        Schema::try_from(schema).ok()
        // None
    }

    pub fn input(&self) -> Option<ArrayRef> {
        let schema = unsafe {&mut*self.in_schema};
        let schema = FFI32_ArrowSchema::from(self.base, schema);
        let schema = Arc::into_raw(Arc::new(schema)) as *const FFI_ArrowSchema;
        
        let array = unsafe {&mut*self.in_array};
        let array = FFI32_ArrowArray::from(self.base, array);
        let array = Arc::into_raw(Arc::new(array)) as *const FFI_ArrowArray;
        
        let result = unsafe {make_array_from_raw(array, schema)};
        result.ok()
    }

    pub fn output(&self, rb: RecordBatch) {
        // let array = Int32Array::from(vec![Some(1), None, Some(3)]);
        // array.to_raw();
        let as_struct = Arc::new(StructArray::from(rb));
        let (array,  schema)= as_struct.to_raw().unwrap();
        self.out_schema = FFI64_ArrowSchema::from(self.base, schema) ;
        self.out_array = array;
    }
}

#[no_mangle]
pub extern "C" fn prepare_transform(base: u64) -> u32 {
    let ctx = TransformContext {
        base,
        in_schema: Arc::into_raw(Arc::new(FFI64_ArrowSchema::empty())) as *mut FFI64_ArrowSchema,
        in_array: Arc::into_raw(Arc::new(FFI64_ArrowArray::empty())) as *mut FFI64_ArrowArray,
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
