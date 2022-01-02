
use std::mem;
use std::sync::Arc;
use std::ops::Deref;
use std::io::Cursor;
use serde_json::Value;
use std::os::raw::c_void;
use arrow::array::ArrayRef;
use arrow::record_batch::RecordBatch;
use arrow::{array::{Int64Array, BooleanArray}, ipc::{self, reader::StreamReader}};
use arrow::datatypes::Int64Type;
use arrow::compute::kernels::filter::filter_record_batch;
use arrow::compute::kernels::comparison::{eq_scalar, neq_scalar, gt_scalar, gt_eq_scalar, lt_scalar, lt_eq_scalar};

#[derive(Debug)]
pub struct Pointer<Kind> {
    value: Box<Kind>,
}

impl<Kind> Pointer<Kind> {
    pub fn new(value: Kind) -> Self {
        Pointer {
            value: Box::new(value),
        }
    }

    pub fn borrow<'a>(self) -> &'a mut Kind {
        Box::leak(self.value)
    }
}

impl<Kind> From<Pointer<Kind>> for i64 {
    fn from(pointer: Pointer<Kind>) -> Self {
        Box::into_raw(pointer.value) as _
    }
}

impl<Kind> From<i64> for Pointer<Kind> {
    fn from(pointer: i64) -> Self {
        Self {
            value: unsafe { Box::from_raw(pointer as *mut Kind) },
        }
    }
}

impl<Kind> Deref for Pointer<Kind> {
    type Target = Kind;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct Tuple (pub i64, pub i64 );


#[no_mangle]
pub fn alloc(len: i64) -> *mut c_void {
    // create a new mutable buffer with capacity `len`
    let mut buf = Vec::with_capacity(len as usize);
    // take a mutable pointer to the buffer
    let ptr = buf.as_mut_ptr();
    // take ownership of the memory block and
    // ensure that its destructor is not
    // called when the object goes out of scope
    // at the end of the function
    std::mem::forget(buf);
    // return the pointer so the runtime
    // can write data at this offset
    ptr as *mut c_void
}

#[no_mangle]
pub unsafe fn dealloc(ptr: i64, size: i64) {
    let data = Vec::from_raw_parts(ptr as *mut u8, size as usize, size as usize);
    std::mem::drop(data);
}    

#[no_mangle]
pub fn create_tuple_ptr(elem1: i64, elem2: i64) -> i64 {
    let ret_tuple = Tuple(elem1, elem2);
    let ret_tuple_ptr = Pointer::new(ret_tuple).into();
    ret_tuple_ptr
}

#[no_mangle]
pub fn get_first_of_tuple(tuple_ptr: i64) -> i64 {
    let tuple = Into::<Pointer<Tuple>>::into(tuple_ptr).borrow();
    (*tuple).0
}

#[no_mangle]
pub fn get_second_of_tuple(tuple_ptr: i64) -> i64 {
    let tuple = Into::<Pointer<Tuple>>::into(tuple_ptr).borrow();
    (*tuple).1
}

#[no_mangle]
pub fn drop_tuple(tuple_ptr: i64) {
    let tuple = Into::<Pointer<Tuple>>::into(tuple_ptr);
    let tuple = tuple.deref();
    unsafe {
        drop(Vec::from_raw_parts(tuple.0 as *mut u8, tuple.1 as usize, tuple.1 as usize));
    };
}

