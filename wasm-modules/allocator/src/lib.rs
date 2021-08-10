
use std::os::raw::c_void;
extern crate wee_alloc;
use std::{io::Cursor};

// Use `wee_alloc` as the global allocator.
#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

use std::mem;
use std::sync::Arc;
use std::ops::Deref;
use std::convert::TryFrom;
use arrow::array::Int16Array;
use arrow::record_batch::RecordBatch;
use arrow::datatypes::{Field, DataType};
use arrow::ffi::{ArrowArray, ArrowArrayRef, FFI_ArrowArray, FFI_ArrowSchema};
use arrow::array::{Array, ArrayRef, StructArray, make_array};

use arrow::{array::{Int64Array}, ipc::{self, reader::StreamReader}};

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


pub fn transform_record_batch(record_in: RecordBatch) -> RecordBatch {
    let num_cols = record_in.num_columns();
    let num_rows = record_in.num_rows();
    // Build a zero array
    let struct_array = Int64Array::from(vec![0; num_rows]);
    let new_column = Arc::new(struct_array);
    // Get the columns except the last column
    let columns: &[ArrayRef] = record_in.columns();
    let first_columns = columns[0..num_cols-1].to_vec();
    // Create a new array with the same columns expect the last where it will be zero column
    let new_array = [first_columns, vec![new_column]].concat();
    // Create a transformed record batch with the same schema and the new array
    let transformed_record = RecordBatch::try_new(
        record_in.schema(),
        new_array
    ).unwrap();
    transformed_record
}

// Creates an example FFI_ArrowArray and FFI_ArrowSchema and returns a pointer to them.
#[no_mangle]
pub fn create_empty_ffi_arrow() -> i64 {
    let arrow_array= unsafe{ArrowArray::empty()};
    let (ffiaa, ffias) = ArrowArray::into_raw(arrow_array);
    let ffi_tuple = Tuple(ffiaa as i64, ffias as i64);
    let ffi_tuple_ptr: i64 = Pointer::new(ffi_tuple).into();
    ffi_tuple_ptr
}

// Creates an example FFI_ArrowArray and FFI_ArrowSchema and returns a pointer to them.
#[no_mangle]
pub fn create_ffi_arrow() -> i64 {
    let field1 = Field::new("a", DataType::Int16, true);
    let field2 = Field::new("b", DataType::Int16, true);
    let int_array1 = Int16Array::from(vec![1]);
    let int_array_data1 = make_array(int_array1.data().clone());
    let int_array2 = Int16Array::from(vec![2]);
    let int_array_data2 = make_array(int_array2.data().clone());
    // The array has two columns, each column has one int16 value
    let array = StructArray::try_from(vec![(field1, int_array_data1), (field2, int_array_data2)]).unwrap();

    let (ffiaa, ffias) = array.to_raw().unwrap();
    let ffi_tuple = Tuple(ffiaa as i64, ffias as i64);
    let ffi_tuple_ptr: i64 = Pointer::new(ffi_tuple).into();
    ffi_tuple_ptr
}

#[no_mangle]
pub fn check_transform(ffi_ptr: i64, after_transform: i32) {
    // Get the pointers of FFI_ArrowArray and FFI_ArrowSchema and create a record batch
    let ffi_tuple = Into::<Pointer<Tuple>>::into(ffi_ptr).borrow();
    let pffiaa = ((*ffi_tuple).0) as *const FFI_ArrowArray;
    let pffias = ((*ffi_tuple).1) as *const FFI_ArrowSchema;
    let arrow_array = unsafe{ArrowArray::try_from_raw(pffiaa, pffias).unwrap()};
    let array_data = arrow_array.to_data().unwrap();
    let struct_array: StructArray = StructArray::from(array_data.clone());
    let record = RecordBatch::from(&struct_array);

    // Check that the data type is correct
    assert_eq!(&DataType::Int16, record.schema().field(0).data_type());
    if after_transform == 1 {
        // Check if the first element of the second column is 0 after the transformation
        assert_eq!(&0, record.column(1).data().buffers().first().unwrap().get(0).unwrap());
    } else {
        // Check if the first element of the second column is 2 before the transformation
        assert_eq!(&2, record.column(1).data().buffers().first().unwrap().get(0).unwrap());
    }

    mem::forget(arrow_array);

}

#[no_mangle]
pub fn drop_record_batch(ffi_ptr: i64) {
    // Get the pointers of FFI_ArrowArray and FFI_ArrowSchema and create a record batch
    let ffi_tuple = Into::<Pointer<Tuple>>::into(ffi_ptr).borrow();
    let pffiaa = ((*ffi_tuple).0) as *const FFI_ArrowArray;
    let pffias = ((*ffi_tuple).1) as *const FFI_ArrowSchema;
    let arrow_array = unsafe{ArrowArray::try_from_raw(pffiaa, pffias).unwrap()};
    let array_data = arrow_array.to_data().unwrap();
    let struct_array: StructArray = StructArray::from(array_data.clone());
    let _record = RecordBatch::from(&struct_array);
}

#[no_mangle]
pub fn create_tuple_ptr(pffiaa: i64, pffias: i64) -> i64 {
    let ret_tuple = Tuple(pffiaa as i64, pffias as i64);
    let ret_tuple_ptr = Pointer::new(ret_tuple).into();
    ret_tuple_ptr
}

#[no_mangle]
pub fn get_ffiaa(ffi_ptr: i64) -> i64 {
    let ffi_tuple = Into::<Pointer<Tuple>>::into(ffi_ptr).borrow();
    (*ffi_tuple).0
}

#[no_mangle]
pub fn get_ffias(ffi_ptr: i64) -> i64 {
    let ffi_tuple = Into::<Pointer<Tuple>>::into(ffi_ptr).borrow();
    (*ffi_tuple).1
}

 //////////IPC related functions//////////

#[no_mangle]
pub fn read_transform_write_from_bytes(bytes_ptr: i64, bytes_len: i64) -> i64 {
    // Read the byte array in the given address and length
    let bytes_array: Vec<u8> = unsafe{ Vec::from_raw_parts(bytes_ptr as *mut _, bytes_len as usize, bytes_len as usize) };
    let cursor = Cursor::new(bytes_array);
    let reader = StreamReader::try_new(cursor).unwrap();
    let mut ret_ptr = 0;
    reader.for_each(|batch| {
        let batch = batch.unwrap();
        // Transform the record batch
        let transformed = transform_record_batch(batch);

        // Write the transformed record batch uing IPC
        let schema = transformed.schema();
        let vec = Vec::new();
        let mut writer = crate::ipc::writer::StreamWriter::try_new(vec, &schema).unwrap();
        writer.write(&transformed).unwrap();
        writer.finish().unwrap();
        mem::forget(transformed);
        let mut bytes_array = writer.into_inner().unwrap();
        let bytes_ptr = bytes_array.as_mut_ptr();
        let bytes_len = bytes_array.len();
        mem::forget(bytes_array);
        ret_ptr =  create_tuple_ptr(bytes_ptr as i64, bytes_len as i64);
    });
    ret_ptr
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
