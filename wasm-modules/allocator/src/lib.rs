
use std::mem;
use std::sync::Arc;
use std::ops::Deref;
use std::io::Cursor;
use std::os::raw::c_void;
use arrow::array::ArrayRef;
use arrow::record_batch::RecordBatch;
use arrow::{array::{Int64Array}, ipc::{self, reader::StreamReader}};
use arrow::compute::add;

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

    // Get the columns except the last column
    let columns: &[ArrayRef] = record_in.columns();
    let first_columns = columns[0..num_cols-1].to_vec();

    // Create a transformed record batch with the same schema and the new array
    let col0 = columns[0].data();
    let pa0 = Int64Array::from(col0.clone());

    let col1 = columns[1].data();
    let pa1 = Int64Array::from(col1.clone());

    let new_column = Arc::new(add(&pa0, &pa1).unwrap());

    let new_array = [first_columns, vec![new_column]].concat();
    // Create a transformed record batch with the same schema and the new array
    let transformed_record = RecordBatch::try_new(
        record_in.schema(),
        new_array
    ).unwrap();
    transformed_record
}

#[no_mangle]
pub fn create_tuple_ptr(elem1: i64, elem2: i64) -> i64 {
    let ret_tuple = Tuple(elem1, elem2);
    let ret_tuple_ptr = Pointer::new(ret_tuple).into();
    ret_tuple_ptr
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
        let mut bytes_array = writer.into_inner().unwrap();
        bytes_array.shrink_to_fit();
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

#[no_mangle]
pub fn drop_tuple(tuple_ptr: i64) {
    let tuple = Into::<Pointer<Tuple>>::into(tuple_ptr);
    let tuple = tuple.deref();
    unsafe {
        drop(Vec::from_raw_parts(tuple.0 as *mut u8, tuple.1 as usize, tuple.1 as usize));
    };
}

