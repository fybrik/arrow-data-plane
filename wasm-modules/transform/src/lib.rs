use std::mem;
use std::sync::Arc;
use std::ops::Deref;
use std::convert::TryFrom;
use arrow::array::Int16Array;
use arrow::record_batch::RecordBatch;
use arrow::datatypes::{Field, DataType};
use arrow::ffi::{ArrowArray, ArrowArrayRef, FFI_ArrowArray, FFI_ArrowSchema};
use arrow::array::{Array, ArrayRef, StructArray, make_array};


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



pub fn transform_record_batch(record: RecordBatch) -> RecordBatch {
    // Create an array with one element of 0
    let zero_array = Int16Array::from(vec![0]);
    let const_array = Arc::new(zero_array);
    // Get the columns except the last column
    let columns: &[ArrayRef] = record.columns();
    let num_cols = record.num_columns();
    let first_columns = columns[0..num_cols-1].to_vec();
    // Create a new array with the same columns expect the last where it will be zero column
    let new_array = [first_columns, vec![const_array]].concat();
    // Create a transformed record batch with the same schema and the new array
    let transformed_record = RecordBatch::try_new(
        record.schema(),
        new_array
    ).unwrap();
    transformed_record
}


#[no_mangle]
pub fn transform(ffi_ptr: i64) -> i64 {
    // Get the pointers of FFI_ArrowArray and FFI_ArrowSchema
    let ffi_tuple = Into::<Pointer<Tuple>>::into(ffi_ptr).borrow();
    let pffiaa = ((*ffi_tuple).0) as *const FFI_ArrowArray;
    let pffias = ((*ffi_tuple).1) as *const FFI_ArrowSchema;
    // Build a record batch from the FFI pointers
    let arrow_array = unsafe{ArrowArray::try_from_raw(pffiaa, pffias).unwrap()};
    let array_data = arrow_array.to_data().unwrap();
    let struct_array : StructArray = array_data.into();
    let record = RecordBatch::from(&struct_array);
    
    // Transformation
    let transformed_record = transform_record_batch(record);
    // Transformation

    // Create a pointer of a tuple of pointers of FFI_ArrowArray and FFI_ArrowSchema for the transformed record batch
    let struct_array: StructArray = transformed_record.into();
    let (ffiaa, ffias) = struct_array.to_raw().unwrap();
    let ret_tuple = Tuple(ffiaa as i64, ffias as i64);
    let ret_tuple_ptr: i64 = Pointer::new(ret_tuple).into();
    mem::forget(arrow_array);
    ret_tuple_ptr
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
    // mem::drop(arrow_array);
}
