use std::sync::Arc;

use crate::common::TransformContext;
use arrow::{array::{Int32Array, PrimitiveArray, StructArray}, compute::unary, datatypes::{DataType, Field, Int32Type, Schema}, record_batch::RecordBatch, util::pretty::print_batches};

#[no_mangle]
pub extern "C" fn transform(ctx: u32) {
    let ctx = ctx as *mut TransformContext;
    let ctx = unsafe{ &*ctx };
    // let schema = ctx.input_schema();
    // println!("{}", schema.unwrap());
    let array = ctx.input().unwrap();
    let as_structarray = array.as_any().downcast_ref::<StructArray>().unwrap();
    let input = RecordBatch::from(as_structarray);
    // let _ = print_batches(&[input]);

    let col_a = input.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
    let col_b = input.column(1).as_any().downcast_ref::<Int32Array>().unwrap();   
    let negated_a = arrow::compute::negate(col_a).unwrap();
    let zero_b: PrimitiveArray<Int32Type> = unary(col_b, |x| 0);

    let projected_schema = Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Int32, false),
    ]);

    let result = RecordBatch::try_new(
        Arc::new(projected_schema),
        vec![
            Arc::new(negated_a),
            Arc::new(zero_b),
        ],
    );

    // ctx.in_array   

}
