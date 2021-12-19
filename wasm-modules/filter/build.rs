//extern crate bindgen;

//use std::env;
//use std::path::PathBuf;

fn main() {
    // Tell cargo to tell rustc to link the system bzip2
    // shared library.
    // println!("cargo:rustc-cdylib-link-arg=--import-memory");
    // //println!("cargo:rustc-cdylib-link-arg=-zstack-size=100000");
    println!("cargo:rustc-cdylib-link-arg=--import-memory");
    println!("cargo:rustc-cdylib-link-arg=-zstack-size=10000");

    // Tell cargo to invalidate the built crate whenever the wrapper changes
    //println!("cargo:rerun-if-changed=wrapper.h");

    // The bindgen::Builder is the main entry point
    // to bindgen, and lets you build up options for
    // the resulting bindings.


    // Write the bindings to the $OUT_DIR/bindings.rs file.

}