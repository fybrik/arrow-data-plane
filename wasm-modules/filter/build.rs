fn main() {
    println!("cargo:rustc-cdylib-link-arg=--import-memory");
    println!("cargo:rustc-cdylib-link-arg=-zstack-size=10000");

}