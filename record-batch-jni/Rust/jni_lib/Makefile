
.PHONY: build
build:
	cargo build --lib --release --features arrow/prettyprint
	cp target/release/libmylib.so ../../libmylib.so

.PHONY: clean
clean:
	cargo clean
