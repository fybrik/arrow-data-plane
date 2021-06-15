
.PHONY: build
build:
	cargo build --lib --release --features arrow/prettyprint
	cp target/release/libmylib.so ../../lib

.PHONY: clean
clean:
	cargo clean
