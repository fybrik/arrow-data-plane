ifeq ($(OS),Windows_NT)
	build_os := windows
	build_arch := amd64
else
	UNAME_S := $(shell uname -s)

	ifeq ($(UNAME_S),Darwin)
		build_os := darwin
	endif

	ifeq ($(UNAME_S),Linux)
		build_os := linux
	endif

	ARCH := $(shell uname -m)

	ifeq ($(ARCH),x86_64)
		build_arch = amd64
	else
		$(error Architecture not supported yet)
	endif
endif

# Compile everything!
build: build-headers build-rust build-java

# Compile the Rust part (only for one target).
# We relay this command to the others, to make sure that
# artifacts are set properly.
build-rust: build-rust-$(build_arch)-$(build_os)

# Compile the Rust part.
build-rust-all-targets: build-rust-amd64-darwin build-rust-amd64-linux build-rust-amd64-windows

build-rust-amd64-darwin:
	rustup target add x86_64-apple-darwin
	cargo build --release --target=x86_64-apple-darwin
	mkdir -p artifacts/darwin-amd64
	cp target/x86_64-apple-darwin/release/libwasm_interface.dylib artifacts/darwin-amd64
	install_name_tool -id "@rpath/libwasm_interface.dylib" ./artifacts/darwin-amd64/libwasm_interface.dylib
	test -h target/current || ln -s x86_64-apple-darwin/release target/current

build-rust-amd64-linux:
	rustup target add x86_64-unknown-linux-gnu
	cargo build --release --target=x86_64-unknown-linux-gnu
	mkdir -p artifacts/linux_64
	cp target/x86_64-unknown-linux-gnu/release/libwasm_interface.so artifacts/linux_64/
	test -h target/current || ln -s x86_64-unknown-linux-gnu/release target/current

build-rust-amd64-windows:
	rustup target add x86_64-pc-windows-msvc
	cargo build --release --target=x86_64-pc-windows-msvc
	mkdir -p artifacts/windows-amd64
	cp target/x86_64-pc-windows-msvc/release/libwasm_interface.dll artifacts/windows-amd64/
	mkdir -p target/current
	cp target/x86_64-pc-windows-msvc/release/libwasm_interface.dll target/current/

# Compile the Java part (incl. `build-test`, see `gradlew`).
build-java:
	"./gradlew" --info build

# Generate the Java C headers.
build-headers:
	"./gradlew" --info generateJniHeaders

# Run the tests.
test: build-headers build-rust test-rust build-java

# Run the Rust tests.
test-rust: test-rust-$(build_arch)-$(build_os)

test-rust-amd64-darwin:
	cargo test --lib --release --target=x86_64-apple-darwin

test-rust-amd64-linux:
	cargo test --lib --release --target=x86_64-unknown-linux-gnu

test-rust-amd64-windows:
	cargo test --lib --release --target=x86_64-pc-windows-msvc

# Make a JAR-file.
package:
	"./gradlew" --info jar

# Clean
clean:
	cargo clean
	rm -rf build
	rm -rf artifacts
