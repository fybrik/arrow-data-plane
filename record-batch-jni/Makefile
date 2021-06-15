.PHONY: build_jar
build_jar:
	$(MAKE) -C Java build

.PHONY: build_lib
build_lib:
	$(MAKE) -C Rust build

.PHONY: build
build: build_jar build_lib

.PHONY: run
run:
	$(MAKE) -C Java run

.PHONY: clean
clean:
	$(MAKE) -C Java clean
	$(MAKE) -C Rust clean
	rm lib/libmylib.so || true
