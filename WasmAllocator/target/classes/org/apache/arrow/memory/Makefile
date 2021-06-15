.PHONY: wasmer-pointers
wasmer-pointers:
	$(eval JAR := $(shell find ./lib/ -name "wasmer-jni-*.jar"))
	
		javac -classpath "../${JAR}" App.java; \
		java -Djava.library.path=$(CURDIR)/lib/jni -classpath ".:../${JAR}" -enableassertions MemoryExample

.PHONY: clean
clean:
	rm src/MemoryExample.class || true