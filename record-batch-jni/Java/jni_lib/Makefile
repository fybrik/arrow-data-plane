.PHONY: build
build:
	mvn clean compile assembly:single

.PHONY: run
run:
	cd target && java -Djava.library.path=../../..  \
		 -cp "java-project-1.0-SNAPSHOT-jar-with-dependencies.jar" \
		  com.ibm.arrowconverter.ArrowJNIAdapter

.PHONY: javah
javah:
	javah src/main/java/com/ibm/arrowconverter/App.java

.PHONY: clean
clean:
	mvn clean
