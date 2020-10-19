all:
	g++ -c -fPIC -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux org_apache_spark_storage_memory_MemoryStore.cc -o native.o
	g++ -shared -fPIC -o libnative.so native.o -lc
simple-2:
	javah -cp /home/zhao/workspace/scala-2.11.8/lib/scala-library.jar:. Simple
	g++ -c -fPIC -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux Simple.cc -o Simple.o
	g++ -shared -fPIC -o libnative.so Simple.o -lc
	scala -cp . -Djava.library.path=. Simple
simple:
	#/home/zhao/workspace/scala-2.11.8/lib
	#this is not correct :SCALA_LIB_HOME=/home/zhao/workspace/scala-2.11.8/lib
	#javah -cp ${SCALA_LIB_HOME}/scala-library.jar:. Simple
	javah -cp /home/zhao/workspace/scala-2.11.8/lib/scala-library.jar:. Simple
	g++ -c -fPIC -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux Simple.cc -o Simple.o
	g++ -shared -fPIC -o libnative.so Simple.o -lc
	#java -Djava.library.path=. Simple
	#scala -cp . -Djava.library.path=$(pwd) Simple
	scala -cp . -Djava.library.path=. Simple
