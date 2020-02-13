#include <iostream>
#include <vector>
#include <jni.h>
#include <unordered_map>
#include <inttypes.h>

#define JEXPORT(method, ret, ...) extern "C" JNIEXPORT ret JNICALL \
	Java_org_apache_spark_storage_memory_MemoryStore_ ## method (JNIEnv* env, jobject obj, ##__VA_ARGS__)

static jlong global_index = 0;
std::unordered_map<jlong, jobjectArray> native_cache;

JEXPORT(putIntoNative, jlong, jobjectArray block){
    native_cache.insert({++global_index, block});
	return global_index;
}

JEXPORT(getFromNative, jobjectArray, jlong index){
    jobjectArray block = native_cache.at(index);
	return block;
}

JEXPORT(getSizeNatve, jlong, jlong index){
    jobjectArray block = native_cache.at(index);
    jlong blocksize = (jlong)sizeof(block);
	return blocksize;
}

JNIEXPORT void JNICALL Java_org_apache_spark_storage_memory_MemoryStore_removeNative
  (JNIEnv *, jobject, jlong index){
    native_cache.erase(index);
}

extern "C" int main(){
	return 0;
}

