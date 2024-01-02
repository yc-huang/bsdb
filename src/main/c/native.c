#include <stdlib.h>
#include <errno.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include "tech_bsdb_io_Native.h"

#include "mph.h"

#define SUX4J_MAP mph
#define SUX4J_LOAD_MAP load_mph
#define SUX4J_GET_BYTE_ARRAY mph_get_byte_array

JNIEXPORT jint JNICALL Java_tech_bsdb_io_Native_open(JNIEnv * env, jclass self, jstring fileName, jint flags)
{
    const char *nativeStringFileName = (*env)->GetStringUTFChars(env, fileName, 0);
    int fd;
    {
       fd = open(nativeStringFileName, flags);
       (*env)->ReleaseStringUTFChars(env, fileName, nativeStringFileName);
    }
    return fd;
}


JNIEXPORT jlong JNICALL Java_tech_bsdb_io_Native_allocateAligned(JNIEnv * env, jclass self, jint size, jint align){
	void *buf;
	int ret = posix_memalign(&buf, align, size);
	if(ret){
		return -1;
	}else{
		return (jlong)buf;
	}
}


JNIEXPORT void JNICALL Java_tech_bsdb_io_Native_free(JNIEnv * env, jclass self, jlong address){
	free((void *)address);
}

JNIEXPORT jlong JNICALL Java_tech_bsdb_io_Native_pread(JNIEnv * env, jclass self, jint fd, jlong position, jlong bufPtr, jint size){
	return pread((int)fd, (void*) bufPtr, (unsigned) size, (off_t)position);
}

JNIEXPORT jint JNICALL Java_tech_bsdb_io_Native_close(JNIEnv * env, jclass cls, jint fd){
	return close((int)fd);
}

JNIEXPORT jlong JNICALL Java_tech_bsdb_io_Native_loadHash
  (JNIEnv * env, jclass cls, jstring path){
    const char* filePath = (*env)->GetStringUTFChars(env, path, NULL);
    int h = open(filePath, O_RDONLY);
	if(h < 0) return -1;
	SUX4J_MAP *SUX4J_MAP = SUX4J_LOAD_MAP(h);
	close(h);
	(*env)->ReleaseStringUTFChars(env, path, filePath);
	return (jlong)SUX4J_MAP;
  }

JNIEXPORT jlong JNICALL Java_tech_bsdb_io_Native_getHash
  (JNIEnv * env, jclass cls, jlong hashFunc, jbyteArray key){
    jsize length = (*env)->GetArrayLength(env, key);
    jbyte* bytes = (*env)->GetByteArrayElements(env, key, NULL);
    uint64_t hash = SUX4J_GET_BYTE_ARRAY(hashFunc, bytes, length);
    (*env)->ReleaseByteArrayElements(env, key, bytes, JNI_ABORT);
    return (jlong)hash;
  }