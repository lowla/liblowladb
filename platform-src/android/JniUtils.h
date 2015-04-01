
#ifndef _JNIUTILS_H
#define _JNIUTILS_H

#include "TeamstudioException.h"
#include <jni.h>
#include <stddef.h>
#include <utf16string.h>
#include <bytevector.h>

extern JavaVM* g_vm;

class JNIEnvWrapper
{
public:
	JNIEnvWrapper();
	JNIEnvWrapper(JNIEnv *env);
	~JNIEnvWrapper();

	JNIEnv* get() { return m_env; }

    jint ThrowNew(jclass clazz, const char *message);
    
	jclass FindClass(const char* name);
	jmethodID GetMethodID(jclass clazz, const char* name, const char* sig);
    jboolean CallBooleanMethod(jobject obj, jmethodID methodID, ...);
    jint CallIntMethod(jobject obj, jmethodID methodID, ...);
    jobject CallObjectMethod(jobject obj, jmethodID methodID, ...);
    void CallVoidMethod(jobject obj, jmethodID methodID, ...);
    jchar CallCharMethod(jobject obj, jmethodID methodID, ...);
    jdouble CallDoubleMethod(jobject obj, jmethodID methodID, ...);

    jmethodID GetStaticMethodID(jclass clazz, const char* name, const char* sig);
    void CallStaticVoidMethod(jclass clazz, jmethodID methodID, ...);
    jboolean CallStaticBooleanMethod(jclass clazz, jmethodID methodID, ...);
    jfieldID GetStaticFieldID(jclass clazz, const char* name, const char* sig);
    jobject GetStaticObjectField(jclass clazz, jfieldID fieldID);

    jfieldID GetFieldID(jclass clazz, const char* name, const char* sig);
	jlong GetLongField(jobject obj, jfieldID fieldID);
    void SetLongField(jobject obj, jfieldID fieldID, jlong value);

    jclass GetObjectClass(jobject obj);

    jobject NewGlobalRef(jobject obj);
    void DeleteGlobalRef(jobject obj);
    void DeleteLocalRef(jobject obj);

    jstring NewString(const jchar* unicodeChars, jsize len);
    jstring NewStringUTF(const char* bytes);
    jobject NewObject(jclass clazz, jmethodID methodID, ...);
    const char* GetStringUTFChars(jstring string, jboolean* isCopy);
    void ReleaseStringUTFChars(jstring string, const char* utf);
    jsize GetArrayLength(jarray array);
    jobject GetObjectArrayElement(jobjectArray array, jsize index);
    jcharArray NewCharArray(jsize length);
    void GetCharArrayRegion(jcharArray array, jsize start, jsize len, jchar* buf);
    jint* GetIntArrayElements(jintArray array, jboolean* isCopy);
    void ReleaseIntArrayElements(jintArray array, jint* elems, jint mode);
    jlong* GetLongArrayElements(jlongArray array, jboolean* isCopy);
    void ReleaseLongArrayElements(jlongArray array, jlong* elems, jint mode);
    jbyteArray NewByteArray(jsize length);
    void SetByteArrayRegion(jbyteArray array, jsize start, jsize len, const jbyte* buf);
	jlongArray NewLongArray(jsize length);
	void SetLongArrayRegion(jlongArray array, jsize start, jsize len, const jlong* buf);

    operator bool() const;

private:
	void handleJavaException();

	JNIEnv *m_env;
	bool m_detach;
};

utf16string convertJString(JNIEnv *env, jstring str);
jbyteArray toByteArray(JNIEnv* env, std::vector<unsigned char> *pVec);
ByteVectorPtr fromByteArray(JNIEnv* env, jbyteArray bytes);
jstring returnUtf16String(JNIEnv* env, utf16string const &str);

extern "C" JNIEXPORT jint Liblowladb_JNI_OnLoad(JavaVM* vm, void* reserved);

#endif
