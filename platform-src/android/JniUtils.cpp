
#include "JniUtils.h"

#include <memory>

#include <android/log.h>

JavaVM *g_vm = NULL;
jobject g_classLoader = NULL;
jmethodID g_loadClass = NULL;

JNIEnvWrapper::JNIEnvWrapper()
{
	m_env = NULL;
	if (g_vm && JNI_EDETACHED == g_vm->GetEnv((void**)&m_env, JNI_VERSION_1_6)) {
		if (JNI_OK == g_vm->AttachCurrentThread(&m_env, NULL)) {
			//__android_log_print(ANDROID_LOG_DEBUG, "UNP-NDK", "Attaching to JNI thread, g_vm=%ld", (long)g_vm);
			m_detach = true;
		}
	}
	else {
		m_detach = false;
	}
}

JNIEnvWrapper::JNIEnvWrapper(JNIEnv *env) : m_env(env), m_detach(false)
{
}

JNIEnvWrapper::~JNIEnvWrapper() {
	if (m_detach && NULL != m_env) {
		g_vm->DetachCurrentThread();
		m_env = NULL;
	}
}

jint JNIEnvWrapper::ThrowNew(jclass clazz, const char *message)
{
    return m_env->ThrowNew(clazz, message);
}

jclass JNIEnvWrapper::FindClass(const char *name)
{
    jstring className = m_env->NewStringUTF(name);
	jclass answer = (jclass)m_env->CallObjectMethod(g_classLoader, g_loadClass, className, false);
    m_env->DeleteLocalRef(className);
	if (0 == answer) {
		__android_log_print(ANDROID_LOG_ERROR, "JniUtils", "Could not locate class %s", name);
	}
	handleJavaException();
	return answer;
}

jmethodID JNIEnvWrapper::GetMethodID(jclass clazz, const char* name, const char* sig)
{
	return m_env->GetMethodID(clazz, name, sig);
}

jboolean JNIEnvWrapper::CallBooleanMethod(jobject obj, jmethodID methodID, ...)
{
    va_list args;
    va_start(args, methodID);
    jboolean result = m_env->CallBooleanMethodV(obj, methodID, args);
    va_end(args);

    handleJavaException();

    return result;
}

jint JNIEnvWrapper::CallIntMethod(jobject obj, jmethodID methodID, ...)
{
    va_list args;
    va_start(args, methodID);
    jint result = m_env->CallIntMethodV(obj, methodID, args);
    va_end(args);

    handleJavaException();

    return result;
}

jobject JNIEnvWrapper::CallObjectMethod(jobject obj, jmethodID methodID, ...)
{
    va_list args;
    va_start(args, methodID);
    jobject result = m_env->CallObjectMethodV(obj, methodID, args);
    va_end(args);

    handleJavaException();

    return result;
}

void JNIEnvWrapper::CallVoidMethod(jobject obj, jmethodID methodID, ...)
{
    va_list args;
    va_start(args, methodID);
    m_env->CallVoidMethodV(obj, methodID, args);
    va_end(args);

    handleJavaException();
}

jchar JNIEnvWrapper::CallCharMethod(jobject obj, jmethodID methodID, ...)
{
    va_list args;
    va_start(args, methodID);
    jchar result = m_env->CallCharMethodV(obj, methodID, args);
    va_end(args);

    handleJavaException();

    return result;
}

jdouble JNIEnvWrapper::CallDoubleMethod(jobject obj, jmethodID methodID, ...)
{
    va_list args;
    va_start(args, methodID);
    jdouble result = m_env->CallDoubleMethodV(obj, methodID, args);
    va_end(args);

    handleJavaException();

    return result;
}

jmethodID JNIEnvWrapper::GetStaticMethodID(jclass clazz, const char* name, const char* sig)
{
	return m_env->GetStaticMethodID(clazz, name, sig);
}

void JNIEnvWrapper::CallStaticVoidMethod(jclass clazz, jmethodID methodID, ...)
{
    va_list args;
    va_start(args, methodID);
    m_env->CallStaticVoidMethodV(clazz, methodID, args);
    va_end(args);
    
    handleJavaException();
}

jboolean JNIEnvWrapper::CallStaticBooleanMethod(jclass clazz, jmethodID methodID, ...)
{
    va_list args;
    va_start(args, methodID);
    jboolean result = m_env->CallStaticBooleanMethodV(clazz, methodID, args);
    va_end(args);

    handleJavaException();

    return result;
}

jfieldID JNIEnvWrapper::GetStaticFieldID(jclass clazz, const char* name, const char* sig)
{
    jfieldID answer = m_env->GetStaticFieldID(clazz, name, sig);
    handleJavaException();
    
    return answer;
}

jobject JNIEnvWrapper::GetStaticObjectField(jclass clazz, jfieldID fieldID)
{
    jobject answer = m_env->GetStaticObjectField(clazz, fieldID);
    handleJavaException();
    
    return answer;
}

jfieldID JNIEnvWrapper::GetFieldID(jclass clazz, const char* name, const char* sig)
{
	return m_env->GetFieldID(clazz, name, sig);
}

jlong JNIEnvWrapper::GetLongField(jobject obj, jfieldID fieldID)
{
	return m_env->GetLongField(obj, fieldID);
}

void JNIEnvWrapper::SetLongField(jobject obj, jfieldID fieldID, jlong value)
{
	m_env->SetLongField(obj, fieldID, value);
}

jclass JNIEnvWrapper::GetObjectClass(jobject obj)
{
	return m_env->GetObjectClass(obj);
}

jobject JNIEnvWrapper::NewGlobalRef(jobject obj)
{
	return m_env->NewGlobalRef(obj);
}

void JNIEnvWrapper::DeleteGlobalRef(jobject obj)
{
	m_env->DeleteGlobalRef(obj);
}

void JNIEnvWrapper::DeleteLocalRef(jobject obj)
{
	m_env->DeleteLocalRef(obj);
}

jstring JNIEnvWrapper::NewString(const jchar* unicodeChars, jsize len)
{
	return m_env->NewString(unicodeChars, len);
}

jstring JNIEnvWrapper::NewStringUTF(const char* bytes)
{
	return m_env->NewStringUTF(bytes);
}

jobject JNIEnvWrapper::NewObject(jclass clazz, jmethodID methodID, ...)
{
    va_list args;
    va_start(args, methodID);
    jobject result = m_env->NewObjectV(clazz, methodID, args);
    va_end(args);

    handleJavaException();

    return result;
}

const char* JNIEnvWrapper::GetStringUTFChars(jstring string, jboolean* isCopy)
{
	return m_env->GetStringUTFChars(string, isCopy);
}

void JNIEnvWrapper::ReleaseStringUTFChars(jstring string, const char* utf)
{
	m_env->ReleaseStringUTFChars(string, utf);
}

jsize JNIEnvWrapper::GetArrayLength(jarray array)
{
	return m_env->GetArrayLength(array);
}

jobject JNIEnvWrapper::GetObjectArrayElement(jobjectArray array, jsize index)
{
	return m_env->GetObjectArrayElement(array ,index);
}

jcharArray JNIEnvWrapper::NewCharArray(jsize length)
{
	return m_env->NewCharArray(length);
}

void JNIEnvWrapper::GetCharArrayRegion(jcharArray array, jsize start, jsize len, jchar* buf)
{
	m_env->GetCharArrayRegion(array, start, len, buf);
}

jint* JNIEnvWrapper::GetIntArrayElements(jintArray array, jboolean* isCopy)
{
	return m_env->GetIntArrayElements(array, isCopy);
}

void JNIEnvWrapper::ReleaseIntArrayElements(jintArray array, jint* elems, jint mode)
{
	m_env->ReleaseIntArrayElements(array, elems, mode);
}

jlong* JNIEnvWrapper::GetLongArrayElements(jlongArray array, jboolean* isCopy)
{
	return m_env->GetLongArrayElements(array, isCopy);
}

void JNIEnvWrapper::ReleaseLongArrayElements(jlongArray array, jlong* elems, jint mode)
{
	m_env->ReleaseLongArrayElements(array, elems, mode);
}

jbyteArray JNIEnvWrapper::NewByteArray(jsize length)
{
	return m_env->NewByteArray(length);
}

void JNIEnvWrapper::SetByteArrayRegion(jbyteArray array, jsize start, jsize len, const jbyte *buf)
{
	m_env->SetByteArrayRegion(array, start, len, buf);
}

jlongArray JNIEnvWrapper::NewLongArray(jsize length)
{
	return m_env->NewLongArray(length);
}

void JNIEnvWrapper::SetLongArrayRegion(jlongArray array, jsize start, jsize len, const jlong *buf)
{
	m_env->SetLongArrayRegion(array, start, len, buf);
}

void JNIEnvWrapper::handleJavaException()
{
	jthrowable throwable = m_env->ExceptionOccurred();
	if (NULL == throwable) {
		return;
	}
	m_env->ExceptionClear();
	jclass clazzThrowable = m_env->FindClass("java/lang/Throwable");
	jclass clazzOOB = m_env->FindClass("java/lang/StringIndexOutOfBoundsException");
	jmethodID getMessage = m_env->GetMethodID(clazzThrowable, "getMessage", "()Ljava/lang/String;");
	jstring message = (jstring)m_env->CallObjectMethod(throwable, getMessage);
	const char *utf = m_env->GetStringUTFChars(message, NULL);
	utf16string strMessage(utf);
	m_env->ReleaseStringUTFChars(message, utf);
	if (m_env->IsInstanceOf(throwable, clazzOOB)) {
		throw StringIndexOutOfBoundsException(strMessage);
	}
	else {
		throw TeamstudioException(strMessage);
	}
}

JNIEnvWrapper::operator bool() const
{
	return NULL != m_env;
}

jbyteArray toByteArray(JNIEnv *env, std::vector<unsigned char> *pVec)
{
	if (NULL == pVec) {
		return NULL;
	}

	jbyteArray byteArray = env->NewByteArray((int)pVec->size());
	env->SetByteArrayRegion(byteArray, 0, (int)pVec->size(), (jbyte *)&(*pVec)[0]);
	return byteArray;
}

ByteVectorPtr fromByteArray(JNIEnv *env, jbyteArray bytes)
{
	if (NULL == bytes) {
		return ByteVectorPtr();
	}

	jbyte* rawBytes = env->GetByteArrayElements(bytes, NULL);
	ByteVectorPtr answer(new std::vector<unsigned char>(rawBytes, rawBytes + env->GetArrayLength(bytes)));
	env->ReleaseByteArrayElements(bytes, rawBytes, JNI_ABORT);
	return answer;
}

utf16string convertJString(JNIEnv *env, jstring str)
{
	if (NULL == str) {
		return "";
	}

	const char *utf = env->GetStringUTFChars(str, NULL);
	utf16string answer(utf);
	env->ReleaseStringUTFChars(str, utf);
	return answer;
}

jstring returnUtf16String(JNIEnv *env, utf16string const &str)
{
	return (jstring)env->NewLocalRef((jstring)str.getPlatformString());
}

extern "C" JNIEXPORT jint Liblowladb_JNI_OnLoad(JavaVM* vm, void* reserved)
{
	__android_log_print(ANDROID_LOG_INFO, "LowlaDB-NDK", "JNI_OnLoad, vm: %ld", (long)vm);

	g_vm = vm;

	JNIEnv *env;
	vm->GetEnv((void**)&env, JNI_VERSION_1_6);

	// Work around the JNI limitation of FindClass needing to be on Java thread by using ClassLoader directly...
    jclass integrationClass = env->FindClass("io/lowla/lowladb/platform/android/Integration");
    jclass classClass = env->FindClass("java/lang/Class");
    jclass classLoaderClass = env->FindClass("java/lang/ClassLoader");
    jmethodID getClassLoaderMethod = env->GetMethodID(classClass, "getClassLoader", "()Ljava/lang/ClassLoader;");
    g_classLoader = env->NewGlobalRef(env->CallObjectMethod(integrationClass, getClassLoaderMethod));
    g_loadClass = env->GetMethodID(classLoaderClass, "loadClass",
                                    "(Ljava/lang/String;Z)Ljava/lang/Class;");

	__android_log_print(ANDROID_LOG_INFO, "LowlaDB-NDK", "JNI_OnLoad completed");

	return JNI_VERSION_1_6;
}
