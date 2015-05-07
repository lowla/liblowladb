/*
 *  integration_android.m
 *  LowlaDB
 *
 *  Created by Mark Dixon on 2/9/11.
 *  Copyright 2011 Teamstudio, Inc. All rights reserved.
 *
 */

#include <unistd.h>
#include <jni.h>
#include <android/log.h>

#include "TeamstudioException.h"
#include "JniUtils.h"
#include "integration.h"

utf16string SysGetDataDirectory() {
    JNIEnvWrapper env;

    try {
        jclass clazz = env.FindClass("io/lowla/lowladb/platform/android/Integration");
        jfieldID fid = env.GetStaticFieldID(clazz, "INSTANCE", "Lio/lowla/lowladb/platform/android/Integration;");
        jobject obj = env.GetStaticObjectField(clazz, fid);
    
        jmethodID mid = env.GetMethodID(clazz, "getDataDirectory", "()Ljava/lang/String;");
        jstring dir = (jstring)env.CallObjectMethod(obj, mid);
    
        env.DeleteLocalRef(clazz);
        env.DeleteLocalRef(obj);
        utf16string answer = convertJString(env.get(), dir);
        env.DeleteLocalRef(dir);
        return answer;
    }
    catch (TeamstudioException &e) {
        env.ThrowNew(env.FindClass("java/lang/IllegalStateException"), e.what());
        return "";
    }
}

std::vector<utf16string> SysListFiles() {
    JNIEnvWrapper env;

    std::vector<utf16string> answer;
    try {
        jclass clazz = env.FindClass("io/lowla/lowladb/platform/android/Integration");
        jfieldID fid = env.GetStaticFieldID(clazz, "INSTANCE", "Lio/lowla/lowladb/platform/android/Integration;");
        jobject obj = env.GetStaticObjectField(clazz, fid);
    
        jmethodID mid = env.GetMethodID(clazz, "listFiles", "()[Ljava/lang/String;");
        jobjectArray arr = (jobjectArray)env.CallObjectMethod(obj, mid);
    
        env.DeleteLocalRef(clazz);
        env.DeleteLocalRef(obj);
        
        for (int i = 0 ; i < env.GetArrayLength(arr) ; ++i) {
            answer.push_back(convertJString(env.get(), (jstring)env.GetObjectArrayElement(arr, i)));
        }
        env.DeleteLocalRef(arr);
        return answer;
    }
    catch (TeamstudioException &e) {
        env.ThrowNew(env.FindClass("java/lang/IllegalStateException"), e.what());
        return answer;
    }
}

utf16string SysNormalizePath(utf16string const& path) {
    utf16string answer = path.replace('\\', '/');
    return answer;
}

utf16string SysGetProperty(const utf16string &key, const utf16string &defaultValue)
{
	JNIEnvWrapper env;

    try {
        jclass clazz = env.FindClass("io/lowla/lowladb/platform/android/Integration");
        jfieldID fid = env.GetStaticFieldID(clazz, "INSTANCE", "Lio/lowla/lowladb/platform/android/Integration;");
        jobject obj = env.GetStaticObjectField(clazz, fid);
    
        jmethodID mid = env.GetMethodID(clazz, "getProperty", "(Ljava/lang/String;Ljava/lang/String;)Ljava/lang/String;");
        jstring jstrKey = env.NewStringUTF(key.c_str());
        jstring jstrDefaultValue = env.NewStringUTF(defaultValue.c_str());
        jstring jstrAnswer = (jstring)env.CallObjectMethod(obj, mid, jstrKey, jstrDefaultValue);
    
        env.DeleteLocalRef(jstrDefaultValue);
        env.DeleteLocalRef(jstrKey);
        env.DeleteLocalRef(clazz);
        env.DeleteLocalRef(obj);
        utf16string answer = convertJString(env.get(), jstrAnswer);
        env.DeleteLocalRef(jstrAnswer);
        return answer;
    }
    catch (TeamstudioException &e) {
        env.ThrowNew(env.FindClass("java/lang/IllegalStateException"), e.what());
        return "";
    }
}

void SysSetProperty(const utf16string &key, const utf16string &value) {
	JNIEnvWrapper env;
    
    try {
        jclass clazz = env.FindClass("io/lowla/lowladb/platform/android/Integration");
        jfieldID fid = env.GetStaticFieldID(clazz, "INSTANCE", "Lio/lowla/lowladb/platform/android/Integration;");
        jobject obj = env.GetStaticObjectField(clazz, fid);
    
        jmethodID mid = env.GetMethodID(clazz, "setProperty", "(Ljava/lang/String;Ljava/lang/String;)V");
        jstring jstrKey = env.NewStringUTF(key.c_str());
        jstring jstrValue = env.NewStringUTF(value.c_str());
        env.CallVoidMethod(obj, mid, jstrKey, jstrValue);
    
        env.DeleteLocalRef(jstrValue);
        env.DeleteLocalRef(jstrKey);
        env.DeleteLocalRef(clazz);
        env.DeleteLocalRef(obj);
    }
    catch (TeamstudioException &e) {
        env.ThrowNew(env.FindClass("java/lang/IllegalStateException"), e.what());
        return;
    }
}

bool SysConfigureFile(const utf16string& filename) {
    return true;
}

void SysSleepMillis(int millis)
{
    usleep(millis * 1000);
}

void SysLogMessage(int level, utf16string const &context, utf16string const &message) 
{
}
