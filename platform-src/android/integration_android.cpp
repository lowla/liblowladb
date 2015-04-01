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

utf16string SysNormalizePath(utf16string const& path)
{
    utf16string answer = path.replace('\\', '/');
    return answer;
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
