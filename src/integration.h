/*
 *  integration.h
 *  Unplugged
 *
 *  Created by Mark Dixon on 2/9/11.
 *  Copyright 2011 Teamstudio, Inc. All rights reserved.
 *
 */

#if !defined(_INTEGRATION_H)
#define _INTEGRATION_H

#include <map>
#include <memory>
#include <vector>
#include <iostream>
#include <string>

#include "utf16string.h"

extern const char *OPTION_RO_USERNAME_FULL;
extern const char *OPTION_RO_LAST_REPLICATE;
extern const char *OPTION_RO_LAST_SYNC;
extern const char *OPTION_HOME_SERVER;
extern const char *OPTION_HOME_USER;
extern const char *OPTION_HOME_PASSWORD;
extern const char *OPTION_HOME_HTTPS;
extern const char *OPTION_AUTO_LAUNCH;
extern const char *OPTION_SYNC_REMINDERS;
extern const char *OPTION_PASSCODE_LOCK;
extern const char *OPTION_PASSCODE_DELAY;
extern const char *OPTION_PASSCODE_REQUIRED;
extern const char *OPTION_DEVELOPMENT_SERVER;


utf16string SysGetDeviceIdentifier();
utf16string SysGetPlatformString(int stringID);
utf16string SysGetDataDirectory();
utf16string SysGetExecutableDirectory();
void SysSetProperty(const utf16string &key, const utf16string &value);
utf16string SysGetProperty(const utf16string &key, const utf16string &defaultValue);
void SysLaunchApp(const utf16string &app, const std::vector<utf16string> &args);


void SysSleepMillis(int millis);

bool SysLoadResource(const utf16string &resourcePath, ByteVectorPtr *resData, utf16string *eTag);
std::vector<utf16string> SysListFiles();

void SysLogMessage(int level, utf16string const &context, utf16string const &message);
utf16string SysGetVersion();
bool SysConfigureFile(const utf16string &filename);

class SysLock
{
public:
    virtual void lock() = 0;
    virtual void unlock() = 0;
};

class SysLockHolder
{
public:
    SysLockHolder(SysLock *lock) : m_lock(lock)
    {
        m_lock->lock();
    }
    
    ~SysLockHolder()
    {
        m_lock->unlock();
    }
private:
    SysLock *m_lock;
};

SysLock *SysCreateRecursiveLock();

utf16string SysGuessContentTypeFromName(const utf16string &name);
utf16string SysNormalizePath(utf16string const &path);
bool SysFileExists(const utf16string &path);
bool SysFileRemove(const utf16string &path);
void SysCreatePathForFile(const utf16string &path);
utf16string SysCreateTempFile(const utf16string &originalPath, ByteVectorPtr contents);

void SysSetTls(int key, void *value);
void *SysGetTls(int key);

void SysConfigureDemoServer();
ByteVectorPtr SysGetUploadedFileData(utf16string const& submittedVal);

void SysGetLocale(std::vector<utf16string> *plocale);

#endif  //_INTEGRATION_H
