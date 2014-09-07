/*
 *  integration_ios.m
 *  Unplugged
 *
 *  Created by Mark Dixon on 2/9/11.
 *  Copyright 2011 Teamstudio, Inc. All rights reserved.
 *
 */

#include <Foundation/Foundation.h>
#import <MobileCoreServices/MobileCoreServices.h>
#include <Security/Security.h>

#include <sys/xattr.h>

#include "TeamstudioException.h"

#include "integration.h"
#include "utf16stringbuilder.h"

const char *OPTION_RO_DEVICE_ID = "ro.device.id";
const char *OPTION_HOME_PASSWORD = "home.password";
const char *OPTION_PASSCODE_LOCK = "unp.passcode.lock";

utf16string SysGetDeviceIdentifier() {
    utf16string deviceId = SysGetProperty(OPTION_RO_DEVICE_ID, "");
    if (deviceId.isEmpty()) {
        CFUUIDRef uuidRef = CFUUIDCreate(NULL);
        CFStringRef uuidStringRef = CFUUIDCreateString(NULL, uuidRef);
        CFRelease(uuidRef);
        char szBuffer[100];
        CFStringGetCString(uuidStringRef, szBuffer, 100, kCFStringEncodingUTF8);
        CFRelease(uuidStringRef);
        deviceId = szBuffer;
        SysSetProperty(OPTION_RO_DEVICE_ID, deviceId);
    }
    return deviceId;
}

// Internal helper function. The caller is responsible for creating and destroying an AutoReleasePool.
static NSString *getDataDirectory() {
	NSArray *paths = NSSearchPathForDirectoriesInDomains(NSDocumentDirectory, NSUserDomainMask, YES);
	NSString *documentsDirectory = paths[0];
	NSString *dataDirectory = [documentsDirectory stringByAppendingPathComponent:@"data"];
    NSFileManager *files = [NSFileManager defaultManager];
    [files createDirectoryAtPath:dataDirectory withIntermediateDirectories:YES attributes:nil error:nil];
    
    return dataDirectory;
}

utf16string SysGetDataDirectory() {
    static utf16string dataDirectory;
    static dispatch_once_t onceToken = 0;
    dispatch_once(&onceToken, ^{
        dataDirectory = [getDataDirectory() UTF8String];
    });
    return dataDirectory;
}

NSMutableDictionary *getKeychainQuery(NSString *key)
{
    return [NSMutableDictionary dictionaryWithObjectsAndKeys:
            (__bridge id)kSecClassGenericPassword, (__bridge id)kSecClass,
            key, (__bridge id)kSecAttrService,
            key, (__bridge id)kSecAttrAccount,
            (__bridge id)kSecAttrAccessibleWhenUnlocked, (__bridge id)kSecAttrAccessible, (void*)nil ];

}

void SysSetProperty(const utf16string &key, const utf16string &value) {
    @autoreleasepool {
        if (key.startsWith(OPTION_HOME_PASSWORD) || key == OPTION_RO_DEVICE_ID || key == OPTION_PASSCODE_LOCK) {
            NSMutableDictionary *keychainQuery = getKeychainQuery(@(key.c_str()));
            SecItemDelete((__bridge CFDictionaryRef)keychainQuery);
            keychainQuery[(__bridge id)kSecValueData] = [NSKeyedArchiver archivedDataWithRootObject:@(value.c_str())];
            SecItemAdd((__bridge CFDictionaryRef)keychainQuery, NULL);
        }
        else{
            NSUserDefaults *userDefaults = [NSUserDefaults standardUserDefaults];
            [userDefaults setObject:@(value.c_str())
                             forKey:@(key.c_str())];
            [userDefaults synchronize];
        }
    }
}

utf16string SysGetProperty(const utf16string &key, const utf16string &defaultValue) {
    @autoreleasepool {
        NSString *value = nil;
        if (key.startsWith(OPTION_HOME_PASSWORD) || key == OPTION_RO_DEVICE_ID || key == OPTION_PASSCODE_LOCK) {
            NSMutableDictionary *keychainQuery = getKeychainQuery(@(key.c_str()));
            keychainQuery[(__bridge id)kSecReturnData] = (id)kCFBooleanTrue;
            keychainQuery[(__bridge id)kSecMatchLimit] = (__bridge id)kSecMatchLimitOne;
            CFDataRef keyData = NULL;
            if (SecItemCopyMatching((__bridge CFDictionaryRef)keychainQuery, (CFTypeRef *)&keyData) == noErr) {
                @try {
                    value = (NSString*)[NSKeyedUnarchiver unarchiveObjectWithData:(__bridge NSData *)keyData];
                }
                @catch (NSException *e) {
                    NSLog(@"Unarchive of %s failed: %@", key.c_str(), e);
                }
                @finally {}
            }
            if (keyData) { 
                CFRelease(keyData);
            }
        }
        else{
            value = [[NSUserDefaults standardUserDefaults] stringForKey:@(key.c_str())];
            utf16string answer = (value != nil) ? [value UTF8String] : defaultValue;
        }
        return (value != nil) ? [value UTF8String] : defaultValue;
    }
}


std::vector<utf16string> SysListFiles() {
    @autoreleasepool {
        NSFileManager *localFileManager=[[NSFileManager alloc] init];
        NSDirectoryEnumerator *dirEnum = [localFileManager enumeratorAtPath:getDataDirectory()];
        
        std::vector<utf16string> answer;
        NSString *file;
        while (nil != (file = [dirEnum nextObject])) {
            if (NSFileTypeRegular == [dirEnum fileAttributes][NSFileType]) {
                answer.push_back([file UTF8String]);
            }
        }
        return answer;
    }
}

void SysLogMessage(int level, utf16string const &context, utf16string const &message)
{
    @autoreleasepool {
        utf16stringbuilder buf;
        switch (level) {
            case 0:
                buf << "DEBUG: ";
                break;
                
            case 1:
                buf << "INFO: ";
                break;
                
            case 2:
                buf << "WARN: ";
                break;
            
            case 3:
                buf << "ERROR: ";
                break;
                
            default:
                buf << "UNK: ";
                break;
        }
        
        if (!context.isEmpty()) {
            buf << "(" << context << ") ";
        }
        
        buf << message;
        
        NSLog(@"%s", buf.toString().c_str());
    }
}

utf16string SysGetVersion()
{
    @autoreleasepool {
        NSString *version = [NSString stringWithFormat:@"%@ (%@)", [[NSBundle mainBundle] infoDictionary][@"CFBundleShortVersionString"], [[NSBundle mainBundle] infoDictionary][@"CFBundleVersion"]];
        return utf16string([version UTF8String]);
    }
}

utf16string SysNormalizePath(utf16string const& path)
{
    utf16string answer = path.replace('\\', '/');
    return answer;
}

static bool addSkipBackupAttributeToFile(const utf16string &filename)
{
    if (&NSURLIsExcludedFromBackupKey == nil) { // iOS <= 5.0.1
        const char* attrName = "com.apple.MobileBackup";
        u_int8_t attrValue = 1;
        
        int result = setxattr(filename.c_str(), attrName, &attrValue, sizeof(attrValue), 0, 0);
        return result == 0;
    } else { // iOS >= 5.1
        NSError *error = nil;
        NSURL *url = [NSURL fileURLWithPath:@(filename.c_str())];
        
        [url setResourceValue:@YES forKey:NSURLIsExcludedFromBackupKey error:&error];
        return error == nil;
    }
}

bool SysConfigureFile(const utf16string& filename) {
    utf16string normalized = SysNormalizePath(filename);
    @autoreleasepool {
        NSError *error = nil;
        NSDictionary *fileAttributes = @{NSFileProtectionKey: NSFileProtectionComplete};
        if (![[NSFileManager defaultManager] setAttributes:fileAttributes ofItemAtPath:@(normalized.c_str()) error:&error]){
            // handle error
            SysLogMessage(3, [[error localizedFailureReason] UTF8String], [[error localizedDescription] UTF8String]);
            return false;
        }
        if (!addSkipBackupAttributeToFile(normalized)) {
            return false;
        }
        return true;
    }
}

void SysSleepMillis(int millis)
{
    usleep(millis * 1000);
}

class RecursiveLock : public SysLock
{
public:
    RecursiveLock();
    ~RecursiveLock();
    
    void lock();
    void unlock();
private:
    NSRecursiveLock *m_lock;
};

RecursiveLock::RecursiveLock()
{
    m_lock = [[NSRecursiveLock alloc] init];
}

RecursiveLock::~RecursiveLock()
{
}

void RecursiveLock::lock()
{
    [m_lock lock];
}

void RecursiveLock::unlock()
{
    [m_lock unlock];
}

SysLock *SysCreateRecursiveLock()
{
    return new RecursiveLock();
}

class IdleTimerDisabler
{
public:
    IdleTimerDisabler();
    ~IdleTimerDisabler();
    
private:
    BOOL m_savedValue;
};

bool SysFileExists(const utf16string &path)
{
    @autoreleasepool {
        NSFileManager *files = [NSFileManager defaultManager];
        return [files isReadableFileAtPath:@(SysNormalizePath(path).c_str())];
    }
}

bool SysFileRemove(const utf16string &path)
{
    @autoreleasepool {
        NSFileManager *files = [NSFileManager defaultManager];
        return [files removeItemAtPath:@(SysNormalizePath(path).c_str()) error:nil];
    }
}

void SysCreatePathForFile(const utf16string &path)
{
    @autoreleasepool {
        NSFileManager *files = [NSFileManager defaultManager];
        NSString *pathString = @(SysNormalizePath(path).c_str());
        NSString *directory = [pathString stringByDeletingLastPathComponent];
        
        [files createDirectoryAtPath:directory withIntermediateDirectories:YES attributes:nil error:nil];
    }
}

utf16string SysCreateTempFile(const utf16string &originalPath, ByteVectorPtr contents)
{
    @autoreleasepool {
        NSString *fullpath = [NSString stringWithUTF8String:SysNormalizePath(originalPath).c_str()];
        NSString *extension = [fullpath pathExtension];
        NSString *getImagePath = [NSTemporaryDirectory() stringByAppendingPathComponent:@"saved"];
        if (0 < [extension length]) {
            getImagePath = [getImagePath stringByAppendingPathExtension:extension];
        }
        NSData *resource = [NSData dataWithBytesNoCopy:&contents->front() length:contents->size() freeWhenDone:NO];
        [resource writeToFile:getImagePath atomically:YES];
        
        return utf16string([[[NSURL fileURLWithPath:getImagePath] absoluteString] UTF8String]);
    }
}

void SysSetTls(int key, void *value)
{
    @autoreleasepool {
        NSMutableDictionary *dict = [[NSThread currentThread] threadDictionary];
        dict[@(key)] = [NSValue valueWithPointer:value];
    }
}

void *SysGetTls(int key)
{
    @autoreleasepool {
        NSMutableDictionary *dict = [[NSThread currentThread] threadDictionary];
        NSValue *value = dict[@(key)];
        if (nil != value) {
            return [value pointerValue];
        }
        return NULL;
    }
}

