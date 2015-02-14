/*
 *  integration_ios.m
 *  Unplugged
 *
 *  Created by Mark Dixon on 2/9/11.
 *  Copyright 2011 Teamstudio, Inc. All rights reserved.
 *
 */

#include "TeamstudioException.h"

#include "integration.h"
#include "utf16stringbuilder.h"

utf16string SysGetDataDirectory() {
    return ".";
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
