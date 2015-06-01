/*
 *  integration_android.m
 *  LowlaDB
 *
 *  Created by Mark Dixon on 2/9/11.
 *  Copyright 2011 Teamstudio, Inc. All rights reserved.
 *
 */

#include "windows.h"
#include "shlobj.h"
#include "TeamstudioException.h"
#include "integration.h"

static void fillInDataDirectory(PWSTR buf)
{
	PWSTR szMyDocuments;
	HRESULT hresult = SHGetKnownFolderPath(FOLDERID_Documents, 0, NULL, &szMyDocuments);

	wcscpy(buf, szMyDocuments);
	wcscat(buf, L"\\lowla\\tests");
	CoTaskMemFree(szMyDocuments);
}

utf16string SysGetDataDirectory() {
	wchar_t buf[MAX_PATH];

	fillInDataDirectory(buf);

	CreateDirectory(buf, NULL);

	return utf16string((utf16char *)buf, 0, wcslen(buf));
}

static void listDirectory(LPWSTR dir, int cchPrefix, std::vector<utf16string> *answer)
{
	WIN32_FIND_DATA fd;
	wchar_t szFindPattern[MAX_PATH];
	wcscpy(szFindPattern, dir);
	wcscat(szFindPattern, L"\\*");
	HANDLE hFind = FindFirstFile(szFindPattern, &fd);
	if (INVALID_HANDLE_VALUE == hFind) {
		return;
	}
	do {
		wchar_t szBuf[MAX_PATH];
		wcscpy(szBuf, dir);
		wcscat(szBuf, L"\\");
		wcscat(szBuf, fd.cFileName);
		if (fd.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) {
			if (fd.cFileName[0] != L'.') {
				listDirectory(szBuf, cchPrefix, answer);
			}
		}
		else {
			utf16string str((utf16char *)szBuf, cchPrefix, wcslen(szBuf) - cchPrefix);
			answer->push_back(str);
		}
	} while (0 != FindNextFile(hFind, &fd));
	FindClose(hFind);
}

std::vector<utf16string> SysListFiles() {
	std::vector<utf16string> answer;
	wchar_t szBuf[MAX_PATH];
	fillInDataDirectory(szBuf);
	listDirectory(szBuf, wcslen(szBuf) + 1, &answer);
	return answer;
}

utf16string SysNormalizePath(utf16string const& path) {
    utf16string answer = path.replace('\\', '/');
    return answer;
}

static std::map<utf16string, utf16string> sm_properties;

utf16string SysGetProperty(const utf16string &key, const utf16string &defaultValue)
{
	auto it = sm_properties.find(key);
	if (it == sm_properties.end()) {
		return defaultValue;
	}
	else {
		return it->second;
	}
}

void SysSetProperty(const utf16string &key, const utf16string &value) {
	sm_properties[key] = value;
}

bool SysConfigureFile(const utf16string& filename) {
    return true;
}

void SysSleepMillis(int millis)
{
	Sleep(millis);
}

void SysLogMessage(int level, utf16string const &context, utf16string const &message) 
{
}
