//
//  utf16string.mm
//  Unplugged
//
//  Created by Mark Dixon on 6/21/12.
//  Copyright (c) 2012 Teamstudio, Inc. All rights reserved.
//

#include <Foundation/Foundation.h>
#include <iomanip>

#include "utf16string.h"
#include "utf16stringbuilder.h"
#include "TeamstudioException.h"

class utf16string::StringData
{
public:
    StringData() { enc = NULL; str = nil; }
    StringData(NSString *strIn) { enc = NULL; str = strIn; }
    ~StringData();
    
    void setEnc(const char *enc);
    
    NSString *str;
    char *enc;
};

utf16string::StringData::~StringData()
{
    if (NULL != enc) {
        free(enc);
    }
}

void utf16string::StringData::setEnc(const char *newEnc)
{
    if (NULL != enc) {
        free(enc);
        enc = NULL;
    }
    enc = strdup(newEnc);
}

const char *utf16string::UTF8("UTF8");
const char *utf16string::ISO_8859_1("ISO-8859-1");

static NSStringEncoding getEncoding(const char *charsetName)
{
    if (charsetName == utf16string::UTF8) {
        return NSUTF8StringEncoding;
    }
    NSStringEncoding enc;
    if (0 == strcmp(utf16string::UTF8, charsetName)) {
        enc = NSUTF8StringEncoding;
    }
    else if (0 == strcmp(utf16string::ISO_8859_1, charsetName)) {
        enc = CFStringConvertEncodingToNSStringEncoding(kCFStringEncodingISOLatin1);
    }
    else {
        throw TeamstudioException("Unknown string encoding: " + utf16string(charsetName));
    }
    return enc;
}

utf16string::utf16string()
{
    m_data = new StringData();
    m_data->str = [[NSString alloc] init];
}

utf16string::utf16string(const utf16string &str)
{
    m_data = new StringData();
    m_data->str = str.m_data->str;
}

utf16string::utf16string(const char *szDefaultEncodedString)
{
    m_data = new StringData();
    m_data->str = [[NSString alloc] initWithCString:szDefaultEncodedString encoding:NSUTF8StringEncoding];
    if (nil == m_data->str) {
        m_data->str = [[NSString alloc] init];
    }
}

utf16string::utf16string(const char *sz, const char *charsetName)
{
    NSStringEncoding enc = getEncoding(charsetName);
    m_data = new StringData();
    m_data->str = [[NSString alloc] initWithCString:sz encoding:enc];
    if (nil == m_data->str) {
        m_data->str = [[NSString alloc] init];
    }
}

utf16string::utf16string(const char *ab, int offset, int length, const char *charsetName)
{
    NSStringEncoding enc = getEncoding(charsetName);
    m_data = new StringData();
    m_data->str = [[NSString alloc] initWithBytes:ab + offset length:length encoding:enc];
    if (nil == m_data->str) {
        m_data->str = [[NSString alloc] init];
    }
}

utf16string::utf16string(const utf16char *ach, int offset, int length)
{
    m_data = new StringData();
    m_data->str = [[NSString alloc] initWithCharacters:ach + offset length:length];
}

utf16string::utf16string(StringData *data)
{
    m_data = data;
}

void utf16string::setData(StringData *data)
{
    delete m_data;
    m_data = data;
}

utf16string::~utf16string() {
    delete m_data;
}

utf16string utf16string::valueOf(utf16char c)
{
    return utf16string(&c, 0, 1);
}

utf16string utf16string::valueOf(double d)
{
    throw TeamstudioException("utf16string::valueOf(double) is currently unsupported - needs dtoa");
}

utf16string utf16string::valueOf(long long ll)
{
    char szBuffer[50];
    snprintf(szBuffer, 50, "%lld", ll);
    szBuffer[49] = '\0';
    return szBuffer;
}

utf16string utf16string::valueOf(unsigned long dw)
{
    char szBuffer[50];
    snprintf(szBuffer, 50, "%lu", dw);
    szBuffer[49] = '\0';
    return szBuffer;
}

utf16string utf16string::valueOf(int i) {
    char szBuffer[50];
    sprintf(szBuffer, "%d", i);
    return utf16string(szBuffer);
}

utf16char utf16string::charAt(int i) const
{
    return [m_data->str characterAtIndex:i];
}

const char *utf16string::c_str() const
{
    return c_str("UTF8");
}

const char *utf16string::c_str(const char *charsetName) const
{
    @autoreleasepool {
        NSStringEncoding enc = getEncoding(charsetName);
        const char *encoded = [m_data->str cStringUsingEncoding:enc];
        if (NULL != encoded) {
            m_data->setEnc(encoded);
        }
        else {
/*
            // We don't include the text in the exception since we're likely to run into problems trying to display it.
            // We carefully log it, tho.
            if (LogFactory::getLevel() <= LOG_DEBUG) {
                utf16stringbuilder output;
                output << "Error converting to '" << charsetName << "'. String data: ";
                int cch = length();
                std::vector<utf16char> chars(cch);
                getChars(0, cch, &chars[0], 0);
                for (int i = 0 ; i < cch ; ++i) {
                    output << "\\" << utils::to_hex<utf16char>(chars[i]).c_str();
                }
                LogFactory::getLog("utf16string")->debug(output.toString());
            }
 */
            throw TeamstudioException("Unable to convert text to " + utf16string(charsetName));
        }
        return m_data->enc;
    }
}

ByteVectorPtr utf16string::getBytes() const
{
    const char *start = c_str();
    return ByteVectorPtr(new std::vector<unsigned char>(start, start + strlen(start)));
}

ByteVectorPtr utf16string::getBytes(const char *charsetName) const
{
    const char *start = c_str(charsetName);
    return ByteVectorPtr(new std::vector<unsigned char>(start, start + strlen(start)));
}

void utf16string::getChars(int srcBegin, int srcEnd, utf16char *dest, int dstBegin) const
{
    [m_data->str getCharacters:dest + dstBegin range:NSMakeRange(srcBegin, srcEnd - srcBegin)];
}

bool utf16string::isEmpty() const
{
    return 0 == [m_data->str length];
}

int utf16string::length() const
{
    return (int)[m_data->str length];
}

bool utf16string::equals(const utf16string &other) const
{
    return [m_data->str isEqualToString:other.m_data->str];
}

int utf16string::compareTo(const utf16string &other) const
{
    return [m_data->str compare:other.m_data->str];
}

bool utf16string::equalsIgnoreCase(const utf16string &other) const
{
    return 0 == [m_data->str caseInsensitiveCompare:other.m_data->str];
}

bool utf16string::endsWith(const utf16string &suffix) const
{
    return [m_data->str hasSuffix:suffix.m_data->str];
}

bool utf16string::startsWith(const utf16string &prefix) const
{
    return [m_data->str hasPrefix:prefix.m_data->str];
}

int utf16string::indexOf(const utf16string &str) const
{
    NSRange range = [m_data->str rangeOfString:str.m_data->str];
    return range.location == NSNotFound ? -1 : (int)range.location;
}

int utf16string::indexOf(const utf16string &str, int fromIndex) const
{
    NSRange searchRange = NSMakeRange(fromIndex, [m_data->str length] - fromIndex);
    if (searchRange.length <= 0) {
        return -1;
    }
    NSRange range = [m_data->str rangeOfString:str.m_data->str options:0 range:searchRange];
    return range.location == NSNotFound ? -1 : (int)range.location;
}

int utf16string::indexOf(utf16char ch) const
{
    utf16string str(&ch, 0, 1);
    return indexOf(str);
}

int utf16string::indexOf(utf16char ch, int fromIndex) const
{
    utf16string str(&ch, 0, 1);
    return indexOf(str, fromIndex);
}

int utf16string::lastIndexOf(utf16char ch) const
{
    utf16string str(&ch, 0, 1);
    return lastIndexOf(str);
}

int utf16string::lastIndexOf(const utf16string &str) const
{
    NSRange range = [m_data->str rangeOfString:str.m_data->str options:NSBackwardsSearch];
    return range.location == NSNotFound ? -1 : (int)range.location;
}

utf16string utf16string::substring(int beginIndex) const
{
    @autoreleasepool {
        @try {
            return utf16string(new StringData([m_data->str substringFromIndex:beginIndex]));
        }
        @catch (NSException *exception) {
            utf16string message([[exception reason] UTF8String]);
            throw StringIndexOutOfBoundsException(message);
        }
    }
}

utf16string utf16string::substring(int beginIndex, int endIndex) const
{
    @autoreleasepool {
        @try {
            return utf16string(new StringData([m_data->str substringWithRange:NSMakeRange(beginIndex, endIndex - beginIndex)]));
        }
        @catch (NSException *exception) {
            utf16string message([[exception reason] UTF8String]);
            throw StringIndexOutOfBoundsException(message);
        }
    }
}

utf16string utf16string::toLowerCase() const
{
    @autoreleasepool {
        return utf16string(new StringData([m_data->str lowercaseString]));
    }
}

utf16string utf16string::toUpperCase() const
{
    @autoreleasepool {
        return utf16string(new StringData([m_data->str uppercaseString]));
    }
}

utf16string utf16string::trim() const
{
    int start = 0;
    while (start < length() && charAt(start) <= 0x20) {
        ++start;
    }
    int end = length() - 1;
    while (start < end && charAt(end) <= 0x20) {
        --end;
    }
    return substring(start, end + 1);
}

utf16string utf16string::replace(utf16char oldChar, utf16char newChar) const
{
    utf16string target(&oldChar, 0, 1);
    utf16string replacement(&newChar, 0, 1);
    return replace(target, replacement);
}

utf16string utf16string::replace(const utf16string &target, const utf16string &replacement) const
{
    @autoreleasepool {
        return utf16string(new StringData([m_data->str stringByReplacingOccurrencesOfString:target.m_data->str withString:replacement.m_data->str]));
    }
}

utf16string &utf16string::operator=(const utf16string &str)
{
    setData(new StringData(str.m_data->str));
    return *this;
}

utf16string &utf16string::operator+=(const utf16string &str)
{
    @autoreleasepool {
        NSString *dataNew = [m_data->str stringByAppendingString:str.m_data->str];
        setData(new StringData(dataNew));
        return *this;
    }
}

utf16string &utf16string::operator+=(const char *s)
{
    return operator+=(utf16string(s));
}

utf16string &utf16string::operator+=(utf16char c)
{
    return operator+=(utf16string(&c, 0, 1));
}

utf16string operator+(const utf16string &lhs, const utf16string &rhs)
{
    utf16string answer(lhs);
    answer += rhs;
    return answer;
}

utf16string operator+(const char *lhs, const utf16string &rhs)
{
    utf16string answer(lhs);
    answer += rhs;
    return answer;
}

utf16string operator+(const utf16string &lhs, const char *rhs)
{
    utf16string answer(lhs);
    answer += rhs;
    return answer;
}

utf16string operator+(const utf16string &lhs, utf16char rhs)
{
    utf16string answer(lhs);
    answer += rhs;
    return answer;
}

utf16string operator+(utf16char lhs, const utf16string &rhs)
{
    utf16string answer;
    answer += lhs;
    answer += rhs;
    return answer;
}

bool operator<(const utf16string &lhs, const utf16string &rhs)
{
    return lhs.compareTo(rhs) < 0;
}

bool operator==(const utf16string &lhs, const utf16string &rhs)
{
    return lhs.equals(rhs);
}

bool operator==(const char *lhs, const utf16string &rhs)
{
    return rhs.equals(lhs);
}

bool operator==(const utf16string &lhs, const char *rhs)
{
    return lhs.equals(rhs);
}

bool operator!=(const utf16string &lhs, const utf16string &rhs)
{
    return !operator==(lhs, rhs);
}

bool operator!=(const char *lhs, const utf16string &rhs)
{
    return !operator==(lhs, rhs);
}

bool operator!=(const utf16string &lhs, const char *rhs)
{
    return !operator==(lhs, rhs);
}

std::istream &operator>>(std::istream &is, utf16string &str)
{
    char sz[50];
    is >> std::setw(49) >> sz;
    str = sz;
    return is;
}

std::size_t utf16string::hash() const
{
    return [m_data->str hash];
}

std::ostream &operator<<(std::ostream &stream, const utf16string &str)
{
    return stream << str.c_str();
}

