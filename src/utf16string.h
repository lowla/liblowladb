//
//  utf16string.h
//  Unplugged
//
//  Created by Mark Dixon on 5/20/12.
//  Copyright (c) 2012 Teamstudio, Inc. All rights reserved.
//

#ifndef Unplugged_jstring_h
#define Unplugged_jstring_h

#include "utf16character.h"
#include "bytevector.h"

class utf16string
{
public:
    // These are char* rather than utf16string to a) avoid circular initializion bugs with
    // string constants and b) improve performance
    static const char *UTF8;
    static const char *ISO_8859_1;
    
    utf16string();
    utf16string(const utf16string &str);
    utf16string(const char *szDefaultEncodedString);
    utf16string(const char *sz, const char *charsetName);
    utf16string(const char *ab, int offset, int length, const char *charsetName);
    utf16string(const utf16char *ach, int offset, int length);
    ~utf16string();
    
    static utf16string valueOf(int i);
    static utf16string valueOf(utf16char c);
    static utf16string valueOf(unsigned long dw);
    static utf16string valueOf(double d);
    static utf16string valueOf(long long ll);
    
    utf16char charAt(int i) const;
    const char *c_str() const;
    const char *c_str(const char *charsetName) const;
    ByteVectorPtr getBytes() const;
    ByteVectorPtr getBytes(const char *charsetName) const;
    void getChars(int srcBegin, int srcEnd, utf16char *dest, int dstBegin) const;
    
    bool isEmpty() const;
    int length() const;
    
    bool equals(const utf16string &other) const;
    bool equalsIgnoreCase(const utf16string &other) const;
    bool endsWith(const utf16string &suffix) const;
    bool startsWith(const utf16string &prefix) const;
    int compareTo(const utf16string &other) const;
    
    int indexOf(const utf16string &str) const;
    int indexOf(const utf16string &str, int fromIndex) const;
    int indexOf(utf16char ch) const;
    int indexOf(utf16char ch, int fromIndex) const;
    int lastIndexOf(utf16char ch) const;
    int lastIndexOf(const utf16string &str) const;
    
    utf16string substring(int beginIndex) const;
    utf16string substring(int beginIndex, int endIndex) const;
    
    utf16string toLowerCase() const;
    utf16string toUpperCase() const;
    utf16string trim() const;
    
    utf16string replace(utf16char oldChar, utf16char newChar) const;
    utf16string replace(const utf16string &target, const utf16string &replacement) const;

    utf16string &operator=(const utf16string &str);
    utf16string &operator+=(const utf16string &str);
    utf16string &operator+=(const char *s);
    utf16string &operator+=(utf16char c);

    void *getPlatformString() const;

    std::size_t hash() const;
    friend std::istream &operator>>(std::istream &is, utf16string &str);
    
private:
    class StringData;
    
    utf16string(StringData *data);
    void setData(StringData *data);
    StringData *m_data;
};

utf16string operator+ (const utf16string &lhs, const utf16string &rhs);
utf16string operator+ (const char *lhs, const utf16string &rhs);
utf16string operator+ (const utf16string &lhs, const char *rhs);
utf16string operator+ (const utf16string &lhs, utf16char rhs);
utf16string operator+ (utf16char lhs, const utf16string &rhs);

// For std::unordered_map
namespace std {
    template <>
    class hash<utf16string>{
        public :
        size_t operator()(const utf16string &str) const
        {
            return str.hash();
        }
    };
};

// For std::map
bool operator< (const utf16string &lhs, const utf16string &rhs);

// For gtest EXPECT_EQ
bool operator== (const utf16string &lhs, const utf16string &rhs);
bool operator== (const char *lhs, const utf16string &rhs);
bool operator== (const utf16string &lhs, const char *rhs);

bool operator!= (const utf16string &lhs, const utf16string &rhs);
bool operator!= (const char *lhs, const utf16string &rhs);
bool operator!= (const utf16string &lhs, const char *rhs);

std::ostream &operator<<(std::ostream &stream, const utf16string &str);

#endif
