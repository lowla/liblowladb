//
//  utf16stringbuilder.cpp
//  Unplugged
//
//  Created by Mark Dixon on 6/22/12.
//  Copyright (c) 2012 Teamstudio, Inc. All rights reserved.
//

#include "TeamstudioException.h"

#include "utf16stringbuilder.h"

utf16string utf16stringbuilder::toString() const
{
    return utf16string(&m_str.front(), 0, m_str.size());
}

utf16stringbuilder &utf16stringbuilder::operator<<(char c)
{
    return append(c);
}

utf16stringbuilder &utf16stringbuilder::operator<<(utf16char c)
{
    return append(c);
}

utf16stringbuilder &utf16stringbuilder::operator<<(const utf16string &str)
{
    return append(str);
}

utf16stringbuilder &utf16stringbuilder::operator<<(int i)
{
    return append(i);
}

utf16stringbuilder &utf16stringbuilder::append(char c)
{
    m_str.push_back(c);
    return *this;
}

utf16stringbuilder &utf16stringbuilder::append(utf16char c)
{
    m_str.push_back(c);
    return *this;
}

utf16stringbuilder &utf16stringbuilder::append(const utf16string &str)
{
    return insert(m_str.end(), str);
}

utf16stringbuilder &utf16stringbuilder::append(int i)
{
    return append(utf16string::valueOf(i));
}

utf16stringbuilder &utf16stringbuilder::insert(int index, const utf16string &str) {
    if (index < 0 || m_str.size() < index) {
        throw StringIndexOutOfBoundsException("String index out of range: " + utf16string::valueOf(index));
    }
    return insert(m_str.begin() + index, str);
}

utf16stringbuilder &utf16stringbuilder::insert(std::vector<utf16char>::iterator it, const utf16string &str) {
    utf16char* ach = new utf16char[str.length()];
    str.getChars(0, str.length(), ach, 0);
    m_str.insert(it, ach, ach + str.length());
    delete[] ach;
    return *this;
}

int utf16stringbuilder::length()
{
    return m_str.size();
}

void utf16stringbuilder::setLength(int newLength)
{
    m_str.resize(newLength, 0);
}