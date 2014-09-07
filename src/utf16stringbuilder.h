//
//  utf16stringbuilder.h
//  Unplugged
//
//  Created by Mark Dixon on 6/18/12.
//  Copyright (c) 2012 Teamstudio, Inc. All rights reserved.
//

#ifndef Unplugged_jstringbuilder_h
#define Unplugged_jstringbuilder_h

#include "utf16string.h"

class utf16stringbuilder
{
public:
    utf16string toString() const;
    
    utf16stringbuilder &operator<<(char c);    
    utf16stringbuilder &operator<<(utf16char c);
    utf16stringbuilder &operator<<(const utf16string &str);
    utf16stringbuilder &operator<<(int i);
    
    utf16stringbuilder &append(char c);
    utf16stringbuilder &append(utf16char c);
    utf16stringbuilder &append(const utf16string &str);
    utf16stringbuilder &append(int i);
    
    utf16stringbuilder &insert(int index, const utf16string &str);
    
    int length();
    void setLength(int newLength);
    
private:
    utf16stringbuilder &insert(std::vector<utf16char>::iterator it, const utf16string &str);
    
    std::vector<utf16char> m_str;
};



#endif
