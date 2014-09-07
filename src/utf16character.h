//
//  utf16character.h
//  Unplugged
//
//  Created by Mark Dixon on 7/12/12.
//  Copyright (c) 2012 Teamstudio, Inc. All rights reserved.
//

#ifndef Unplugged_jcharacter_h
#define Unplugged_jcharacter_h

typedef unsigned short utf16char;

class utf16character
{
public:
    static bool isLetter(utf16char c);
};

#endif
