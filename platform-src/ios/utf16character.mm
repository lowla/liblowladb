//
//  utf16character.mm
//  Unplugged
//
//  Created by Mark Dixon on 7/12/12.
//  Copyright (c) 2012 Teamstudio, Inc. All rights reserved.
//

#include <Foundation/Foundation.h>

#include "utf16character.h"

bool utf16character::isLetter(utf16char c)
{
    static NSCharacterSet *letters = [NSCharacterSet letterCharacterSet];
    return [letters characterIsMember:c];
}
