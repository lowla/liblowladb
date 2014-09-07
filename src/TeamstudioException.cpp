/*
 *  TeamstudioException.cpp
 *  Unplugged
 *
 *  Created by Alexander Strakovich on 4/20/11.
 *  Copyright 2011 Teamstudio, Inc. All rights reserved.
 *
 */

#include "TeamstudioException.h"

TeamstudioException::TeamstudioException(const utf16string &message) : m_message(message) {
}

TeamstudioException::TeamstudioException(const utf16string &message, const TeamstudioException &e) : m_message(message + ": " + e.what()) {
}

const char* TeamstudioException::what() const throw() {
	return m_message.c_str();
}

StringIndexOutOfBoundsException::StringIndexOutOfBoundsException(const utf16string &message) : TeamstudioException(message)
{
}