/*
 *  TeamstudioException.h
 *  Unplugged
 *
 *  Created by Alexander Strakovich on 4/20/11.
 *  Copyright 2011 Teamstudio, Inc. All rights reserved.
 *
 */

#if !defined(_TEAMSTUDIOEXCEPTION_H)
#define _TEAMSTUDIOEXCEPTION_H

#include <exception>
#include "utf16string.h"

class TeamstudioException : public std::exception {
public:
	TeamstudioException(const utf16string &message);
	TeamstudioException(const utf16string &message, const TeamstudioException &e);
	virtual ~TeamstudioException() throw() {}
	virtual const char* what() const throw();
private:
	utf16string m_message;
};

class StringIndexOutOfBoundsException : public TeamstudioException
{
public:
    StringIndexOutOfBoundsException(const utf16string &message);
};

class DatabaseNotFoundException : public TeamstudioException
{
public:
    DatabaseNotFoundException(const utf16string &message) : TeamstudioException(message) { }
};

#endif  //_TEAMSTUDIOEXCEPTION_H
