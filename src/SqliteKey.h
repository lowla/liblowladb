/*
 *  SqliteKey.h
 *  Unplugged
 *
 *  Created by  Alexander Strakovich on 6/3/11.
 *  Copyright 2011 Teamstudio, Inc. All rights reserved.
 *
 */

#if !defined(_SQLITEKEY_H)
#define _SQLITEKEY_H

extern "C" {
#include "sqlite3.h"
#include "sqliteInt.h"
#include "vdbeInt.h"
}

class SqliteKey {
public:
	virtual int getSize() const = 0;
	virtual int writeToPointer(unsigned char *pc) = 0;
	virtual UnpackedRecord *newUnpackedRecord() = 0;

	void deleteUnpackedRecord(UnpackedRecord *record);
	long getPageNo() const;
	void setPageNo(long pageNo);
protected:
	SqliteKey(long pageNo);
	long m_pageNo;
};

#endif  //_SQLITEKEY_H
