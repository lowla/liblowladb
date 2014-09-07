/*
 *  SqliteKey.cpp
 *  Unplugged
 *
 *  Created by  Alexander Strakovich on 6/3/11.
 *  Copyright 2011 Teamstudio, Inc. All rights reserved.
 *
 */

#include "SqliteKey.h"

SqliteKey::SqliteKey(long pageNo) : m_pageNo(pageNo) {
}

long SqliteKey::getPageNo() const {
	return m_pageNo;
}

void SqliteKey::setPageNo(long pageNo) {
	m_pageNo = pageNo;
}

void SqliteKey::deleteUnpackedRecord(UnpackedRecord *precord) {
	sqlite3VdbeDeleteUnpackedRecord(precord);
}
