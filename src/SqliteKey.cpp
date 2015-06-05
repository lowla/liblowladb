/*
 *  SqliteKey.cpp
 *  Unplugged
 *
 *  Created by  Alexander Strakovich on 6/3/11.
 *  Copyright 2011 Teamstudio, Inc. All rights reserved.
 *
 */

#include "SqliteKey.h"

SqliteKey::SqliteKey(i64 recordId) : m_id(recordId) {
    m_idSize = sqlite3VarintLen(m_id);
}

i64 SqliteKey::getId() const {
	return m_id;
}

void SqliteKey::setId(i64 recordId) {
	m_id = recordId;
    m_idSize = sqlite3VarintLen(m_id);
}

int SqliteKey::getIdSize() const {
    return m_idSize;
}

void SqliteKey::deleteUnpackedRecord(UnpackedRecord *precord) {
	// TODO
	//sqlite3VdbeDeleteUnpackedRecord(precord);
}
