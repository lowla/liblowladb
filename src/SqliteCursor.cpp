/*
 *  SqliteCursor.cpp
 *  Unplugged
 *
 *  Created by  Alexander Strakovich on 5/18/11.
 *  Copyright 2011 Teamstudio, Inc. All rights reserved.
 *
 */

#include "SqliteKey.h"
#include "SqliteCursor.h"

SqliteCursor::SqliteCursor() : open(false) {
	sqlite3BtreeCursorZero(&cursor);
}

SqliteCursor::~SqliteCursor() {
    if (open) {
        close();
    }
}

int SqliteCursor::create(Btree *pBtree, int iTable, int wrFlag, struct KeyInfo *pKeyInfo) {
    int rc = sqlite3BtreeLockTable(pBtree, iTable, wrFlag);
	rc = sqlite3BtreeCursor(pBtree, iTable, wrFlag, pKeyInfo, &cursor);
    if (SQLITE_OK == rc) {
        sqlite3BtreeEnter(pBtree);
        sqlite3BtreeCacheOverflow(&cursor);
    }
	open = (SQLITE_OK == rc);
	return rc;
}

int SqliteCursor::movetoUnpacked(UnpackedRecord *pIdxKey, i64 intKey, int biasRight, int *pRes) {
	int rc = sqlite3BtreeMovetoUnpacked(&cursor, pIdxKey, intKey, biasRight, pRes);
	return rc;
}

int SqliteCursor::first(int *pRes) {
	int rc = sqlite3BtreeFirst(&cursor, pRes);
	return rc;
}

int SqliteCursor::last(int *pRes) {
	int rc = sqlite3BtreeLast(&cursor, pRes);
	return rc;
}

int SqliteCursor::next(int *pRes) {
	int rc = sqlite3BtreeNext(&cursor, pRes);
	return rc;
}

int SqliteCursor::keySize(i64 *pSize) {
	int rc = sqlite3BtreeKeySize(&cursor, pSize);
	return rc;
}

int SqliteCursor::dataSize(u32 *pSize) {
	int rc = sqlite3BtreeDataSize(&cursor, pSize);
	return rc;
}

int SqliteCursor::data(u32 offset, u32 amt, void *pBuf) {
	int rc = sqlite3BtreeData(&cursor, offset, amt, pBuf);
	return rc;
}

int SqliteCursor::key(u32 offset, u32 amt, void *pBuf) {
	int rc = sqlite3BtreeKey(&cursor, offset, amt, pBuf);
	return rc;
}

const void *SqliteCursor::keyFetch(int *pAmt) {
    // We need to retrieve the size before calling KeyFetch to trigger sqlite to parse the current cell
    i64 wdc;
    sqlite3BtreeKeySize(&cursor, &wdc);
    
	return sqlite3BtreeKeyFetch(&cursor, pAmt);
}

const void *SqliteCursor::dataFetch(int *pAmt) {
    // We need to retrieve the size before calling DataFetch to trigger sqlite to parse the current cell
    u32 wdc;
    sqlite3BtreeDataSize(&cursor, &wdc);
    
	return sqlite3BtreeDataFetch(&cursor, pAmt);
}

int SqliteCursor::insert(const void *pKey, i64 nKey, const void *pData, int nData, int nZero, bool appendBias, int seekResult) {
	int rc = sqlite3BtreeInsert(&cursor, pKey, nKey, pData, nData, nZero, appendBias, seekResult);
	return rc;
}

int SqliteCursor::putData(u32 offset, u32 amt, const void *pData) {
    int rc = sqlite3BtreePutData(&cursor, offset, amt, (void *)pData);
    return rc;
}

int SqliteCursor::deleteCurrent() {
	int rc = sqlite3BtreeDelete(&cursor);
	return rc;
}

int SqliteCursor::deleteKey(SqliteKey *pKey) {
	UnpackedRecord *precord = pKey->newUnpackedRecord();
	int res = 0;
	int rc = movetoUnpacked(precord, 0, 0, &res);
	if (0 == res) {
		rc = deleteCurrent();
	}
	pKey->deleteUnpackedRecord(precord);
	return rc;
}

int SqliteCursor::dropTable() {
    assert(!open); // All cursors must be closed before you can drop anything
	Btree *pBtree = cursor.pBtree;
	int iTable = static_cast<int>(cursor.pgnoRoot);
    int iMoved = 0;
    int rc = sqlite3BtreeDropTable(pBtree, iTable, &iMoved);
	return rc;
}

int SqliteCursor::close() {
    sqlite3BtreeLeave(cursor.pBtree);
	int rc = sqlite3BtreeCloseCursor(&cursor);
	open = false;
	return rc;
}

bool SqliteCursor::isEof() {
	return 0 != sqlite3BtreeEof(&cursor);
}

bool SqliteCursor::isEmpty() {
	if (!isEof()) {
		return false;
	}
	int res = 0;
	first(&res);
	return (res != 0);
}

bool SqliteCursor::isSeekMatch(UnpackedRecord *pIdxKey) {
    if (isEof()) {
        return false;
    }
    i64 keyLen;
    keySize(&keyLen);
    char *keyBuf = new char[(int)keyLen];
    key(0, (int)keyLen, keyBuf);
    
    bool answer = (0 == sqlite3VdbeRecordCompare((int)keyLen, keyBuf, pIdxKey));
    delete[] keyBuf;
    return answer;
}

i64 SqliteCursor::getPos() {
    i64 answer;
    if (SQLITE_OK == sqlite3BtreePos(&cursor, &answer)) {
        return answer;
    }
    return 0;
}

i64 SqliteCursor::count() {
    i64 answer;
    if (SQLITE_OK == sqlite3BtreeCount(&cursor, &answer)) {
        return answer;
    }
    return -1;
}