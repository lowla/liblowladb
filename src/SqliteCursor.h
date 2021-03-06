/*
 *  SqliteCursor.h
 *  Unplugged
 *
 *  Created by  Alexander Strakovich on 5/18/11.
 *  Copyright 2011 Teamstudio, Inc. All rights reserved.
 *
 */

#if !defined(_SQLITECURSOR_H)
#define _SQLITECURSOR_H

#include <memory>
#include <string>

extern "C" {
#include <btreeInt.h>
}

#define CURSOR_READONLY		0
#define CURSOR_READWRITE	1

class SqliteKey;

// Helper class to manage B-tree cursors
class SqliteCursor {
public:
	SqliteCursor();
	~SqliteCursor();
    
    typedef std::shared_ptr<SqliteCursor> ptr;
    
	int create(Btree *pBtree, int iTable, int wrFlag, struct KeyInfo *pKeyInfo);
	int movetoUnpacked(SqliteKey *pKey, i64 intKey, int biasRight, int *pRes);
	int first(int *pRes);
	int	last(int *pRes);
	int next(int *pRes);
	int keySize(i64 *pSize);
	int dataSize(u32 *pSize);
	int data(u32 offset, u32 amt, void *pBuf);
	int key(u32 offset, u32 amt, void *pBuf);
	const void *keyFetch(u32 *pAmt);
	const void *dataFetch(u32 *pAmt);
	int insert(const void *pKey, i64 nKey, const void *pData, int nData, int nZero, bool appendBias, int seekResult);
    int putData(u32 offset, u32 amt, const void *pData);
	int deleteCurrent();
	int deleteKey(SqliteKey *pKey);
	int dropTable();
	int close();
	bool isEof();
	bool isEmpty();
    bool isSeekMatch(UnpackedRecord *pIdxKey);
    i64 count();
    
private:
	BtCursor cursor;
	bool open;
};

#endif  //_SQLITECURSOR_H
