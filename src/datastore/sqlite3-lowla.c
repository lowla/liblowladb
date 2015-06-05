// To upgrade to a newer version of sqlite
// 1. Copy over sqlite3.c and sqlite3.h from the amalgamation
//
// 2. The following methods must be removed/renamed in sqlite3.c to support our custom indexes
// 
//       sqlite3VdbeFindCompare
//       sqlite3VdbeRecordUnpack
// 
//    Replacement implementations are provided in this file
//
// 3. Add the following header at the top of sqlite3.c. The removes the need for
//    every compilation environment to set these critical values correctly.

// Defines for LowlaDB
#define SQLITE_PRIVATE
#define SQLITE_FILE_HEADER "Teamstudio Db 1"
// End of LowlaDB defines

// 4. From the preprocessed sqlite source, copy over the following headers
//       btreeInt.h
//       sqliteInt.h
//       vdbeInt.h
//    along with any other headers that are recursively included. We require several definitions
//    from these files and they are too complex to copy into a lowla-specifc header file.



// End of header

#include "sqliteInt.h"
#include "vdbeInt.h"

void sqlite3VdbeRecordUnpack(
	KeyInfo *pKeyInfo,     /* Information about the record format */
	int nKey,              /* Size of the binary record */
	const void *pKey,      /* The binary record */
	UnpackedRecord *p
	){
	p->pKeyInfo = pKeyInfo;
	p->nField = 1;
	p->default_rc = 0;
	p->aMem->flags = 0;
	p->aMem->n = nKey;
	p->aMem->zMalloc = (char*)pKey;
}

static int lowlaVdbeRecordCompare(int nKey1, const void *pKey1,
	UnpackedRecord *pPKey2) {
	return pPKey2->pKeyInfo->aColl[0]->xCmp(pPKey2, nKey1, pKey1, pPKey2->aMem->n, pPKey2->aMem->zMalloc);
}

RecordCompare sqlite3VdbeFindCompare(UnpackedRecord *p) {
	return lowlaVdbeRecordCompare;
}
