//
//  vdbe.m
//  osxnsf
//
//  Created by Mark Dixon on 4/16/10.
//  Copyright 2010 Teamstudio, Inc. All rights reserved.
//

#include "sqliteInt.h"
#include "vdbeInt.h"
#include "vdbe.h"

UnpackedRecord *sqlite3VdbeRecordUnpack(
    KeyInfo *pKeyInfo,     /* Information about the record format */
    int nKey,              /* Size of the binary record */
    const void *pKey,      /* The binary record */
    char *pSpace,          /* Unaligned space available to hold the object */
    int szSpace            /* Size of pSpace[] in bytes */
    ){
    if (szSpace < sizeof(UnpackedRecord) + sizeof(Mem)) {
        return NULL;
    }
    UnpackedRecord* answer = (UnpackedRecord*)pSpace;
    answer->aMem = (Mem*)(answer + 1);
    answer->pKeyInfo = pKeyInfo;
    answer->flags = 0;
    answer->nField = 1;
    answer->aMem->flags = 0;
    answer->aMem->n = nKey;    
    answer->aMem->zMalloc = (char*)pKey;
    return answer;
}

void sqlite3VdbeDeleteUnpackedRecord(UnpackedRecord* p) {
    if (p->aMem->flags & MEM_Dyn) {
        sqlite3_free(p->aMem->zMalloc);
    }
    if (p->flags & UNPACKED_NEED_DESTROY) {
        sqlite3_free(p->aMem);
    }
    if (p->flags & UNPACKED_NEED_FREE) {
        sqlite3_free(p);
    }
}

int sqlite3VdbeRecordCompare(
   int nKey1, const void *pKey1, /* Left key */
   UnpackedRecord *pPKey2        /* Right key */
   ){
    return pPKey2->pKeyInfo->aColl[0]->xCmp(pPKey2, nKey1, pKey1, pPKey2->aMem->n, pPKey2->aMem->zMalloc);
}
