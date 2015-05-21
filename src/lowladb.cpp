#include "algorithm"
#include "cstdio"
#include "set"

#include "bson/bson.h"
#include "integration.h"
#include "SqliteCursor.h"
#include "SqliteKey.h"
#include "TeamstudioException.h"
#include "utf16stringbuilder.h"
#include "json/json.h"
#include "lowladb.h"
#include "md5.h"

static const int PULL_BATCH_SIZE = 100;

static utf16string getFullPath(const utf16string &pathName) {
	utf16string dataDirectory(SysGetDataDirectory());
	utf16string filePath = dataDirectory + "/" + SysNormalizePath(pathName);
    if (filePath.endsWith(".nsf")) {
        filePath = filePath.substring(0, filePath.length() - 4) + ".unp";
    }
	return filePath;
}

static int createDatabase(const utf16string &filePath) {
    sqlite3 *pDb;
	int rc = sqlite3_open(filePath.c_str(), &pDb);
	if (SQLITE_OK == rc && NULL != pDb) {
		Btree *pBt = pDb->aDb[0].pBt;
		if (NULL != pBt) {
            sqlite3_mutex_enter(pDb->mutex);
			rc = sqlite3BtreeBeginTrans(pBt, 1);
			if (SQLITE_OK == rc) {
				sqlite3BtreeCommit(pBt);
			}
            sqlite3_mutex_leave(pDb->mutex);
		}
		sqlite3_close(pDb);
        SysConfigureFile(filePath);
	}
    return rc;
}

class Tx;
class CLowlaDBNsCache {
public:
    CLowlaDBNsCache();
    ~CLowlaDBNsCache();
    CLowlaDBCollectionImpl *collectionForNs(const char *ns);
    void setNotifyOnClose(bool notify);
    
private:
    std::map<utf16string, std::shared_ptr<CLowlaDBImpl>> m_dbCache;
    std::vector<std::shared_ptr<Tx>> m_txCache;
    std::map<utf16string, std::shared_ptr<CLowlaDBCollectionImpl>> m_collCache;
    typedef std::map<utf16string, std::shared_ptr<CLowlaDBImpl>>::iterator db_iterator;
    typedef std::map<utf16string, std::shared_ptr<CLowlaDBCollectionImpl>>::iterator coll_iterator;
    
    bool m_notifyOnClose;
};

class CLowlaDBSyncDocumentLocation {
public:
    SqliteCursor::ptr m_cursor;
    SqliteCursor::ptr m_logCursor;
    std::unique_ptr<CLowlaDBBsonImpl> m_found;
    std::unique_ptr<CLowlaDBBsonImpl> m_foundMeta;
    bool m_logFound;
    int64_t m_sqliteId;
};

class CLowlaDBPullDataImpl {
public:
    typedef std::vector<std::unique_ptr<CLowlaDBBsonImpl>>::iterator atomIterator;
    
    CLowlaDBPullDataImpl();
    void appendAtom(const char *atomBson);
    void setSequence(int sequence);
    atomIterator atomsBegin();
    atomIterator atomsEnd();
    atomIterator eraseAtom(atomIterator walk);
    void eraseAtom(const char *id);
    void setRequestMore(const char *requestMore);
    bool hasRequestMore();
    utf16string getRequestMore();

    bool isComplete();
    int getSequenceForNextRequest();
    
private:
    std::vector<std::unique_ptr<CLowlaDBBsonImpl>> m_atoms;
    int m_sequence;
    int m_processedSequence;
    utf16string m_requestMore;
};

class CLowlaDBPushDataImpl {
public:
    bool isComplete();
    CLowlaDBBson::ptr request();
    void registerIds(const utf16string &ns, const std::vector<int64_t> &ids);
    std::unique_ptr<CLowlaDBSyncDocumentLocation> canAcceptPushResponse(CLowlaDBCollectionImpl *coll, const char *ns, const char *id);
    
private:
    std::map<utf16string, std::vector<int64_t>> m_ids;
    
    class CPushedRecord {
    public:
        CPushedRecord(const utf16string &ns, const char *id, int64_t sqliteId, const utf16string &md5);
        
        utf16string m_ns;
        utf16string m_id;
        int64_t m_sqliteId;
        utf16string m_md5;
    };
    
    std::vector<CPushedRecord> m_pending;
};

class CLowlaDBImpl : public std::enable_shared_from_this<CLowlaDBImpl> {
public:
    typedef std::shared_ptr<CLowlaDBImpl> ptr;
    
    CLowlaDBImpl(const utf16string &name, sqlite3 *pDb);
    ~CLowlaDBImpl();
    
    utf16string name();
    
    std::shared_ptr<CLowlaDBCollectionImpl> createCollection(const utf16string &name);
    void collectionNames(std::vector<utf16string> *plstNames);
    
    SqliteCursor::ptr openCursor(int root);
    Btree *btree();
    
private:
    utf16string m_name;
    sqlite3 *m_pDb;
};

class CLowlaDBWriteResultImpl {
public:
    CLowlaDBWriteResultImpl();
    int getDocumentCount();
    const char *getDocument(int n);
    
    void appendDocument(std::unique_ptr<CLowlaDBBsonImpl> doc);
    void setDocumentCount(int count);
    
private:
    int m_count;
    std::vector<std::unique_ptr<CLowlaDBBsonImpl>> m_docs;
};

class CLowlaDBBsonImpl : public bson {
public:
    typedef enum {
        OWN, // Takes ownership of the bson data and will free it when done
        REF, // Holds a reference to the data but the caller is responsible for freeing it later
        COPY // Makes a copy of the bson data, leaving the caller to free the original any time
    } Mode;
    
    CLowlaDBBsonImpl();
    CLowlaDBBsonImpl(const char *data, Mode ownsData);
    ~CLowlaDBBsonImpl();
    
    static std::shared_ptr<CLowlaDBBsonImpl> empty();
    
    bool containsKey(const char *key);
    void appendDouble(const char *ey, double value);
    void appendString(const char *key, const char *value);
    void appendObject(const char *key, const char *value);
    void appendOid(const char *key, const bson_oid_t *oid);
    void appendBool(const char *key, bool value);
    void appendDate(const char *key, int64_t value);
    void appendInt(const char *key, int value);
    void appendLong(const char *key, int64_t value);
    void appendAll(CLowlaDBBsonImpl *bson);
    void appendElement(const CLowlaDBBsonImpl *src, const char *key);
    void appendElement(const char *newKey, const CLowlaDBBsonImpl *src, const char *key);
    
    void startArray(const char *key);
    void finishArray();
    void startObject(const char *key);
    void finishObject();
    
    bool doubleForKey(const char *key, double *ret);
    bool stringForKey(const char *key, const char **ret) const;
    bool objectForKey(const char *key, const char **ret);
    bool arrayForKey(const char *key, const char **ret);
    bool oidForKey(const char *key, bson_oid_t *ret);
    bool boolForKey(const char *key, bool *ret) const;
    bool dateForKey(const char *key, bson_date_t *ret);
    bool intForKey(const char *key, int *ret);
    bool longForKey(const char *key, int64_t *ret);
    
    bool equalValues(const char *key, const CLowlaDBBsonImpl *other, const char *otherKey);
    bool isEmpty();
    
    const char *data();
    size_t size();
    
    void finish();
};

class CLowlaDBCollectionImpl : public std::enable_shared_from_this<CLowlaDBCollectionImpl> {
public:
    typedef std::shared_ptr<CLowlaDBCollectionImpl> ptr;
    
    CLowlaDBCollectionImpl(CLowlaDBImpl::ptr db, const utf16string &name, int root, int logRoot, int lowlaIndexRoot);
    std::unique_ptr<CLowlaDBWriteResultImpl> insert(CLowlaDBBsonImpl *obj, const char *lowlaId);
    std::unique_ptr<CLowlaDBWriteResultImpl> insert(std::vector<CLowlaDBBsonImpl> &arr);
    std::unique_ptr<CLowlaDBWriteResultImpl> remove(CLowlaDBBsonImpl *query);
    std::unique_ptr<CLowlaDBWriteResultImpl> save(CLowlaDBBsonImpl *obj);
    std::unique_ptr<CLowlaDBWriteResultImpl> update(CLowlaDBBsonImpl *query, CLowlaDBBsonImpl *object, bool upsert, bool multi);
    
    SqliteCursor::ptr openCursor();
    SqliteCursor::ptr openLogCursor();
    CLowlaDBImpl::ptr db();
    bool shouldPullDocument(const CLowlaDBBsonImpl *atom);
    std::unique_ptr<CLowlaDBSyncDocumentLocation> locateDocumentForId(const char *id);
    std::unique_ptr<CLowlaDBSyncDocumentLocation> locateDocumentForSqliteId(int64_t id);
    utf16string name();
    utf16string ns();
    
    void setWriteLog(bool writeLog);
    void updateDocument(SqliteCursor *cursor, int64_t id, CLowlaDBBsonImpl *obj, CLowlaDBBsonImpl *oldObj, CLowlaDBBsonImpl *oldMeta);
    
    void notifyListeners();

private:
    bool isReplaceObject(CLowlaDBBsonImpl *update);
    std::unique_ptr<CLowlaDBBsonImpl> applyUpdate(CLowlaDBBsonImpl *update, CLowlaDBBsonImpl *original);

    void registerLowlaId(const char *lowlaId, i64 id);
    i64 locateLowlaId(const char *lowlaId);
    void forgetLowlaId(const char *lowlaId);
    
    CLowlaDBImpl::ptr m_db;
    int m_root;
    int m_logRoot;
    int m_lowlaIndexRoot;
    utf16string m_name;
    bool m_writeLog;
};

class CLowlaDBCollectionListenerImpl
{
public:
    static CLowlaDBCollectionListenerImpl *instance();
    
    void addListener(LowlaDbCollectionListener listener, void *user);
    void removeListener(LowlaDbCollectionListener listener);

    void notifyListeners(const char *ns);
    
private:
    typedef std::pair<LowlaDbCollectionListener, void *> Entry;
    std::vector<Entry> m_listeners;
};

static std::unique_ptr<CLowlaDBImpl> lowla_db_open(const utf16string &name);

class Tx
{
public:
    Tx(Btree *pBt, bool readOnly);
    ~Tx();
    
    void commit();
    void detach();
    
    bool isOwnTx();
    int  rc();
    
private:
    static const int TRANS_READONLY = 0;
    static const int TRANS_READWRITE = 1;
    
    Btree *m_pBt;
    bool m_ownTx;
    int m_rc;
};

class CLowlaDBCursorImpl {
public:
    CLowlaDBCursorImpl(CLowlaDBCollectionImpl::ptr coll, std::shared_ptr<CLowlaDBBsonImpl> query, std::shared_ptr<CLowlaDBBsonImpl> keys);
    CLowlaDBCursorImpl(const CLowlaDBCursorImpl &other);
    
    std::unique_ptr<CLowlaDBCursorImpl> limit(int limit);
    std::unique_ptr<CLowlaDBCursorImpl> skip(int skip);
    std::unique_ptr<CLowlaDBCursorImpl> sort(std::shared_ptr<CLowlaDBBsonImpl> sort);
    std::unique_ptr<CLowlaDBCursorImpl> showPending();
    
    std::unique_ptr<CLowlaDBCursorImpl> showDiskLoc();
    std::unique_ptr<CLowlaDBBsonImpl> next();
    
    SqliteCursor::ptr sqliteCursor();
    int64_t currentId();
    std::unique_ptr<CLowlaDBBsonImpl> currentMeta();
    int64_t count();
    
private:
    bool matches(CLowlaDBBsonImpl *found);
    std::unique_ptr<CLowlaDBBsonImpl> project(CLowlaDBBsonImpl *found, int64_t id);

    std::unique_ptr<CLowlaDBBsonImpl> nextSorted();
    std::unique_ptr<CLowlaDBBsonImpl> nextUnsorted();
    void performSortedQuery();
    void parseSortSpec();
    std::shared_ptr<CLowlaDBBsonImpl> createSortKey(CLowlaDBBsonImpl *found);
    
    // The cursor has to come after the tx so that it is destructed (closed) before we end the tx
    CLowlaDBCollectionImpl::ptr m_coll;
    std::unique_ptr<Tx> m_tx;
    SqliteCursor::ptr m_cursor;
    SqliteCursor::ptr m_logCursor;
    
    std::shared_ptr<CLowlaDBBsonImpl> m_query;
    std::shared_ptr<CLowlaDBBsonImpl> m_keys;
    std::shared_ptr<CLowlaDBBsonImpl> m_sort;
    std::vector<std::pair<std::vector<utf16string>, int>> m_parsedSort;
    
    int m_unsortedOffset;
    std::vector<int64_t> m_sortedIds;
    std::vector<int64_t>::iterator m_walkSorted;
    
    int m_limit;
    int m_skip;
    bool m_showPending;
    bool m_showDiskLoc;
};

CLowlaDBNsCache::CLowlaDBNsCache() : m_notifyOnClose(false)
{
}

CLowlaDBNsCache::~CLowlaDBNsCache() {
    if (m_notifyOnClose) {
        for (coll_iterator it = m_collCache.begin() ; it != m_collCache.end() ; ++it) {
            it->second->notifyListeners();
        }
    }
    for (std::shared_ptr<Tx> const &tx : m_txCache) {
        tx->commit();
    }
}

void CLowlaDBNsCache::setNotifyOnClose(bool notify) {
    m_notifyOnClose = notify;
}

CLowlaDBCollectionImpl *CLowlaDBNsCache::collectionForNs(const char *ns) {
    utf16string strNs(ns);
    int dotPos = strNs.indexOf('.');
    if (-1 == dotPos || dotPos == strNs.length() - 1) {
        return nullptr;
    }
    coll_iterator it = m_collCache.find(strNs);
    if (it != m_collCache.end()) {
        return it->second.get();
    }
    utf16string strDb = strNs.substring(0, strNs.indexOf('.'));
    utf16string coll = strNs.substring(dotPos + 1);
    db_iterator dbIt = m_dbCache.find(strDb);
    if (dbIt == m_dbCache.end()) {
        m_dbCache[strDb] = lowla_db_open(strDb);
        dbIt = m_dbCache.find(strDb);
        m_txCache.push_back(std::shared_ptr<Tx>(new Tx(dbIt->second->btree(), false)));
    }
    CLowlaDBImpl *db = dbIt->second.get();
    m_collCache[strNs] = db->createCollection(coll);
    it = m_collCache.find(strNs);
    return it->second.get();
}

static int beginTransWithRetry(Btree *pBt, int wrFlag)
{
    int rc = sqlite3BtreeBeginTrans(pBt, wrFlag);
    int count = 0;
    while (SQLITE_BUSY == rc) {
        if (0 == (count % 10)) {
            SysLogMessage(0, "beginTransWithRetry", "sleeping after rc=" + utf16string::valueOf(rc));
        }
        ++count;
        SysSleepMillis(100);
        rc = sqlite3BtreeBeginTrans(pBt, wrFlag);
    }
    return rc;
}

Tx::Tx(Btree *pBt, bool readOnly = false) : m_pBt(pBt)
{
    sqlite3_mutex_enter(pBt->db->mutex);
    m_rc = SQLITE_OK;
    if (readOnly) {
        if (sqlite3BtreeIsInReadTrans(m_pBt)) {
            m_ownTx = false;
        }
        else {
            m_rc = beginTransWithRetry(m_pBt, TRANS_READONLY);
            if (SQLITE_OK == m_rc) {
                m_ownTx = true;
            }
        }
    }
    else if (sqlite3BtreeIsInTrans(m_pBt)) {
        m_ownTx = false;
    }
    else {
        m_rc = beginTransWithRetry(m_pBt, TRANS_READWRITE);
        if (SQLITE_OK == m_rc) {
            m_ownTx = true;
        }
    }
}

Tx::~Tx()
{
    if (SQLITE_OK == m_rc && m_ownTx) {
        sqlite3BtreeRollback(m_pBt);
    }
    sqlite3_mutex_leave(m_pBt->db->mutex);
}

void Tx::commit()
{
    if (m_ownTx) {
        sqlite3BtreeCommit(m_pBt);
        m_ownTx = false;
    }
}

void Tx::detach()
{
    if (m_ownTx) {
        m_ownTx = false;
    }
}

bool Tx::isOwnTx()
{
    return m_ownTx;
}

int Tx::rc() {
    return m_rc;
}

CLowlaDB::ptr CLowlaDB::create(std::shared_ptr<CLowlaDBImpl> pimpl) {
    return CLowlaDB::ptr(new CLowlaDB(pimpl));
}

CLowlaDB::CLowlaDB(std::shared_ptr<CLowlaDBImpl> pimpl) : m_pimpl(pimpl) {
}

class LowlaIdKey : public SqliteKey {
public:
    static KeyInfo *getKeyInfo();
    static int compare(void *pUser, int n1, const void *key1, int n2, const void *key2);
    
    LowlaIdKey(const char *lowlaId, i64 recordId);

    int getSize() const;
    int writeToPointer(unsigned char *pc);
    UnpackedRecord *newUnpackedRecord();
    
private:
    const char *m_pszKey;
    int m_cb;
};

LowlaIdKey::LowlaIdKey(const char *lowlaId, i64 recordId) : SqliteKey(recordId) {
    m_pszKey = lowlaId;
    m_cb = (int)strlen(m_pszKey);
}

KeyInfo *LowlaIdKey::getKeyInfo() {
    static KeyInfo keyInfo;
    static CollSeq collSec;
    keyInfo.nField = 1;
    keyInfo.aColl[0] = &collSec;
    collSec.xCmp = LowlaIdKey::compare;
    return &keyInfo;
}

int LowlaIdKey::compare(void *pUser, int n1, const void *key1, int n2, const void *key2) {
    UnpackedRecord *record = (UnpackedRecord*)pUser;
    int answer = n1 - n2;
    if (0 == answer) {
        answer = strcmp((const char *)key1, (const char *)key2);
    }
    if (0 == answer) {
        size_t cb = strlen((const char *)key1);
        sqlite3GetVarint((const unsigned char *)key1 + cb + 1, (u64*)&record->rowid);
    }
    return answer;
}

int LowlaIdKey::getSize() const {
    return m_cb + 1 + getIdSize();
}

int LowlaIdKey::writeToPointer(unsigned char *pc) {
    memcpy(pc, m_pszKey, m_cb);
    pc[m_cb] = '\0';
    sqlite3PutVarint(pc + m_cb + 1, getId());
    return getSize();
}

UnpackedRecord* LowlaIdKey::newUnpackedRecord() {
    UnpackedRecord* answer = (UnpackedRecord*)sqlite3_malloc(sizeof(UnpackedRecord) + sizeof(Mem) + getSize());
    answer->flags = UNPACKED_NEED_FREE;
    answer->nField = 1;
    answer->pKeyInfo = LowlaIdKey::getKeyInfo();
    answer->aMem = (Mem*)(answer + 1);
    answer->aMem->flags = 0;
    answer->aMem->n = getSize();
    answer->aMem->zMalloc = (char *)(answer->aMem + 1);
    writeToPointer((unsigned char*)answer->aMem->zMalloc);
    return answer;
}

CLowlaDBImpl::CLowlaDBImpl(const utf16string &name, sqlite3 *pDb) : m_name(name), m_pDb(pDb) {
}

CLowlaDBImpl::~CLowlaDBImpl() {
    sqlite3_close(m_pDb);
}

utf16string CLowlaDBImpl::name() {
    return m_name;
}

static std::unique_ptr<CLowlaDBImpl> lowla_db_open(const utf16string &name) {
    static sqlite3_mutex *mutex = sqlite3_mutex_alloc(SQLITE_MUTEX_RECURSIVE);
    sqlite3_mutex_enter(mutex);
    
    utf16string filePath = getFullPath(name);
    sqlite3 *pDb;
    int rc = sqlite3_open_v2(filePath.c_str(), &pDb, SQLITE_OPEN_READWRITE, 0);
    if (SQLITE_OK == rc) {
        // We have opened the file, but it may not be a database. Starting a transaction is the best check.
        Tx tx(pDb->aDb[0].pBt);
        std::unique_ptr<CLowlaDBImpl> pimpl;
        if (SQLITE_OK == tx.rc()) {
            pimpl.reset(new CLowlaDBImpl(name, pDb));
            tx.commit();
        }
        sqlite3_mutex_leave(mutex);
        return pimpl;
    }
    rc = createDatabase(filePath);
    if (SQLITE_OK == rc) {
        rc = sqlite3_open_v2(filePath.c_str(), &pDb, SQLITE_OPEN_READWRITE, 0);
    }
    if (SQLITE_OK == rc) {
        std::unique_ptr<CLowlaDBImpl> pimpl(new CLowlaDBImpl(name, pDb));
        sqlite3_mutex_leave(mutex);
        return pimpl;
    }
    sqlite3_mutex_leave(mutex);
    return std::unique_ptr<CLowlaDBImpl>();
}

CLowlaDB::ptr CLowlaDB::open(const utf16string &name) {
    std::shared_ptr<CLowlaDBImpl> pimpl = lowla_db_open(name);
    if (pimpl) {
        return CLowlaDB::create(pimpl);
    }
    return CLowlaDB::ptr();
}

CLowlaDBCollection::ptr CLowlaDB::createCollection(const utf16string &name) {
    std::shared_ptr<CLowlaDBCollectionImpl> pimpl = m_pimpl->createCollection(name);
    return CLowlaDBCollection::create(pimpl);
}

void CLowlaDB::collectionNames(std::vector<utf16string> *plstNames) {
    m_pimpl->collectionNames(plstNames);
}

std::shared_ptr<CLowlaDBCollectionImpl> CLowlaDBImpl::createCollection(const utf16string &name) {
    SqliteCursor headerCursor;
    Btree *pBt = m_pDb->aDb[0].pBt;
    
    Tx tx(pBt);
    
    const char *collName = name.c_str(utf16string::UTF8);
    
    int rc = headerCursor.create(pBt, 1, CURSOR_READWRITE, NULL);
    if (SQLITE_OK != rc) {
        return nullptr;
    }
    int res;
    rc = headerCursor.first(&res);
    if (SQLITE_OK != rc) {
        return nullptr;
    }
    if (!res) {
        while (!headerCursor.isEof()) {
            int wdc;
            const void *rawData = headerCursor.dataFetch(&wdc);
            bson data[1];
            bson_init_finished_data(data, (char *)rawData, false);
            bson_iterator it[1];
            bson_type type = bson_find(it, data, "collName");
            const char *foundName = "";
            if (BSON_STRING == type) {
                foundName = bson_iterator_string(it);
            }
            int collRoot = -1;
            type = bson_find(it, data, "collRoot");
            if (BSON_INT == type) {
                collRoot = bson_iterator_int(it);
            }
            int collLogRoot = -1;
            type = bson_find(it, data, "collLogRoot");
            if (BSON_INT == type) {
                collLogRoot = bson_iterator_int(it);
            }
            int lowlaIndexRoot = -1;
            type = bson_find(it, data, "lowlaIndexRoot");
            if (BSON_INT == type) {
                lowlaIndexRoot = bson_iterator_int(it);
            }
            bson_destroy(data);
            
            if (0 == strcmp(collName, foundName) && -1 != collRoot && -1 != collLogRoot) {
                headerCursor.close();
                return std::make_shared<CLowlaDBCollectionImpl>(shared_from_this(), name, collRoot, collLogRoot, lowlaIndexRoot);
            }
            headerCursor.next(&res);
        }
    }
    int collRoot = 0;
    rc = sqlite3BtreeCreateTable(pBt, &collRoot, BTREE_INTKEY | BTREE_LEAFDATA);
    if (SQLITE_OK != rc) {
        return nullptr;
    }
    int collLogRoot = 0;
    rc = sqlite3BtreeCreateTable(pBt, &collLogRoot, BTREE_INTKEY | BTREE_LEAFDATA);
    if (SQLITE_OK != rc) {
        return nullptr;
    }
    int lowlaIndexRoot = 0;
    rc = sqlite3BtreeCreateTable(pBt, &lowlaIndexRoot, BTREE_ZERODATA);
    if (SQLITE_OK != rc) {
        return nullptr;
    }
    
    bson data[1];
    bson_init(data);
    
    bson_append_string(data, "collName", collName);
    bson_append_int(data, "collRoot", collRoot);
    bson_append_int(data, "collLogRoot", collLogRoot);
    bson_append_int(data, "lowlaIndexRoot", lowlaIndexRoot);
    bson_finish(data);
    
    rc = headerCursor.last(&res);
    i64 lastInternalId = 0;
    if (SQLITE_OK == rc && 0 == res) {
        rc = headerCursor.keySize(&lastInternalId);
    }
    i64 newId = lastInternalId + 1;
    
    headerCursor.insert(NULL, newId, bson_data(data), bson_size(data), 0, true, 0);
    headerCursor.close();
    bson_destroy(data);

    tx.commit();
    return std::make_shared<CLowlaDBCollectionImpl>(shared_from_this(), name, collRoot, collLogRoot, lowlaIndexRoot);
}

void CLowlaDBImpl::collectionNames(std::vector<utf16string> *plstNames) {
    SqliteCursor headerCursor;
    Btree *pBt = m_pDb->aDb[0].pBt;
    
    Tx tx(pBt);
    
    int rc = headerCursor.create(pBt, 1, CURSOR_READONLY, NULL);
    if (SQLITE_OK != rc) {
        return;
    }
    int res;
    rc = headerCursor.first(&res);
    while (SQLITE_OK == rc && 0 == res) {
        int wdc;
        const void *rawData = headerCursor.dataFetch(&wdc);
        bson data[1];
        bson_init_finished_data(data, (char *)rawData, false);
        bson_iterator it[1];
        bson_type type = bson_find(it, data, "collName");
        const char *foundName = "";
        if (BSON_STRING == type) {
            foundName = bson_iterator_string(it);
            plstNames->push_back(foundName);
        }
        bson_destroy(data);
        
        rc = headerCursor.next(&res);
    }
    headerCursor.close();
}

Btree *CLowlaDBImpl::btree() {
    return m_pDb->aDb[0].pBt;
}

SqliteCursor::ptr CLowlaDBImpl::openCursor(int root) {
    SqliteCursor::ptr answer(new SqliteCursor);
    answer->create(m_pDb->aDb[0].pBt, root, CURSOR_READWRITE, NULL);
    return answer;
}

CLowlaDBCollection::ptr CLowlaDBCollection::create(std::shared_ptr<CLowlaDBCollectionImpl> pimpl) {
    return CLowlaDBCollection::ptr(new CLowlaDBCollection(pimpl));
}

std::shared_ptr<CLowlaDBCollectionImpl> CLowlaDBCollection::pimpl() {
    return m_pimpl;
}

CLowlaDBCollection::CLowlaDBCollection(std::shared_ptr<CLowlaDBCollectionImpl> pimpl) : m_pimpl(pimpl) {
}

CLowlaDBWriteResult::ptr CLowlaDBCollection::insert(const char *bsonData) {
    return insert(bsonData, nullptr);
}

CLowlaDBWriteResult::ptr CLowlaDBCollection::insert(const char *bsonData, const char *lowlaId) {
    CLowlaDBBsonImpl bson(bsonData, CLowlaDBBsonImpl::REF);
    std::shared_ptr<CLowlaDBWriteResultImpl> pimpl = m_pimpl->insert(&bson, lowlaId);
    return CLowlaDBWriteResult::create(pimpl);
}

CLowlaDBWriteResult::ptr CLowlaDBCollection::insert(const std::vector<const char *> &bsonData) {
    std::vector<CLowlaDBBsonImpl> bsonArr;
    for (const char *bson : bsonData) {
        bsonArr.emplace_back(bson, CLowlaDBBsonImpl::REF);
    }
    std::shared_ptr<CLowlaDBWriteResultImpl> pimpl = m_pimpl->insert(bsonArr);
    return CLowlaDBWriteResult::create(pimpl);
}

CLowlaDBWriteResult::ptr CLowlaDBCollection::remove(const char *queryBson) {
    if (queryBson) {
        CLowlaDBBsonImpl query(queryBson, CLowlaDBBsonImpl::REF);
        std::shared_ptr<CLowlaDBWriteResultImpl> pimpl = m_pimpl->remove(&query);
        return CLowlaDBWriteResult::create(pimpl);
    }
    else {
        std::shared_ptr<CLowlaDBWriteResultImpl> pimpl = m_pimpl->remove(nullptr);
        return CLowlaDBWriteResult::create(pimpl);
    }
}

CLowlaDBWriteResult::ptr CLowlaDBCollection::save(const char *bsonData) {
    CLowlaDBBsonImpl bson(bsonData, CLowlaDBBsonImpl::REF);
    std::shared_ptr<CLowlaDBWriteResultImpl> pimpl = m_pimpl->save(&bson);
    return CLowlaDBWriteResult::create(pimpl);
}

CLowlaDBWriteResult::ptr CLowlaDBCollection::update(const char *queryBson, const char *objectBson, bool upsert, bool multi) {
    CLowlaDBBsonImpl query(queryBson, CLowlaDBBsonImpl::REF);
    CLowlaDBBsonImpl object(objectBson, CLowlaDBBsonImpl::REF);
    std::shared_ptr<CLowlaDBWriteResultImpl> pimpl = m_pimpl->update(&query, &object, upsert, multi);
    return CLowlaDBWriteResult::create(pimpl);
}

static utf16string generateLowlaId(CLowlaDBCollectionImpl *coll, CLowlaDBBsonImpl *obj) {
    const char *id;
    bson_oid_t oid;
    if (obj->stringForKey("_id", &id)) {
        return coll->ns() + "$" + id;
    }
    else if (obj->oidForKey("_id", &oid)) {
        char buf[CLowlaDBBson::OID_STRING_SIZE];
        CLowlaDBBson::oidToString((const char *)&oid, buf);
        return coll->ns() + "$ObjectID(" + buf + ")";
    }
    else {
        return "";
        assert(false);
    }
}

CLowlaDBCollectionImpl::CLowlaDBCollectionImpl(CLowlaDBImpl::ptr db, const utf16string &name, int root, int logRoot, int lowlaIndexRoot) : m_db(db), m_name(name), m_root(root), m_logRoot(logRoot), m_lowlaIndexRoot(lowlaIndexRoot), m_writeLog(true) {
}

static void throwIfDocumentInvalidForInsertion(bson const *obj) {
    bson_iterator it[1];
    bson_iterator_init(it, obj);
    while (BSON_EOO != bson_iterator_next(it)) {
        if ('$' == bson_iterator_key(it)[0]) {
            utf16string msg("The dollar ($) prefixed field ");
            msg += bson_iterator_key(it);
            msg += " is not valid";
            throw TeamstudioException(msg);
        }
    }
}

static void throwIfDocumentInvalidForUpdate(bson const *obj) {
    bson_iterator it[1];
    bson_iterator_init(it, obj);
    bool foundOp = false;
    bool foundReplace = false;
    
    while (BSON_EOO != bson_iterator_next(it)) {
        const char *key = bson_iterator_key(it);
        if ('$' == key[0]) {
            if (foundReplace) {
                throw TeamstudioException("Can not mix operations and values in object updates");
            }
            foundOp = true;
            
            if (0 == strcmp("$set", key)) {
                bson sub[1];
                bson_iterator_subobject_init(it, sub, false);
                throwIfDocumentInvalidForInsertion(sub);
            }
            else if (0 == strcmp("$unset", key)) {
            }
            else {
                utf16string msg("The dollar ($) prefixed field ");
                msg += key;
                msg += " is not valid";
                throw TeamstudioException(msg);
            }
        }
        else {
            if (foundOp) {
                throw TeamstudioException("Can not mix operations and values in object updates");
            }
            foundReplace = true;
        }
    }
}
std::unique_ptr<CLowlaDBWriteResultImpl> CLowlaDBCollectionImpl::insert(CLowlaDBBsonImpl *obj, const char *lowlaId) {
    
    throwIfDocumentInvalidForInsertion(obj);
    if (!obj->containsKey("_id")) {
        CLowlaDBBsonImpl fixed[1];
        bson_oid_t newOid;
        bson_oid_gen(&newOid);
        fixed->appendOid("_id", &newOid);
        fixed->appendAll(obj);
        fixed->finish();
        std::unique_ptr<CLowlaDBWriteResultImpl> answer = insert(fixed, lowlaId);
        return answer;
    }
    
    std::unique_ptr<CLowlaDBWriteResultImpl> answer(new CLowlaDBWriteResultImpl);
    
    Tx tx(m_db->btree());
    
    SqliteCursor::ptr cursor = m_db->openCursor(m_root);
    int res = 0;
    int rc = cursor->last(&res);
    i64 lastInternalId = 0;
    if (SQLITE_OK == rc && 0 == res) {
        rc = cursor->keySize(&lastInternalId);
    }
    i64 newId = lastInternalId + 1;
    CLowlaDBBsonImpl meta;
    if (nullptr != lowlaId) {
        meta.appendString("id", lowlaId);
    }
    else {
        meta.appendString("id", generateLowlaId(this, obj).c_str());
    }
    meta.finish();
    meta.stringForKey("id", &lowlaId);
    rc = cursor->insert(NULL, newId, obj->data(), (int)obj->size(), (int)meta.size(), true, 0);
    if (SQLITE_OK == rc) {
        rc = cursor->movetoUnpacked(nullptr, newId, 0, &res);
        cursor->putData((int)obj->size(), (int)meta.size(), meta.data());
        if (m_writeLog) {
            SqliteCursor::ptr logCursor = m_db->openCursor(m_logRoot);
            static char logData[] = {5, 0, 0, 0, 0};
            rc = logCursor->insert(NULL, newId, logData, sizeof(logData), (int)meta.size(), true, 0);
            logCursor->movetoUnpacked(nullptr, newId, 0, &res);
            logCursor->putData(sizeof(logData), (int)meta.size(), meta.data());
        }
        registerLowlaId(lowlaId, newId);
    }
    if (obj->ownsData) {
        obj->ownsData = false;
        answer->appendDocument(std::unique_ptr<CLowlaDBBsonImpl>(new CLowlaDBBsonImpl(obj->data(), CLowlaDBBsonImpl::OWN)));
    }
    else {
        answer->appendDocument(std::unique_ptr<CLowlaDBBsonImpl>(new CLowlaDBBsonImpl(obj->data(), CLowlaDBBsonImpl::REF)));
    }
    
    rc = cursor->close();
    
    // We only notify listeners if we're a self-contained transaction. If we're part of a larger
    // transaction then its owner should handle the notification so we don't spam clients.
    if (tx.isOwnTx()) {
        notifyListeners();
    }
    
    tx.commit();
    
    return answer;
}

std::unique_ptr<CLowlaDBWriteResultImpl> CLowlaDBCollectionImpl::insert(std::vector<CLowlaDBBsonImpl> &arr) {

    // To match JS client behavior, we check first and throw if any document has invalid field names
    for (CLowlaDBBsonImpl const &doc : arr) {
        throwIfDocumentInvalidForInsertion(&doc);
    }
    std::unique_ptr<CLowlaDBWriteResultImpl> answer(new CLowlaDBWriteResultImpl);
    
    Tx tx(m_db->btree());
    SqliteCursor::ptr cursor = m_db->openCursor(m_root);
    SqliteCursor::ptr logCursor = m_db->openCursor(m_logRoot);

    for (int i = 0 ; i < arr.size() ; ++i) {
        CLowlaDBBsonImpl *obj = &arr[i];
        CLowlaDBBsonImpl fixed;
        if (!obj->containsKey("_id")) {
            bson_oid_t newOid;
            bson_oid_gen(&newOid);
            fixed.appendOid("_id", &newOid);
            fixed.appendAll(obj);
            fixed.finish();
            obj = &fixed;
        }
    
        int res = 0;
        int rc = cursor->last(&res);
        i64 lastInternalId = 0;
        if (SQLITE_OK == rc && 0 == res) {
            rc = cursor->keySize(&lastInternalId);
        }
        i64 newId = lastInternalId + 1;
        CLowlaDBBsonImpl meta;
        meta.appendString("id", generateLowlaId(this, obj).c_str());
        meta.finish();
        const char *lowlaId;
        meta.stringForKey("id", &lowlaId);
        rc = cursor->insert(NULL, newId, obj->data(), (int)obj->size(), (int)meta.size(), true, 0);
        if (SQLITE_OK == rc) {
            rc = cursor->movetoUnpacked(nullptr, newId, 0, &res);
            cursor->putData((int)obj->size(), (int)meta.size(), meta.data());
            static char logData[] = {5, 0, 0, 0, 0};
            if (m_writeLog) {
                rc = logCursor->insert(NULL, newId, logData, sizeof(logData), (int)meta.size(), true, 0);
                logCursor->movetoUnpacked(nullptr, newId, 0, &res);
                logCursor->putData(sizeof(logData), (int)meta.size(), meta.data());
            }
            registerLowlaId(lowlaId, newId);
        }
        if (obj->ownsData) {
            obj->ownsData = false;
            answer->appendDocument(std::unique_ptr<CLowlaDBBsonImpl>(new CLowlaDBBsonImpl(obj->data(), CLowlaDBBsonImpl::OWN)));
        }
        else {
            answer->appendDocument(std::unique_ptr<CLowlaDBBsonImpl>(new CLowlaDBBsonImpl(obj->data(), CLowlaDBBsonImpl::REF)));
        }
    }
    
    logCursor->close();
    cursor->close();
    
    notifyListeners();
    
    tx.commit();
    
    return answer;
}

std::unique_ptr<CLowlaDBWriteResultImpl> CLowlaDBCollectionImpl::remove(CLowlaDBBsonImpl *query) {
    Tx tx(m_db->btree());
    
    // The cursor needs a shared_ptr so we create a new ClowlaDBBsonImpl using the incoming data
    std::shared_ptr<CLowlaDBBsonImpl> cursorQuery;
    if (query) {
        cursorQuery.reset(new CLowlaDBBsonImpl(query->data(), CLowlaDBBsonImpl::REF));
    }
    
    auto cursor = std::make_shared<CLowlaDBCursorImpl>(shared_from_this(), cursorQuery, nullptr);

    std::vector<int64_t> idsToDelete;
    
    // We can't delete the documents while we iterate the cursor, so we create the log documents in
    // a first pass and then go back and delete the documents.
    std::unique_ptr<CLowlaDBBsonImpl> found = cursor->next();
    while (found) {
        int64_t id = cursor->currentId();
        updateDocument(cursor->sqliteCursor().get(), id, nullptr, found.get(), cursor->currentMeta().get());
        idsToDelete.push_back(id);
        found = cursor->next();
    }

    // Now go through and delete the documents.
    for (int64_t id : idsToDelete) {
        int rc, res;
        rc = cursor->sqliteCursor()->movetoUnpacked(nullptr, id, 0, &res);
        if (SQLITE_OK == rc && 0 == res) {
            const char *lowlaId;
            cursor->currentMeta()->stringForKey("id", &lowlaId);
            cursor->sqliteCursor()->deleteCurrent();
            forgetLowlaId(lowlaId);
        }
    }

    cursor.reset();
    notifyListeners();
    tx.commit();
    std::unique_ptr<CLowlaDBWriteResultImpl> wr(new CLowlaDBWriteResultImpl);
    wr->setDocumentCount((int)idsToDelete.size());
    return wr;
}

std::unique_ptr<CLowlaDBWriteResultImpl> CLowlaDBCollectionImpl::save(CLowlaDBBsonImpl *obj) {
    if (obj->containsKey("_id)")) {
        CLowlaDBBsonImpl query;
        query.appendElement(obj, "_id");
        query.finish();
        return update(&query, obj, true, false);
    }
    else {
        return insert(obj, "");
    }
}

std::unique_ptr<CLowlaDBWriteResultImpl> CLowlaDBCollectionImpl::update(CLowlaDBBsonImpl *query, CLowlaDBBsonImpl *object, bool upsert, bool multi) {

    throwIfDocumentInvalidForUpdate(object);
    
    Tx tx(m_db->btree());

    // The cursor needs a shared_ptr so we create a new ClowlaDBBsonImpl using the incoming data
    std::shared_ptr<CLowlaDBBsonImpl> cursorQuery(new CLowlaDBBsonImpl(query->data(), CLowlaDBBsonImpl::REF));
    auto cursor = std::make_shared<CLowlaDBCursorImpl>(shared_from_this(), cursorQuery, nullptr);
    if (!multi) {
        cursor = cursor->limit(1);
    }
    
    std::unique_ptr<CLowlaDBBsonImpl> found = cursor->next();
    if (!found && upsert) {
        return insert(object, "");
    }
    std::unique_ptr<CLowlaDBWriteResultImpl> wr(new CLowlaDBWriteResultImpl);
    while (found) {
        int64_t id = cursor->currentId();
        std::unique_ptr<CLowlaDBBsonImpl> bsonToWrite = applyUpdate(object, found.get());
        updateDocument(cursor->sqliteCursor().get(), id, bsonToWrite.get(), found.get(), cursor->currentMeta().get());
        wr->appendDocument(std::move(bsonToWrite));
        
        found = cursor->next();
    }
    cursor.reset();
    notifyListeners();
    tx.commit();
    return wr;
}

std::unique_ptr<CLowlaDBBsonImpl> CLowlaDBCollectionImpl::applyUpdate(CLowlaDBBsonImpl *update, CLowlaDBBsonImpl *original)  {
    std::unique_ptr<CLowlaDBBsonImpl> answer(new CLowlaDBBsonImpl());
    if (isReplaceObject(update)) {
        answer->appendElement(original, "_id");
        bson_iterator it[1];
        bson_iterator_init(it, update);
        while (bson_iterator_next(it)) {
            if (0 != strcmp("_id", bson_iterator_key(it))) {
                bson_append_element(answer.get(), nullptr, it);
            }
        }
        answer->finish();
    }
    else {
        const char *tmp;
        std::shared_ptr<CLowlaDBBsonImpl> set;
        std::shared_ptr<CLowlaDBBsonImpl> unset;
        if (update->objectForKey("$set", &tmp)) {
            set.reset(new CLowlaDBBsonImpl(tmp, CLowlaDBBsonImpl::REF));
        }
        else {
            set = CLowlaDBBsonImpl::empty();
        }
        if (update->objectForKey("$unset", &tmp)) {
            unset.reset(new CLowlaDBBsonImpl(tmp, CLowlaDBBsonImpl::REF));
        }
        else {
            unset = CLowlaDBBsonImpl::empty();
        }
        answer->appendElement(original, "_id");
        bson_iterator it[1];
        bson_iterator_init(it, original);
        while (bson_iterator_next(it)) {
            const char *key = bson_iterator_key(it);
            if (0 == strcmp("_id", key)) {
                continue;
            }
            else if (set->containsKey(key)) {
                answer->appendElement(set.get(), key);
            }
            else if (unset->containsKey(key)) {
                continue;
            }
            else {
                answer->appendElement(original, key);
            }
        }
        bson_iterator_init(it, set.get());
        while (bson_iterator_next(it)) {
            const char *key = bson_iterator_key(it);
            if (!original->containsKey(key)) {
                bson_append_element(answer.get(), nullptr, it);
            }
        }
        answer->finish();
    }
    return answer;
}

void CLowlaDBCollectionImpl::updateDocument(SqliteCursor *cursor, int64_t id, CLowlaDBBsonImpl *obj, CLowlaDBBsonImpl *oldObj, CLowlaDBBsonImpl *oldMeta) {
    int rc = 0, res;
    if (obj) {
        rc = cursor->insert(NULL, id, obj->data(), (int)obj->size(), (int)oldMeta->size(), false, 0);
        if (SQLITE_OK == rc) {
            cursor->movetoUnpacked(nullptr, id, 0, &res);
            rc = cursor->putData((int)obj->size(), (int)oldMeta->size(), oldMeta->data());
        }
    }
    if (SQLITE_OK == rc && m_writeLog) {
        SqliteCursor::ptr logCursor = m_db->openCursor(m_logRoot);
        rc = logCursor->movetoUnpacked(nullptr, id, 0, &res);
        if (SQLITE_OK == rc && 0 != res) {
            logCursor->insert(nullptr, id, oldObj->data(), (int)oldObj->size(), (int)oldMeta->size(), false, res);
            if (SQLITE_OK == rc) {
                logCursor->movetoUnpacked(nullptr, id, 0, &res);
                rc = logCursor->putData((int)oldObj->size(), (int)oldMeta->size(), oldMeta->data());
            }
        }
    }
}

void CLowlaDBCollectionImpl::notifyListeners() {
    utf16string ns = m_db->name() + "." + m_name;
    CLowlaDBCollectionListenerImpl::instance()->notifyListeners(ns.c_str());
}

bool CLowlaDBCollectionImpl::isReplaceObject(CLowlaDBBsonImpl *update) {
    bson_iterator it[1];
    bson_iterator_init(it, update);
    if (bson_iterator_next(it)) {
        const char *key = bson_iterator_key(it);
        return key[0] != '$';
    }
    return true;
}

SqliteCursor::ptr CLowlaDBCollectionImpl::openCursor() {
    return m_db->openCursor(m_root);
}

SqliteCursor::ptr CLowlaDBCollectionImpl::openLogCursor() {
    return m_db->openCursor(m_logRoot);
}

void CLowlaDBCollectionImpl::registerLowlaId(const char *lowlaId, i64 id) {
    Btree *pBt = m_db->btree();
    
    Tx tx(pBt);
    
    SqliteCursor lowlaCursor;
    lowlaCursor.create(pBt, m_lowlaIndexRoot, CURSOR_READWRITE, LowlaIdKey::getKeyInfo());
    
    LowlaIdKey key(lowlaId, id);
    int keySize = key.getSize();
    std::vector<unsigned char> ac(keySize);
    key.writeToPointer(&ac[0]);
    lowlaCursor.insert(&ac[0], keySize, nullptr, 0, 0, false, 0);
    
    lowlaCursor.close();
    
    tx.commit();
}

i64 CLowlaDBCollectionImpl::locateLowlaId(const char *lowlaId) {
    Btree *pBt = m_db->btree();
    
    Tx tx(pBt);
    
    SqliteCursor lowlaCursor;
    lowlaCursor.create(pBt, m_lowlaIndexRoot, CURSOR_READWRITE, LowlaIdKey::getKeyInfo());
    
    LowlaIdKey key(lowlaId, 0);
    UnpackedRecord* precord = key.newUnpackedRecord();
    int rc, res;
    rc = lowlaCursor.movetoUnpacked(precord, 0, 0, &res);
    i64 answer = 0;
    if (0 == res) {
        answer = precord->rowid;
    }
    key.deleteUnpackedRecord(precord);
    lowlaCursor.close();
    
    tx.commit();
    
    return answer;
}

void CLowlaDBCollectionImpl::forgetLowlaId(const char *lowlaId) {
    Btree *pBt = m_db->btree();
    
    Tx tx(pBt);
    
    SqliteCursor lowlaCursor;
    lowlaCursor.create(pBt, m_lowlaIndexRoot, CURSOR_READWRITE, LowlaIdKey::getKeyInfo());
    
    LowlaIdKey key(lowlaId, 0);
    lowlaCursor.deleteKey(&key);
    lowlaCursor.close();
    
    tx.commit();
}

utf16string CLowlaDBCollectionImpl::name() {
    return m_name;
}

utf16string CLowlaDBCollectionImpl::ns() {
    return m_db->name() + "." + m_name;
}

CLowlaDBImpl::ptr CLowlaDBCollectionImpl::db() {
    return m_db;
}

void CLowlaDBCollectionImpl::setWriteLog(bool writeLog) {
    m_writeLog = writeLog;
}

std::unique_ptr<CLowlaDBSyncDocumentLocation> CLowlaDBCollectionImpl::locateDocumentForId(const char *id) {
    return locateDocumentForSqliteId(locateLowlaId(id));
}

std::unique_ptr<CLowlaDBSyncDocumentLocation> CLowlaDBCollectionImpl::locateDocumentForSqliteId(int64_t id) {
    std::unique_ptr<CLowlaDBSyncDocumentLocation> answer(new CLowlaDBSyncDocumentLocation);
    
    answer->m_logFound = false;
    answer->m_sqliteId = id;
    if (0 == answer->m_sqliteId) {
        return answer;
    }
    
    Tx tx(db()->btree());
    
    SqliteCursor::ptr cursor = openCursor();
    answer->m_cursor = cursor;
    int res;
    int rc = cursor->movetoUnpacked(nullptr, answer->m_sqliteId, false, &res);
    if (SQLITE_OK == rc && 0 ==res) {
        u32 dataSize;
        cursor->dataSize(&dataSize);
        int objSize = 0;
        int cbRequired = 4;
        bson_little_endian32(&objSize, cursor->dataFetch(&cbRequired));
        int metaSize = dataSize - objSize;
        char *data = (char *)bson_malloc(objSize);
        cursor->data(0, objSize, data);
        answer->m_found.reset(new CLowlaDBBsonImpl(data, CLowlaDBBsonImpl::OWN));
        char *meta = (char *)bson_malloc(metaSize);
        cursor->data(objSize, metaSize, meta);
        answer->m_foundMeta.reset(new CLowlaDBBsonImpl(meta, CLowlaDBBsonImpl::OWN));
    }
    answer->m_logCursor = m_db->openCursor(m_logRoot);
    int resLog;
    int rcLog = answer->m_logCursor->movetoUnpacked(nullptr, answer->m_sqliteId, 0, &resLog);
    answer->m_logFound = (SQLITE_OK == rcLog && 0 == resLog);
    return answer;
}

bool CLowlaDBCollectionImpl::shouldPullDocument(const CLowlaDBBsonImpl *atom) {
    
    Tx tx(m_db->btree());
    
    const char *id;
    atom->stringForKey("id", &id);
    std::unique_ptr<CLowlaDBSyncDocumentLocation> loc = locateDocumentForId(id);

    // If found
    if (loc->m_found) {
        // See if there's an outgoing record in the log. If so, don't pull
        if (loc->m_logFound) {
            return false;
        }
        // If the found record has the same version then don't want to pull
        if (loc->m_found->equalValues("_version", atom, "version")) {
            return false;
        }
        // Good to pull
        return true;
    }
    // So it wasn't found. There may be an outgoing deletion, but we have no easy way to find it.
    // This is rare, so we can allow the pull to go ahead and then when the push gets processed the
    // adapter will sort out the conflict
    return true;
}

CLowlaDBWriteResult::ptr CLowlaDBWriteResult::create(std::shared_ptr<CLowlaDBWriteResultImpl> pimpl) {
    return CLowlaDBWriteResult::ptr(new CLowlaDBWriteResult(pimpl));
}

int CLowlaDBWriteResult::documentCount() {
    return m_pimpl->getDocumentCount();
}

CLowlaDBBson::ptr CLowlaDBWriteResult::document(int n) {
    return CLowlaDBBson::create(m_pimpl->getDocument(n), false);
}

CLowlaDBWriteResult::CLowlaDBWriteResult(std::shared_ptr<CLowlaDBWriteResultImpl> pimpl) : m_pimpl(pimpl) {
}

CLowlaDBWriteResultImpl::CLowlaDBWriteResultImpl() : m_count(0) {
}

int CLowlaDBWriteResultImpl::getDocumentCount() {
    if (0 == m_count) {
        return (int)m_docs.size();
    }
    return m_count;
}

const char *CLowlaDBWriteResultImpl::getDocument(int n) {
    return m_docs[n]->data();
}

void CLowlaDBWriteResultImpl::appendDocument(std::unique_ptr<CLowlaDBBsonImpl> doc) {
    m_docs.push_back(std::move(doc));
}

void CLowlaDBWriteResultImpl::setDocumentCount(int count) {
    m_count = count;
}

const size_t CLowlaDBBson::OID_SIZE = sizeof(bson_oid_t);
const size_t CLowlaDBBson::OID_STRING_SIZE = 2 * OID_SIZE + 1;

CLowlaDBBson::ptr CLowlaDBBson::create() {
    return CLowlaDBBson::ptr(new CLowlaDBBson(std::make_shared<CLowlaDBBsonImpl>()));
}

CLowlaDBBson::ptr CLowlaDBBson::create(std::shared_ptr<CLowlaDBBsonImpl> pimpl) {
    if (pimpl) {
        return CLowlaDBBson::ptr(new CLowlaDBBson(pimpl));
    }
    else {
        return CLowlaDBBson::ptr();
    }
}

CLowlaDBBson::ptr CLowlaDBBson::create(const char *bson, bool ownsData) {
    CLowlaDBBsonImpl::Mode mode = ownsData ? CLowlaDBBsonImpl::OWN : CLowlaDBBsonImpl::REF;
    return CLowlaDBBson::ptr(new CLowlaDBBson(std::make_shared<CLowlaDBBsonImpl>(bson, mode)));
}

CLowlaDBBson::ptr CLowlaDBBson::empty() {
    return CLowlaDBBson::ptr(new CLowlaDBBson(CLowlaDBBsonImpl::empty()));
}

std::shared_ptr<CLowlaDBBsonImpl> CLowlaDBBson::pimpl() {
    return m_pimpl;
}

void CLowlaDBBson::appendDouble(const char *key, double value) {
    m_pimpl->appendDouble(key, value);
}

void CLowlaDBBson::appendString(const char *key, const char *value) {
    m_pimpl->appendString(key, value);
}

void CLowlaDBBson::appendObject(const char *key, const char *value) {
    m_pimpl->appendObject(key, value);
}

void CLowlaDBBson::appendOid(const char *key, const void *value) {
    m_pimpl->appendOid(key, (const bson_oid_t *)value);
}

void CLowlaDBBson::appendBool(const char *key, bool value) {
    m_pimpl->appendBool(key, value);
}

void CLowlaDBBson::appendDate(const char *key, int64_t value) {
    m_pimpl->appendDate(key, value);
}

void CLowlaDBBson::appendInt(const char *key, int value) {
    m_pimpl->appendInt(key, value);
}

void CLowlaDBBson::appendLong(const char *key, int64_t value) {
    m_pimpl->appendLong(key, value);
}

void CLowlaDBBson::startArray(const char *key) {
    m_pimpl->startArray(key);
}

void CLowlaDBBson::finishArray() {
    m_pimpl->finishArray();
}

void CLowlaDBBson::startObject(const char *key) {
    m_pimpl->startObject(key);
}

void CLowlaDBBson::finishObject() {
    m_pimpl->finishObject();
}

void CLowlaDBBson::finish() {
    m_pimpl->finish();
}

bool CLowlaDBBson::containsKey(const char *key) {
    return m_pimpl->containsKey(key);
}

bool CLowlaDBBson::doubleForKey(const char *key, double *ret) {
    return m_pimpl->doubleForKey(key, ret);
}

bool CLowlaDBBson::stringForKey(const char *key, const char **ret) {
    return m_pimpl->stringForKey(key, ret);
}

bool CLowlaDBBson::objectForKey(const char *key, CLowlaDBBson::ptr *ret) {
    const char *objBson;
    if (m_pimpl->objectForKey(key, &objBson)) {
        *ret = create(objBson, false);
        return true;
    }
    return false;
}

bool CLowlaDBBson::arrayForKey(const char *key, CLowlaDBBson::ptr *ret) {
    const char *objBson;
    if (m_pimpl->arrayForKey(key, &objBson)) {
        *ret = create(objBson, false);
        return true;
    }
    return false;
}

bool CLowlaDBBson::oidForKey(const char *key, char *ret) {
    return m_pimpl->oidForKey(key, (bson_oid_t *)ret);
}

bool CLowlaDBBson::boolForKey(const char *key, bool *ret) {
    return m_pimpl->boolForKey(key, ret);
}

bool CLowlaDBBson::dateForKey(const char *key, int64_t *ret) {
    return m_pimpl->dateForKey(key, ret);
}

bool CLowlaDBBson::intForKey(const char *key, int *ret) {
    return m_pimpl->intForKey(key, ret);
}

bool CLowlaDBBson::longForKey(const char *key, int64_t *ret) {
    return m_pimpl->longForKey(key, ret);
}

void CLowlaDBBson::oidToString(const char *oid, char *buffer) {
    bson_oid_to_string((const bson_oid_t *)oid, buffer);
}

void CLowlaDBBson::oidGenerate(char *buffer) {
    bson_oid_gen((bson_oid_t *)buffer);
}

void CLowlaDBBson::oidFromString(char *oid, const char *str) {
    bson_oid_from_string((bson_oid_t *)oid, str);
}

bool CLowlaDBBson::equalValues(const char *key, CLowlaDBBson::ptr other, const char *otherKey) {
    return m_pimpl->equalValues(key, other->pimpl().get(), otherKey);
}

const char *CLowlaDBBson::data() {
    return m_pimpl->data();
}

size_t CLowlaDBBson::size() {
    return m_pimpl->size();
}

CLowlaDBBson::CLowlaDBBson(std::shared_ptr<CLowlaDBBsonImpl> pimpl) : m_pimpl(pimpl) {
}
                                                                           
CLowlaDBBsonImpl::CLowlaDBBsonImpl() {
    bson_init(this);
}

CLowlaDBBsonImpl::CLowlaDBBsonImpl(const char *data, Mode mode) {
    switch (mode) {
        case OWN:
            bson_init_finished_data(this, (char *)data, true);
            break;
        case REF:
            bson_init_finished_data(this, (char *)data, false);
            break;
        case COPY:
            bson_init_finished_data_with_copy(this, data);
            break;
    }
}

CLowlaDBBsonImpl::~CLowlaDBBsonImpl() {
    bson_destroy(this);
}

std::shared_ptr<CLowlaDBBsonImpl> CLowlaDBBsonImpl::empty() {
    std::shared_ptr<CLowlaDBBsonImpl> answer = std::make_shared<CLowlaDBBsonImpl>();
    answer->finish();
    return answer;
}

const char *CLowlaDBBsonImpl::data() {
    assert(finished);
    return bson_data(this);
}

size_t CLowlaDBBsonImpl::size() {
    return bson_size(this);
}

void CLowlaDBBsonImpl::finish() {
    bson_finish(this);
}

bool CLowlaDBBsonImpl::containsKey(const char *key) {
    assert(finished); // Iterators can run off the end of unfinished bson and crash
    bson_iterator it[1];
    bson_type type = bson_find(it, this, key);
    return BSON_EOO != type;
}

void CLowlaDBBsonImpl::appendDouble(const char *key, double value) {
    bson_append_double(this, key, value);
}

void CLowlaDBBsonImpl::appendString(const char *key, const char *value) {
    bson_append_string(this, key, value);
}

void CLowlaDBBsonImpl::appendObject(const char *key, const char *value) {
    bson_append_start_object(this, key);
    CLowlaDBBsonImpl obj(value, CLowlaDBBsonImpl::REF);
    appendAll(&obj);
    bson_append_finish_object(this);
}

void CLowlaDBBsonImpl::appendOid(const char *key, const bson_oid_t *oid) {
    bson_append_oid(this, key, oid);
}

void CLowlaDBBsonImpl::appendBool(const char *key, bool value) {
    bson_append_bool(this, key, value);
}

void CLowlaDBBsonImpl::appendDate(const char *key, int64_t value) {
    bson_append_date(this, key, value);
}

void CLowlaDBBsonImpl::appendInt(const char *key, int value) {
    bson_append_int(this, key, value);
}

void CLowlaDBBsonImpl::appendLong(const char *key, int64_t value) {
    bson_append_long(this, key, value);
}

void CLowlaDBBsonImpl::appendAll(CLowlaDBBsonImpl *bson) {
    bson_iterator it[1];
    bson_iterator_init(it, bson);
    while (bson_iterator_next(it)) {
        bson_append_element(this, nullptr, it);
    }
}

void CLowlaDBBsonImpl::appendElement(const CLowlaDBBsonImpl *src, const char *key) {
    bson_iterator it[1];
    bson_type type = bson_find(it, src, key);
    if (BSON_EOO != type) {
        bson_append_element(this, nullptr, it);
    }
}

void CLowlaDBBsonImpl::appendElement(const char *newKey, const CLowlaDBBsonImpl *src, const char *key) {
    bson_iterator it[1];
    bson_type type = bson_find(it, src, key);
    if (BSON_EOO != type) {
        bson_append_element(this, newKey, it);
    }
}

void CLowlaDBBsonImpl::startArray(const char *key) {
    bson_append_start_array(this, key);
}

void CLowlaDBBsonImpl::finishArray() {
    bson_append_finish_array(this);
}

void CLowlaDBBsonImpl::startObject(const char *key) {
    bson_append_start_object(this, key);
}

void CLowlaDBBsonImpl::finishObject() {
    bson_append_finish_object(this);
}

bool CLowlaDBBsonImpl::doubleForKey(const char *key, double *ret) {
    bson_iterator it[1];
    bson_type type = bson_find(it, this, key);
    if (BSON_DOUBLE == type) {
        *ret = bson_iterator_double(it);
        return true;
    }
    return false;
}

bool CLowlaDBBsonImpl::stringForKey(const char *key, const char **ret) const {
    bson_iterator it[1];
    bson_type type = bson_find(it, this, key);
    if (BSON_STRING == type) {
        *ret = bson_iterator_string(it);
        return true;
    }
    return false;
}

bool CLowlaDBBsonImpl::objectForKey(const char *key, const char **ret) {
    bson_iterator it[1];
    bson_type type = bson_find(it, this, key);
    if (BSON_OBJECT == type) {
        bson sub[1];
        bson_iterator_subobject_init(it, sub, false);
        *ret = bson_data(sub);
        return true;
    }
    return false;
}

bool CLowlaDBBsonImpl::arrayForKey(const char *key, const char **ret) {
    bson_iterator it[1];
    bson_type type = bson_find(it, this, key);
    if (BSON_ARRAY == type) {
        bson sub[1];
        bson_iterator_subobject_init(it, sub, false);
        *ret = bson_data(sub);
        return true;
    }
    return false;
}

bool CLowlaDBBsonImpl::oidForKey(const char *key, bson_oid_t *ret) {
    bson_iterator it[1];
    bson_type type = bson_find(it, this, key);
    if (BSON_OID == type) {
        *ret = *bson_iterator_oid(it);
        return true;
    }
    return false;
}

bool CLowlaDBBsonImpl::boolForKey(const char *key, bool *ret) const {
    bson_iterator it[1];
    bson_type type = bson_find(it, this, key);
    if (BSON_BOOL == type) {
        *ret = bson_iterator_bool_raw(it);
        return true;
    }
    return false;
}

bool CLowlaDBBsonImpl::dateForKey(const char *key, bson_date_t *ret) {
    bson_iterator it[1];
    bson_type type = bson_find(it, this, key);
    if (BSON_DATE == type) {
        *ret = bson_iterator_date(it);
        return true;
    }
    return false;
}

bool CLowlaDBBsonImpl::intForKey(const char *key, int *ret) {
    bson_iterator it[1];
    bson_type type = bson_find(it, this, key);
    if (BSON_INT == type) {
        *ret = bson_iterator_int_raw(it);
        return true;
    }
    return false;
}

bool CLowlaDBBsonImpl::longForKey(const char *key, int64_t *ret) {
    bson_iterator it[1];
    bson_type type = bson_find(it, this, key);
    if (BSON_LONG == type) {
        *ret = bson_iterator_long_raw(it);
        return true;
    }
    return false;
}

bool CLowlaDBBsonImpl::equalValues(const char *key, const CLowlaDBBsonImpl *other, const char *otherKey) {
    bson_iterator it[1];
    bson_type type = bson_find(it, this, key);
    if (BSON_EOO == type) {
        return false;
    }
    bson_iterator otherIt[1];
    bson_type otherType = bson_find(otherIt, other, otherKey);
    if (type != otherType) {
        return false;
    }
    const char *val = bson_iterator_value(it);
    const char *otherVal = bson_iterator_value(otherIt);
    bson_iterator_next(it);
    bson_iterator_next(otherIt);
    long valLength = it->cur - val;
    long otherValLength = otherIt->cur - otherVal;
    if (valLength != otherValLength) {
        return false;
    }
    return 0 == memcmp(val, otherVal, valLength);
}

bool CLowlaDBBsonImpl::isEmpty() {
    // An empty bson is 5 bytes: 4 for the length and 1 for a null.
    return 5 == size();
}

CLowlaDBCursor::ptr CLowlaDBCursor::create(std::shared_ptr<CLowlaDBCursorImpl> pimpl) {
    return CLowlaDBCursor::ptr(new CLowlaDBCursor(pimpl));
}

std::shared_ptr<CLowlaDBCursorImpl> CLowlaDBCursor::pimpl() {
    return m_pimpl;
}

CLowlaDBCursor::ptr CLowlaDBCursor::create(CLowlaDBCollection::ptr coll, const char *query) {
    // Cursors have a complex lifetime so this is one of the few cases where we need to copy the bson
    std::shared_ptr<CLowlaDBBsonImpl> bsonQuery;
    if (query) {
        bsonQuery.reset(new CLowlaDBBsonImpl(query, CLowlaDBBsonImpl::COPY));
    }
    
    return create(std::make_shared<CLowlaDBCursorImpl>(coll->pimpl(), bsonQuery, std::shared_ptr<CLowlaDBBsonImpl>()));
}

CLowlaDBCursor::ptr CLowlaDBCursor::limit(int limit) {
    std::shared_ptr<CLowlaDBCursorImpl> pimpl = m_pimpl->limit(limit);
    return create(pimpl);
}

CLowlaDBCursor::ptr CLowlaDBCursor::skip(int skip) {
    std::shared_ptr<CLowlaDBCursorImpl> pimpl = m_pimpl->skip(skip);
    return create(pimpl);
}

CLowlaDBCursor::ptr CLowlaDBCursor::sort(const char *sort) {
    std::shared_ptr<CLowlaDBBsonImpl> bsonSort(new CLowlaDBBsonImpl(sort, CLowlaDBBsonImpl::COPY));

    std::shared_ptr<CLowlaDBCursorImpl> pimpl = m_pimpl->sort(bsonSort);
    return create(pimpl);
}

CLowlaDBCursor::ptr CLowlaDBCursor::showPending() {
    std::shared_ptr<CLowlaDBCursorImpl> pimpl = m_pimpl->showPending();
    return create(pimpl);
}

CLowlaDBBson::ptr CLowlaDBCursor::next() {
    std::shared_ptr<CLowlaDBBsonImpl> answer = m_pimpl->next();
    return CLowlaDBBson::create(answer);
}

int64_t CLowlaDBCursor::count() {
    return m_pimpl->count();
}

CLowlaDBCursor::CLowlaDBCursor(std::shared_ptr<CLowlaDBCursorImpl> pimpl) : m_pimpl(pimpl) {
}

CLowlaDBCursorImpl::CLowlaDBCursorImpl(const CLowlaDBCursorImpl &other) : m_coll(other.m_coll), m_query(other.m_query), m_keys(other.m_keys), m_sort(other.m_sort), m_limit(other.m_limit), m_skip(other.m_skip), m_showPending(other.m_showPending), m_showDiskLoc(other.m_showDiskLoc) {
}

CLowlaDBCursorImpl::CLowlaDBCursorImpl(CLowlaDBCollectionImpl::ptr coll, std::shared_ptr<CLowlaDBBsonImpl> query, std::shared_ptr<CLowlaDBBsonImpl> keys) : m_coll(coll), m_query(query), m_keys(keys), m_limit(0), m_skip(0), m_showPending(false), m_showDiskLoc(false) {
}

std::unique_ptr<CLowlaDBCursorImpl> CLowlaDBCursorImpl::limit(int limit) {
    std::unique_ptr<CLowlaDBCursorImpl> answer(new CLowlaDBCursorImpl(*this));
    answer->m_limit = limit;
    return answer;
}

std::unique_ptr<CLowlaDBCursorImpl> CLowlaDBCursorImpl::skip(int skip) {
    std::unique_ptr<CLowlaDBCursorImpl> answer(new CLowlaDBCursorImpl(*this));
    answer->m_skip = skip;
    return answer;
}

std::unique_ptr<CLowlaDBCursorImpl> CLowlaDBCursorImpl::sort(std::shared_ptr<CLowlaDBBsonImpl> sort) {
    std::unique_ptr<CLowlaDBCursorImpl> answer(new CLowlaDBCursorImpl(*this));
    answer->m_sort = sort;
    return answer;
}

std::unique_ptr<CLowlaDBCursorImpl> CLowlaDBCursorImpl::showPending() {
    std::unique_ptr<CLowlaDBCursorImpl> answer(new CLowlaDBCursorImpl(*this));
    answer->m_showPending = true;
    return answer;
}

std::unique_ptr<CLowlaDBCursorImpl> CLowlaDBCursorImpl::showDiskLoc() {
    std::unique_ptr<CLowlaDBCursorImpl> answer(new CLowlaDBCursorImpl(*this));
    answer->m_showDiskLoc = true;
    return answer;
}

std::unique_ptr<CLowlaDBBsonImpl> CLowlaDBCursorImpl::next() {
    if (m_sort) {
        return nextSorted();
    }
    else {
        return nextUnsorted();
    }
}

std::unique_ptr<CLowlaDBBsonImpl> CLowlaDBCursorImpl::nextUnsorted() {
    int rc;
    int res;
    if (!m_tx) {
        m_tx.reset(new Tx(m_coll->db()->btree()));
        m_cursor = m_coll->openCursor();
        m_logCursor = m_coll->openLogCursor();
        m_unsortedOffset = 0;
        rc = m_cursor->first(&res);
    }
    else {
        if (0 != m_limit && m_skip + m_limit < m_unsortedOffset) {
            rc = m_cursor->last(&res);
        }
        rc = m_cursor->next(&res);
    }
    while (SQLITE_OK == res && 0 == rc) {
        u32 size;
        m_cursor->dataSize(&size);
        char *data = (char *)bson_malloc(size);
        m_cursor->data(0, size, data);
        CLowlaDBBsonImpl found(data, CLowlaDBBsonImpl::OWN);
        if (nullptr == m_query || matches(&found)) {
            ++m_unsortedOffset;
            if (m_skip < m_unsortedOffset && (0 == m_limit || m_unsortedOffset <= m_skip + m_limit)) {
                i64 id;
                m_cursor->keySize(&id);
                std::unique_ptr<CLowlaDBBsonImpl> answer = project(&found, id);
                return answer;
            }
        }
        rc = m_cursor->next(&res);
    }
    return std::unique_ptr<CLowlaDBBsonImpl>();
}

std::unique_ptr<CLowlaDBBsonImpl> CLowlaDBCursorImpl::nextSorted() {
    int rc;
    int res;
    if (!m_tx) {
        m_tx.reset(new Tx(m_coll->db()->btree()));
        m_cursor = m_coll->openCursor();
        m_logCursor = m_coll->openLogCursor();
        performSortedQuery();
    }
    
    while (m_walkSorted != m_sortedIds.end()) {
        i64 id = *m_walkSorted++;
        rc = m_cursor->movetoUnpacked(nullptr, id, 1, &res);
        if (SQLITE_OK != rc || 0 != res) {
            continue;
        }
        u32 size;
        m_cursor->dataSize(&size);
        char *data = (char *)bson_malloc(size);
        m_cursor->data(0, size, data);
        CLowlaDBBsonImpl found(data, CLowlaDBBsonImpl::OWN);
        std::unique_ptr<CLowlaDBBsonImpl> answer = project(&found, id);
        return answer;
    }
    return std::unique_ptr<CLowlaDBBsonImpl>();
}

/* Compare two bson values using the algorithm described at http://docs.mongodb.org/master/reference/method/cursor.sort/#cursor.sort
 */
static int compareBsonFields(bson_iterator *itA, bson_iterator *itB)
{
    static const int typeOrder[] = { BSON_MINKEY, BSON_NULL, BSON_INT, BSON_STRING, BSON_OBJECT, BSON_ARRAY, BSON_BINDATA, BSON_OID, BSON_BOOL, BSON_DATE, BSON_TIMESTAMP, BSON_REGEX, BSON_MAXKEY};
    
    int typeA = bson_iterator_type(itA);
    int typeB = bson_iterator_type(itB);

    if (BSON_LONG == typeA || BSON_DOUBLE == typeA) {
        typeA = BSON_INT;
    }
    if (BSON_SYMBOL == typeA) {
        typeA = BSON_STRING;
    }
    if (BSON_LONG == typeB || BSON_DOUBLE == typeB) {
        typeB = BSON_INT;
    }
    if (BSON_SYMBOL == typeB) {
        typeB = BSON_STRING;
    }
    if (typeA != typeB) {
        const int *end = typeOrder + sizeof(typeOrder) / sizeof(int);
        auto posA = std::find(typeOrder, end, typeA);
        auto posB = std::find(typeOrder, end, typeB);
        return posA < posB ? -1 : +1;
    }
    switch (typeA) {
        case BSON_MINKEY:
        case BSON_NULL:
        case BSON_MAXKEY:
            return 0;
        case BSON_INT: {
            double dA = bson_iterator_double(itA);
            double dB = bson_iterator_double(itB);
            return dA < dB ? -1 : (dB < dA ? +1 : 0);
        }
        case BSON_STRING:
            return strcmp(bson_iterator_string(itA), bson_iterator_string(itB));
        case BSON_OBJECT: {
            bson subA[1];
            bson subB[1];
            bson_iterator_subobject_init(itA, subA, false);
            bson_iterator_subobject_init(itB, subB, false);
            if (bson_size(subA) != bson_size(subB) ) {
                return bson_size(subA) < bson_size(subB) ? -1 : +1;
            }
            return memcmp(bson_data(subA), bson_data(subB), bson_size(subA));
        }
        case BSON_ARRAY:
            assert(false); // Arrays should have been replaced with their max/min value
            break;
        case BSON_BINDATA:
            if (bson_iterator_bin_len(itA) != bson_iterator_bin_len(itB)) {
                return bson_iterator_bin_len(itA) < bson_iterator_bin_len(itB) ? -1 : +1;
            }
            if (bson_iterator_bin_type(itA) != bson_iterator_bin_type(itB)) {
                return bson_iterator_bin_type(itA) < bson_iterator_bin_type(itB) ? -1 : +1;
            }
            return memcmp(bson_iterator_bin_data(itA), bson_iterator_bin_data(itB), bson_iterator_bin_len(itA));
        case BSON_OID:
            return memcmp(bson_iterator_oid(itA), bson_iterator_oid(itB), sizeof(bson_oid_t));
        case BSON_BOOL:
            if (!bson_iterator_bool(itA)) {
                return bson_iterator_bool(itB) ? -1 : 0;
            }
            else {
                return bson_iterator_bool(itB) ? 0 : +1;
            }
        case BSON_DATE: {
            bson_date_t dA = bson_iterator_date(itA);
            bson_date_t dB = bson_iterator_date(itB);
            return dA < dB ? -1 : (dA == dB ? 0 : +1);
        }
        case BSON_TIMESTAMP:
        case BSON_REGEX:
            assert(false); // We don't support these!
            break;
    }
    return 0;
}

class MyLess {
public:
    MyLess(std::vector<std::pair<std::vector<utf16string>, int>> const &parsedSort) {
        for (auto walk = parsedSort.begin() ; walk != parsedSort.end() ; ++walk) {
            m_asc.push_back(walk->second == 1);
        }
    }
    
    bool operator () (std::shared_ptr<CLowlaDBBsonImpl> const &a, std::shared_ptr<CLowlaDBBsonImpl> const &b) const {
        bson_iterator itA[1];
        bson_iterator itB[1];
        
        bson_iterator_init(itA, a.get());
        bson_iterator_init(itB, b.get());
        
        int idx = 0;
        int type = bson_iterator_next(itA);
        while (BSON_EOO != type) {
            int typeB = bson_iterator_next(itB);
            assert(BSON_EOO != typeB);
            int compare = compareBsonFields(itA, itB);
            if (0 != compare) {
                return (m_asc[idx] && compare < 0) || (!m_asc[idx] && 0 < compare);
            }
            ++idx;
            type = bson_iterator_next(itA);
        }
        return false;
    }
    
private:
    std::vector<bool> m_asc;
};

void CLowlaDBCursorImpl::performSortedQuery() {
    parseSortSpec();
    
    MyLess comp(m_parsedSort);
    std::multimap<std::shared_ptr<CLowlaDBBsonImpl>, i64, MyLess> sortData(comp);
    
    int rc, res;
    rc = m_cursor->first(&res);
    while (SQLITE_OK == rc && 0 == res) {
        u32 size;
        m_cursor->dataSize(&size);
        char *data = (char *)bson_malloc(size);
        m_cursor->data(0, size, data);
        CLowlaDBBsonImpl found(data, CLowlaDBBsonImpl::OWN);
        
        if (nullptr == m_query || matches(&found)) {
            i64 id;
            m_cursor->keySize(&id);
            std::shared_ptr<CLowlaDBBsonImpl> key = createSortKey(&found);
            sortData.insert(std::pair<std::shared_ptr<CLowlaDBBsonImpl>, i64>(key, id));
        }
        rc = m_cursor->next(&res);
    }
    m_sortedIds.reserve(0 == m_limit ? sortData.size() - m_skip : m_limit);
    auto walk = sortData.begin();
    std::advance(walk, m_skip);
    if (0 == m_limit) {
        for ( ; walk != sortData.end(); ++walk) {
            m_sortedIds.push_back(walk->second);
        }
    }
    else {
        for (int limit = 0; walk != sortData.end() && limit < m_limit; ++walk, ++limit) {
            m_sortedIds.push_back(walk->second);
        }
    }
    m_walkSorted = m_sortedIds.begin();
}

void CLowlaDBCursorImpl::parseSortSpec()
{
    bson_iterator it[1];
    bson_iterator_init(it, m_sort.get());
    bson_type walk = bson_iterator_next(it);
    while (BSON_EOO != walk) {
        int sortOrder = bson_iterator_int(it);
        if (1 != sortOrder && -1 != sortOrder) {
            throw TeamstudioException("Invalid sort specification: values must be +/- 1");
        }
        std::vector<utf16string> keys;
        utf16string key(bson_iterator_key(it));
        int startPos = 0;
        int dotPos = key.indexOf('.');
        while (-1 != dotPos) {
            keys.push_back(key.substring(startPos, dotPos));
            startPos = dotPos + 1;
            dotPos = startPos < key.length() ? key.indexOf('.', startPos) : -1;
        }
        if (startPos < key.length()) {
            keys.push_back(key.substring(startPos));
        }
        m_parsedSort.push_back(std::make_pair(keys, sortOrder));
        
        walk = bson_iterator_next(it);
    }
}

static bson_type locateDottedField(bson_iterator *it, CLowlaDBBsonImpl *doc, std::vector<utf16string> const &keys)
{
    bson_type answer = BSON_EOO;
    bson_iterator_init(it, doc);
    for (int i = 0 ; i < keys.size() - 1 ; ++i) {
        const char *keystr = keys[i].c_str();
        answer = bson_iterator_next(it);
        while (BSON_EOO != answer) {
            if (BSON_OBJECT == answer && 0 == strcmp(keystr, bson_iterator_key(it))) {
                bson_iterator_subiterator(it, it);
                break;
            }
            answer = bson_iterator_next(it);
        }
        if (BSON_EOO == answer) {
            return answer;
        }
    }
    const char *keystr = keys.back().c_str();
    answer = bson_iterator_next(it);
    while (BSON_EOO != answer) {
        if (0 == strcmp(keystr, bson_iterator_key(it))) {
            break;
        }
        answer = bson_iterator_next(it);
    }
    return answer;
}

std::shared_ptr<CLowlaDBBsonImpl> CLowlaDBCursorImpl::createSortKey(CLowlaDBBsonImpl *found) {
    
    std::shared_ptr<CLowlaDBBsonImpl> answer(new CLowlaDBBsonImpl);
    
    for (auto walk = m_parsedSort.begin() ; walk != m_parsedSort.end() ; ++walk) {
        bson_iterator it[1];
        bson_type type = locateDottedField(it, found, walk->first);
        if (BSON_EOO == type) {
            bson_append_null(answer.get(), "");
        }
        else if (BSON_ARRAY != type) {
            bson_append_element(answer.get(), "", it);
        }
        else {
            throw TeamstudioException("Internal error: array values not implemented for sort");
        }
    }
    answer->finish();
    return answer;
}

SqliteCursor::ptr CLowlaDBCursorImpl::sqliteCursor() {
    return m_cursor;
}

std::unique_ptr<CLowlaDBBsonImpl> CLowlaDBCursorImpl::currentMeta() {
    u32 dataSize;
    m_cursor->dataSize(&dataSize);
    int objSize = 0;
    int cbRequired = 4;
    bson_little_endian32(&objSize, m_cursor->dataFetch(&cbRequired));
    int metaSize = dataSize - objSize;
    char *meta = (char *)bson_malloc(metaSize);
    m_cursor->data(objSize, metaSize, meta);
    return std::unique_ptr<CLowlaDBBsonImpl>(new CLowlaDBBsonImpl(meta, CLowlaDBBsonImpl::OWN));
}

int64_t CLowlaDBCursorImpl::currentId() {
    int64_t answer;
    m_cursor->keySize(&answer);
    return answer;
}


bool CLowlaDBCursorImpl::matches(CLowlaDBBsonImpl *found) {
    if (nullptr == m_query) {
        return true;
    }
    bson_iterator it[1];
    bson_iterator_init(it, m_query.get());
    while (bson_iterator_next(it)) {
        const char *key = bson_iterator_key(it);
        if (!m_query->equalValues(key, found, key)) {
            return false;
        }
    }
    return true;
}

std::unique_ptr<CLowlaDBBsonImpl> CLowlaDBCursorImpl::project(CLowlaDBBsonImpl *found, i64 id) {
    if (nullptr == m_keys && !m_showDiskLoc && !m_showPending) {
        if (found->ownsData) {
            found->ownsData = false;
        }
        std::unique_ptr<CLowlaDBBsonImpl> answer(new CLowlaDBBsonImpl(found->data(), CLowlaDBBsonImpl::OWN));
        return answer;
    }
    std::unique_ptr<CLowlaDBBsonImpl> answer(new CLowlaDBBsonImpl());
    answer->appendElement(found, "_id");
    bson_iterator it[1];
    bson_iterator_init(it, found);
    while (BSON_EOO != bson_iterator_next(it)) {
        const char *key = bson_iterator_key(it);
        if (nullptr == m_keys) {
            if (0 != strcmp("_id", key)) {
                bson_append_element(answer.get(), nullptr, it);
            }
        }
    }
    if (m_showDiskLoc) {
        CLowlaDBBsonImpl diskLoc[1];
        diskLoc->appendInt("file", 0);
        diskLoc->appendLong("offset", id);
        diskLoc->finish();
        answer->appendObject("$diskLoc", diskLoc->data());
    }
    if (m_showPending) {
        int res = 0;
        int rc = m_logCursor->movetoUnpacked(nullptr, id, 0, &res);
        answer->appendBool("$pending", SQLITE_OK == rc && 0 == res);
    }
    answer->finish();
    return answer;
}

int64_t CLowlaDBCursorImpl::count() {
    if (!m_tx) {
        m_tx.reset(new Tx(m_coll->db()->btree()));
        m_cursor = m_coll->openCursor();
    }
    
    int rc;
    int res;
    i64 answer = 0;
    
    rc = m_cursor->first(&res);
    if (nullptr == m_query) {
        answer = m_cursor->count();
    }
    else {
        while (SQLITE_OK == res && 0 == rc) {
            u32 size;
            m_cursor->dataSize(&size);
            char *data = (char *)bson_malloc(size);
            m_cursor->data(0, size, data);
            CLowlaDBBsonImpl found(data, CLowlaDBBsonImpl::OWN);
            if (matches(&found)) {
                ++answer;
                if (0 != m_limit && m_skip + m_limit <= answer) {
                    break;
                }
            }
            rc = m_cursor->next(&res);
        }
    }
    if (answer <= m_skip) {
        answer = 0;
    }
    else if (0 == m_limit || answer <= m_skip + m_limit) {
        answer -= m_skip;
    }
    else {
        answer = m_limit;
    }
    return answer;
}

CLowlaDBPullData::ptr CLowlaDBPullData::create(std::shared_ptr<CLowlaDBPullDataImpl> pimpl) {
    return CLowlaDBPullData::ptr(new CLowlaDBPullData(pimpl));
}

std::shared_ptr<CLowlaDBPullDataImpl> CLowlaDBPullData::pimpl() {
    return m_pimpl;
}

CLowlaDBPullData::CLowlaDBPullData(std::shared_ptr<CLowlaDBPullDataImpl> pimpl) : m_pimpl(pimpl) {
}

bool CLowlaDBPullData::hasRequestMore() {
    return m_pimpl->hasRequestMore();
}

utf16string CLowlaDBPullData::getRequestMore() {
    return m_pimpl->getRequestMore();
}

bool CLowlaDBPullData::isComplete() {
    return m_pimpl->isComplete();
}

int CLowlaDBPullData::getSequenceForNextRequest() {
    return m_pimpl->getSequenceForNextRequest();
}

CLowlaDBPullDataImpl::CLowlaDBPullDataImpl() : m_sequence(0) {
}

void CLowlaDBPullDataImpl::appendAtom(const char *atomBson) {
    m_atoms.emplace_back(new CLowlaDBBsonImpl(atomBson, CLowlaDBBsonImpl::COPY));
}

void CLowlaDBPullDataImpl::setSequence(int sequence) {
    m_sequence = sequence;
}

CLowlaDBPullDataImpl::atomIterator CLowlaDBPullDataImpl::atomsBegin() {
    return m_atoms.begin();
}

CLowlaDBPullDataImpl::atomIterator CLowlaDBPullDataImpl::atomsEnd() {
    return m_atoms.end();
}

CLowlaDBPullDataImpl::atomIterator CLowlaDBPullDataImpl::eraseAtom(atomIterator walk) {
    return m_atoms.erase(walk);
}

void CLowlaDBPullDataImpl::eraseAtom(const char *id) {
    for (atomIterator walk = m_atoms.begin() ; walk != m_atoms.end() ; ++walk) {
        const char *val;
        if ((*walk)->stringForKey("id", &val) && 0 == strcmp(val, id)) {
            m_atoms.erase(walk);
            return;
        }
    }
}

void CLowlaDBPullDataImpl::setRequestMore(const char *requestMore) {
    m_requestMore = requestMore;
}

bool CLowlaDBPullDataImpl::hasRequestMore() {
    return !m_requestMore.isEmpty();
}

utf16string CLowlaDBPullDataImpl::getRequestMore() {
    return m_requestMore;
}

bool CLowlaDBPullDataImpl::isComplete() {
    return !hasRequestMore() && m_atoms.empty();
}

int CLowlaDBPullDataImpl::getSequenceForNextRequest() {
    if (m_atoms.empty()) {
        return m_sequence;
    }
    int answer = 0;
    m_atoms[0]->intForKey("sequence", &answer);
    return answer;
}

CLowlaDBPushData::ptr CLowlaDBPushData::create(std::shared_ptr<CLowlaDBPushDataImpl> pimpl) {
    return CLowlaDBPushData::ptr(new CLowlaDBPushData(pimpl));
}

std::shared_ptr<CLowlaDBPushDataImpl> CLowlaDBPushData::pimpl() {
    return m_pimpl;
}

bool CLowlaDBPushData::isComplete() {
    return m_pimpl->isComplete();
}

CLowlaDBPushData::CLowlaDBPushData(std::shared_ptr<CLowlaDBPushDataImpl> pimpl) : m_pimpl(pimpl) {
}

void CLowlaDBPushDataImpl::registerIds(const utf16string &ns, const std::vector<int64_t> &ids) {
    if (ids.empty()) {
        return;
    }
    if (m_ids.find(ns) == m_ids.end()) {
        m_ids.emplace(std::make_pair(ns, ids));
    }
    else {
        std::vector<int64_t> &existing = m_ids[ns];
        existing.insert(existing.end(), ids.begin(), ids.end());
    }
}

bool CLowlaDBPushDataImpl::isComplete() {
    return m_ids.empty();
}

static bool appendModification(CLowlaDBBson::ptr answer, CLowlaDBBsonImpl *newDoc, CLowlaDBBsonImpl *oldDoc, const char *lowlaId, int index) {
    // Create the $set subobject
    CLowlaDBBsonImpl setObj;
    bson_iterator walkNew[1];
    bson_iterator_init(walkNew, newDoc);
    while (BSON_EOO != bson_iterator_next(walkNew)) {
        const char *key = bson_iterator_key(walkNew);
        if (0 != strcmp("_id", key) && !newDoc->equalValues(key, oldDoc, key)) {
            setObj.appendElement(newDoc, key);
        }
    }
    setObj.finish();
    
    // Create the $unset subobject
    CLowlaDBBsonImpl unsetObj;
    bson_iterator walkOld[1];
    bson_iterator_init(walkOld, oldDoc);
    while (BSON_EOO != bson_iterator_next(walkOld)) {
        const char *key = bson_iterator_key(walkOld);
        if (!newDoc->containsKey(key)) {
            unsetObj.appendString(key, "");
        }
    }
    unsetObj.finish();
    
    // If they're both empty then nothing to push
    if (setObj.isEmpty() && unsetObj.isEmpty()) {
        return false;
    }
    
    // Append it all to the answer
    answer->startObject(utf16string::valueOf(index).c_str());
    // Append the lowla fields
    answer->startObject("_lowla");
    answer->appendString("id", lowlaId);
    answer->pimpl()->appendElement("version", oldDoc, "_version");
    answer->finishObject();
    // Append the operations
    answer->startObject("ops");
    if (!setObj.isEmpty()) {
        answer->appendObject("$set", setObj.data());
    }
    if (!unsetObj.isEmpty()) {
        answer->appendObject("$unset", unsetObj.data());
    }
    answer->finishObject();
    answer->finishObject();
    
    return true;
}

static void appendDeletion(CLowlaDBBson::ptr answer, CLowlaDBBsonImpl *oldDoc, const char *lowlaId, int index) {
    answer->startObject(utf16string::valueOf(index).c_str());
    // Append the lowla fields
    answer->startObject("_lowla");
    answer->appendString("id", lowlaId);
    answer->pimpl()->appendElement("version", oldDoc, "_version");
    answer->appendBool("deleted", true);
    answer->finishObject();
    answer->finishObject();
}

CLowlaDBBson::ptr CLowlaDBPushDataImpl::request() {
    auto walk = m_ids.begin();
    CLowlaDBNsCache nsCache;
    CLowlaDBCollectionImpl *coll;
    
    while (walk != m_ids.end()) {
        coll = nsCache.collectionForNs(walk->first.c_str());
        if (nullptr == coll) {
            // If the collection is now gone, there's nothing more we can do
            walk = m_ids.erase(walk);
        }
        else {
            break;
        }
    }
    if (walk == m_ids.end()) {
        return CLowlaDBBson::ptr();
    }
    
    std::vector<int64_t> &sqliteIds = walk->second;
    int processed = 0;
    CLowlaDBBson::ptr answer = CLowlaDBBson::create();
    answer->startArray("documents");
    
    while (processed < 10 && !sqliteIds.empty()) {
        int64_t id = sqliteIds.back();
        sqliteIds.pop_back();
        std::unique_ptr<CLowlaDBSyncDocumentLocation> loc = coll->locateDocumentForSqliteId(id);
        if (!loc->m_logFound) {
            // Something's gone horribly wrong - by definition we *were* in the log. All we can do is skip
            continue;
        }
        // Load up the log document & meta
        u32 dataSize;
        loc->m_logCursor->dataSize(&dataSize);
        int objSize = 0;
        int cbRequired = 4;
        bson_little_endian32(&objSize, loc->m_logCursor->dataFetch(&cbRequired));
        int metaSize = dataSize - objSize;
        char *data = (char *)bson_malloc(objSize);
        loc->m_logCursor->data(0, objSize, data);
        std::unique_ptr<CLowlaDBBsonImpl> logDoc(new CLowlaDBBsonImpl(data, CLowlaDBBsonImpl::OWN));
        char *meta = (char *)bson_malloc(metaSize);
        loc->m_logCursor->data(objSize, metaSize, meta);
        std::unique_ptr<CLowlaDBBsonImpl> logMeta(new CLowlaDBBsonImpl(meta, CLowlaDBBsonImpl::OWN));
        
        const char *lowlaId;
        logMeta->stringForKey("id", &lowlaId);
        
        if (loc->m_found) {
            bool changesFound = appendModification(answer, loc->m_found.get(), logDoc.get(), lowlaId, processed);
            if (!changesFound) {
                loc->m_logCursor->deleteCurrent();
                continue;
            }
        }
        else {
            appendDeletion(answer, logDoc.get(), lowlaId, processed);
        }
        
        // And create the pending record
        utf16string newHash;
        if (loc->m_found) {
            MD5 md5;
            md5.update(loc->m_found->data(), (int)loc->m_found->size());
            newHash = md5.finalize().hexdigest();
        }
        m_pending.emplace_back(walk->first, lowlaId, id, newHash);
        
        ++processed;
    }
    answer->finishArray();
    answer->finish();
    
    if (sqliteIds.empty()) {
        m_ids.erase(walk);
    }
        
    return answer;
}

// Returns a non-null SyncDocumentLocation if the response can be accepted
std::unique_ptr<CLowlaDBSyncDocumentLocation> CLowlaDBPushDataImpl::canAcceptPushResponse(CLowlaDBCollectionImpl *coll, const char *ns, const char *id) {
    // We can accept a push response if
    // a) we sent a push request for this document; and
    // b) the document hasn't changed since we sent the request
    std::unique_ptr<CLowlaDBSyncDocumentLocation> loc;
    for (CLowlaDBPushDataImpl::CPushedRecord &pr : m_pending) {
        if (pr.m_id == id && pr.m_ns == ns) {
            // At this point we know we sent a push request. Need to check the hash next
            loc = coll->locateDocumentForSqliteId(pr.m_sqliteId);
            // If the record is now deleted and was deleted when we pushed then ok
            if (!loc->m_found) {
                if (!pr.m_md5.isEmpty()) {
                    loc.reset();
                }
                return loc;
            }
            // Otherwise the hashes need to match
            utf16string newHash;
            if (loc->m_found) {
                MD5 md5;
                md5.update(loc->m_found->data(), (int)loc->m_found->size());
                newHash = md5.finalize().hexdigest();
            }
            if (newHash != pr.m_md5) {
                loc.reset();
            }
            return loc;
        }
    }
    return loc;
}

CLowlaDBPushDataImpl::CPushedRecord::CPushedRecord(const utf16string &ns, const char *lowlaId, int64_t id, const utf16string &md5) : m_ns(ns), m_id(lowlaId), m_sqliteId(id), m_md5(md5)
{
}

utf16string lowladb_get_version() {
    return "0.0.2";
}

void lowladb_db_delete(const utf16string &name) {
    utf16string filePath = getFullPath(name);

    remove(filePath.c_str());
}

CLowlaDBPullData::ptr lowladb_parse_syncer_response(const char *bsonData) {
    CLowlaDBBsonImpl bson(bsonData, CLowlaDBBsonImpl::REF);
    std::shared_ptr<CLowlaDBPullDataImpl> pd(new CLowlaDBPullDataImpl);
    int sequence;
    if (bson.intForKey("sequence", &sequence)) {
        pd->setSequence(sequence);
    }
    const char *atoms;
    if (bson.arrayForKey("atoms", &atoms)) {
        CLowlaDBBsonImpl array(atoms, CLowlaDBBsonImpl::REF);
        int i = 0;
        char szBuf[20];
        sprintf(szBuf, "%d", i);
        const char *obj;
        while (array.objectForKey(szBuf, &obj)) {
            pd->appendAtom(obj);
            ++i;
            sprintf(szBuf, "%d", i);
        }
    }
    return CLowlaDBPullData::create(pd);
}

static bool shouldPullDocument(const CLowlaDBBsonImpl *atom, CLowlaDBNsCache &cache) {
    const char *ns;
    atom->stringForKey("clientNs", &ns);
    CLowlaDBCollectionImpl *coll = cache.collectionForNs(ns);
    if (coll) {
        return coll->shouldPullDocument(atom);
    }
    return false;
}

CLowlaDBBson::ptr lowladb_create_pull_request(CLowlaDBPullData::ptr pd) {
    std::shared_ptr<CLowlaDBPullDataImpl> pullData = pd->pimpl();
    CLowlaDBNsCache cacheNs;
    std::shared_ptr<CLowlaDBBsonImpl> answer(new CLowlaDBBsonImpl);
    if (pullData->hasRequestMore()) {
        answer->appendString("requestMore", pullData->getRequestMore().c_str());
        answer->finish();
        return CLowlaDBBson::create(answer);
    }
    int i = 0;
    answer->startArray("ids");
    CLowlaDBPullDataImpl::atomIterator walk = pullData->atomsBegin();
    bool foundIdToPull = false;
    while (walk != pullData->atomsEnd()) {
        const CLowlaDBBsonImpl *atom = (*walk).get();
        bool deleted;
        if (atom->boolForKey("deleted", &deleted) && deleted) {
            // We don't pull deletions, but we need to keep the atoms so that
            // we'll know to delete the documents as we process the response.
            ++walk;
            continue;
        }
        else if (shouldPullDocument(atom, cacheNs)) {
            foundIdToPull = true;
            const char *id;
            atom->stringForKey("id", &id);
            answer->appendString(utf16string::valueOf(i++).c_str(), id);
            if (PULL_BATCH_SIZE == i) {
                break;
            }
            ++walk;
        }
        else {
            walk = pullData->eraseAtom(walk);
        }
    }
    answer->finishArray();
    answer->finish();
    if (foundIdToPull) {
        return CLowlaDBBson::create(answer);
    }
    else {
        return CLowlaDBBson::ptr();
    }
}

void processLeadingDeletions(CLowlaDBPullDataImpl *pullData, CLowlaDBNsCache &cache) {
    CLowlaDBPullDataImpl::atomIterator walk = pullData->atomsBegin();
    while (walk != pullData->atomsEnd()) {
        const CLowlaDBBsonImpl *atom = (*walk).get();
        bool isDeletion;
        if (atom->boolForKey("deleted", &isDeletion) && isDeletion) {
            const char *ns;
            atom->stringForKey("clientNs", &ns);
            CLowlaDBCollectionImpl *coll = cache.collectionForNs(ns);
            if (coll) {
                const char *id;
                atom->stringForKey("id", &id);
                std::unique_ptr<CLowlaDBSyncDocumentLocation> loc = coll->locateDocumentForId(id);
                // We only process the deletion if there is no outgoing record
                if (!loc->m_logFound && loc->m_found) {
                    loc->m_cursor->deleteCurrent();
                }
            }
            walk = pullData->eraseAtom(walk);
        }
        else {
            break;
        }
    }
}

void lowladb_apply_pull_response(const std::vector<CLowlaDBBson::ptr> &response, CLowlaDBPullData::ptr pd) {
    std::shared_ptr<CLowlaDBPullDataImpl> pullData = pd->pimpl();
    CLowlaDBNsCache cache;
    cache.setNotifyOnClose(true);
    
    int i = 0;
    while (i < response.size()) {
        processLeadingDeletions(pullData.get(), cache);
        CLowlaDBBson::ptr metaBson = response[i];
        const char *requestMore;
        if (metaBson->stringForKey("requestMore", &requestMore)) {
            pullData->setRequestMore(requestMore);
            ++i;
            continue;
        }
        bool isDeletion = metaBson->boolForKey("deleted", &isDeletion) && isDeletion;
        if (!isDeletion) {
            if (response.size() <= i + 1) {
                // Error - non deletion metadata not followed by document
                break;
            }
            ++i;
        }

        const char *ns;
        metaBson->stringForKey("clientNs", &ns);
        CLowlaDBCollectionImpl *coll = cache.collectionForNs(ns);
        if (!coll) {
            ++i;
            continue;
        }
        coll->setWriteLog(false);
        Tx tx(coll->db()->btree());
        const char *id;
        metaBson->stringForKey("id", &id);
        std::unique_ptr<CLowlaDBSyncDocumentLocation> loc = coll->locateDocumentForId(id);
        // Don't do anything if there's an outgoing log document
        if (loc->m_logFound) {
            pullData->eraseAtom(id);
            ++i;
            continue;
        }
        if (isDeletion) {
            if (loc->m_found) {
                loc->m_cursor->deleteCurrent();
            }
        }
        else {
            CLowlaDBBson::ptr dataBson = response[i];
            if (loc->m_found) {
                coll->updateDocument(loc->m_cursor.get(), loc->m_sqliteId, dataBson->pimpl().get(), loc->m_found.get(), loc->m_foundMeta.get());
            }
            else {
                coll->insert(dataBson->pimpl().get(), id);
            }
        }
        if (loc->m_cursor) {
            loc->m_cursor->close();
        }
        tx.commit();
        // Clear the id from the todo list
        pullData->eraseAtom(id);
        ++i;
    }
    processLeadingDeletions(pullData.get(), cache);
}

static void collectPushDataForCollection(CLowlaDBCollectionImpl *coll, CLowlaDBPushDataImpl *pd) {
    SqliteCursor::ptr log = coll->openLogCursor();
    std::vector<int64_t> ids;
    int rc, res;
    rc = log->first(&res);
    while (SQLITE_OK == rc && 0 == res) {
        int64_t id;
        rc = log->keySize(&id);
        if (SQLITE_OK == rc) {
            ids.push_back(id);
            rc = log->next(&res);
        }
    }
    pd->registerIds(coll->db()->name() + "." + coll->name(), ids);
}

CLowlaDBPushData::ptr lowladb_collect_push_data() {
    std::shared_ptr<CLowlaDBPushDataImpl> pd(new CLowlaDBPushDataImpl);

    std::vector<utf16string> dbs = SysListFiles();
    for (const utf16string &dbName : dbs) {
        CLowlaDBImpl::ptr db = lowla_db_open(dbName);
        if (!db) {
            continue;
        }
        Tx tx(db->btree());
        std::vector<utf16string> collections;
        db->collectionNames(&collections);
        for (const utf16string &collName : collections) {
            CLowlaDBCollectionImpl::ptr coll = db->createCollection(collName);
            collectPushDataForCollection(coll.get(), pd.get());
        }
    }
    return CLowlaDBPushData::create(pd);
}

CLowlaDBBson::ptr lowladb_create_push_request(CLowlaDBPushData::ptr pd) {
    return pd->pimpl()->request();
}

static void mergeId(CLowlaDBBsonImpl *newMeta, CLowlaDBBsonImpl *oldMeta, const char *id) {
    newMeta->appendString("id", id);
    bson_iterator walk[1];
    bson_iterator_init(walk, oldMeta);
    while (BSON_EOO != bson_iterator_next(walk)) {
        if (0 == strcmp("id", bson_iterator_key(walk))) {
            continue;
        }
        bson_append_element(newMeta, nullptr, walk);
    }
    newMeta->finish();
}

void lowladb_apply_push_response(std::vector<CLowlaDBBson::ptr> &response, CLowlaDBPushData::ptr pd) {
    CLowlaDBPushDataImpl *pushData = pd->pimpl().get();
    CLowlaDBNsCache cache;
    cache.setNotifyOnClose(true);

    int i = 0;
    while (i < response.size()) {
        CLowlaDBBson::ptr metaBson = response[i];
        bool isDeletion = metaBson->boolForKey("deleted", &isDeletion) && isDeletion;
        if (!isDeletion) {
            if (response.size() <= i + 1) {
                // Error - non deletion metadata not followed by document
                break;
            }
            ++i;
        }
        
        const char *ns;
        metaBson->stringForKey("clientNs", &ns);
        CLowlaDBCollectionImpl *coll = cache.collectionForNs(ns);
        if (!coll) {
            ++i;
            continue;
        }
        coll->setWriteLog(false);
        Tx tx(coll->db()->btree());
        
        bool responseChangesId = true;
        const char *id;
        if (!metaBson->stringForKey("clientId", &id)) {
            responseChangesId = false;
            metaBson->stringForKey("id", &id);
        }
        // Make sure the document hasn't changed since we started the pull - ignore if so
        std::unique_ptr<CLowlaDBSyncDocumentLocation> loc = pushData->canAcceptPushResponse(coll, ns, id);
        if (!loc) {
            ++i;
            continue;
        }
        if (isDeletion) {
            if (loc->m_found) {
                loc->m_cursor->deleteCurrent();
            }
        }
        else {
            CLowlaDBBson::ptr dataBson = response[i];
            if (loc->m_found) {
                if (responseChangesId) {
                    const char *newId;
                    metaBson->stringForKey("id", &newId);
                    std::unique_ptr<CLowlaDBBsonImpl> newMeta(new CLowlaDBBsonImpl());
                    mergeId(newMeta.get(), loc->m_foundMeta.get(), newId);
                    loc->m_foundMeta = std::move(newMeta);
               }
                coll->updateDocument(loc->m_cursor.get(), loc->m_sqliteId, dataBson->pimpl().get(), loc->m_found.get(), loc->m_foundMeta.get());
            }
            else {
                coll->insert(dataBson->pimpl().get(), id);
            }
        }
        // The insert may have invalidated the log cursor so re-seek before deleting
        if (loc->m_logCursor) {
            int res;
            loc->m_logCursor->movetoUnpacked(nullptr, loc->m_sqliteId, 0, &res);
            if (0 == res) {
                loc->m_logCursor->deleteCurrent();
            }
            loc->m_logCursor->close();
        }
        if (loc->m_cursor) {
            loc->m_cursor->close();
        }
        tx.commit();
        ++i;
    }
}

static void bson_to_json_value(const char *bsonData, Json::Value *value) {
    bson bsonVal[1];
    bson_init_finished_data(bsonVal, (char *)bsonData, false);
    bson_iterator it[1];
    bson_iterator_init(it, bsonVal);
    
    while (bson_iterator_next(it)) {
        const char *key = bson_iterator_key(it);
        Json::Value *lval;
        if (value->isArray()) {
            lval = &(*value)[atoi(key)];
        }
        else {
            lval = &(*value)[key];
        }
        switch (bson_iterator_type(it)) {
            case BSON_DOUBLE: {
                *lval = bson_iterator_double_raw(it);
                break;
            }
            case BSON_STRING: {
                *lval = bson_iterator_string(it);
                break;
            }
            case BSON_OBJECT: {
                Json::Value obj;
                bson sub[1];
                bson_iterator_subobject_init(it, sub, false);
                bson_to_json_value(bson_data(sub), &obj);
                *lval = obj;
                break;
            }
            case BSON_ARRAY: {
                Json::Value arr(Json::arrayValue);
                bson sub[1];
                bson_iterator_subobject_init(it, sub, false);
                bson_to_json_value(bson_data(sub), &arr);
                *lval = arr;
                break;
            }
            case BSON_BOOL: {
                *lval = (bool)bson_iterator_bool(it);
                break;
            }
            case BSON_INT: {
                *lval = bson_iterator_int_raw(it);
                break;
            }
            case BSON_LONG: {
                *lval = bson_iterator_long_raw(it);
                break;
            }
            case BSON_OID: {
                Json::Value obj;
                obj["_bsonType"] = "ObjectId";
                char buf[CLowlaDBBson::OID_STRING_SIZE];
                CLowlaDBBson::oidToString((char *)bson_iterator_oid(it), buf);
                obj["hexString"] = buf;
                *lval = obj;
            }
        }
    }
}

utf16string lowladb_bson_to_json(const char *bsonData) {
    Json::Value root;
    
    bson_to_json_value(bsonData, &root);
    return root.toStyledString().c_str();
}

static void appendJsonValueToBson(CLowlaDBBsonImpl *bson, const char *key, const Json::Value &value)
{
    switch (value.type()) {
        case Json::nullValue:
            break;
        case Json::intValue:
            bson->appendInt(key, value.asInt());
            break;
        case Json::uintValue:
            bson->appendLong(key, value.asInt64());
            break;
        case Json::realValue:
            bson->appendDouble(key, value.asDouble());
            break;
        case Json::stringValue:
            bson->appendString(key, value.asCString());
            break;
        case Json::booleanValue:
            bson->appendBool(key, value.asBool());
            break;
        case Json::arrayValue:
            bson->startArray(key);
            for (Json::Value::iterator it = value.begin() ; it != value.end() ; ++it) {
                appendJsonValueToBson(bson, utf16string::valueOf((int)it.index()).c_str(), *it);
            }
            bson->finishArray();
            break;
        case Json::objectValue:
            const Json::Value bsonType = value["_bsonType"];
            if (!!bsonType && bsonType.type() == Json::stringValue) {
                if (bsonType.asString() == "Date") {
                    bson->appendDate(key, value["millis"].asInt64());
                }
                else if (bsonType.asString() == "ObjectId") {
                    const char *str = value["hexString"].asString().c_str();
                    if (24 != strlen(str)) {
                        throw TeamstudioException("Invalid ObjectId string: " + utf16string(str));
                    }
                    bson_oid_t oid;
                    bson_oid_from_string(&oid, str);
                    bson->appendOid(key, &oid);
                }
            }
            else {
                bson->startObject(key);
                for (Json::Value::iterator it = value.begin() ; it != value.end() ; ++it) {
                    appendJsonValueToBson(bson, it.memberName(), *it);
                }
                bson->finishObject();
            }
            break;
    }
}

static CLowlaDBBson::ptr jsonValue_to_bson(const Json::Value &value) {
    CLowlaDBBson::ptr answer = CLowlaDBBson::create();
    for (Json::Value::iterator it = value.begin() ; it != value.end() ; ++it) {
        appendJsonValueToBson(answer->pimpl().get(), it.memberName(), *it);
    }
    answer->finish();
    return answer;
}

CLowlaDBBson::ptr lowladb_json_to_bson(const char *json) {
    return lowladb_json_to_bson(json, strlen(json));
}

CLowlaDBBson::ptr lowladb_json_to_bson(const char *json, size_t length) {
    Json::Value root;
    Json::Reader reader;
    if (reader.parse(json, json + length, root, false)) {
        return jsonValue_to_bson(root);
    }
    return CLowlaDBBson::ptr();
}

void lowladb_load_json(const char *json) {
    Json::Value root;
    Json::Reader reader;
    if (!reader.parse(json, root, false)) {
        return;
    }
    
    // Create an empty pull data - required by process_pull_response
    CLowlaDBPullData::ptr pd = lowladb_parse_syncer_response(CLowlaDBBsonImpl::empty()->data());
    Json::Value documents = root["documents"];
    if (!documents.isArray()) {
        return;
    }
    for (Json::ArrayIndex i = 0 ; i < documents.size(); ++i) {
        Json::Value chunk = documents[i];
        std::vector<CLowlaDBBson::ptr> bsonChunk;
        for (Json::ArrayIndex j = 0 ; j < chunk.size() ; ++j) {
            bsonChunk.push_back(jsonValue_to_bson(chunk[j]));
        }
        lowladb_apply_pull_response(bsonChunk, pd);
    }
}

void lowladb_apply_json_pull_response(const char *json, CLowlaDBPullData::ptr pd) {
    Json::Value root;
    Json::Reader reader;
    if (!reader.parse(json, root, false)) {
        return;
    }
    
    if (!root.isArray()) {
        return;
    }
    std::vector<CLowlaDBBson::ptr> bsonChunk;
    for (Json::ArrayIndex j = 0 ; j < root.size() ; ++j) {
        bsonChunk.push_back(jsonValue_to_bson(root[j]));
    }
    lowladb_apply_pull_response(bsonChunk, pd);
}

void lowladb_apply_json_push_response(const char *json, CLowlaDBPushData::ptr pd) {
    Json::Value root;
    Json::Reader reader;
    if (!reader.parse(json, root, false)) {
        return;
    }
    
    if (!root.isArray()) {
        return;
    }
    std::vector<CLowlaDBBson::ptr> bsonChunk;
    for (Json::ArrayIndex j = 0 ; j < root.size() ; ++j) {
        bsonChunk.push_back(jsonValue_to_bson(root[j]));
    }
    lowladb_apply_push_response(bsonChunk, pd);
}

CLowlaDBCollectionListenerImpl *CLowlaDBCollectionListenerImpl::instance()
{
    static CLowlaDBCollectionListenerImpl impl;
    return &impl;
}

void CLowlaDBCollectionListenerImpl::addListener(LowlaDbCollectionListener listener, void *user)
{
    m_listeners.push_back(std::make_pair(listener, user));
}

void CLowlaDBCollectionListenerImpl::removeListener(LowlaDbCollectionListener listener)
{
    auto walk = m_listeners.begin();
    while (walk != m_listeners.end()) {
        if (walk->first == listener) {
            walk = m_listeners.erase(walk);
        }
        else {
            ++walk;
        }
    }
}

void CLowlaDBCollectionListenerImpl::notifyListeners(const char *ns)
{
    for (const Entry &e : m_listeners) {
        (e.first)(e.second, ns);
    }
}

void lowladb_add_collection_listener(LowlaDbCollectionListener listener, void *user) {
    CLowlaDBCollectionListenerImpl::instance()->addListener(listener, user);
}

void lowladb_remove_collection_listener(LowlaDbCollectionListener listener) {
    CLowlaDBCollectionListenerImpl::instance()->removeListener(listener);
}
