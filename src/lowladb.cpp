#include "algorithm"
#include "cstdio"
#include "set"

#include "bson/bson.h"
#include "integration.h"
#include "SqliteCursor.h"
#include "TeamstudioException.h"
#include "utf16stringbuilder.h"
#include "json/json.h"
#include "lowladb.h"

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

class CLowlaDBNsCache {
public:
    CLowlaDBCollectionImpl *collectionForNs(const char *ns);
    
private:
    std::map<utf16string, std::unique_ptr<CLowlaDBImpl>> m_dbCache;
    std::map<utf16string, std::unique_ptr<CLowlaDBCollectionImpl>> m_collCache;
    typedef std::map<utf16string, std::unique_ptr<CLowlaDBImpl>>::iterator db_iterator;
    typedef std::map<utf16string, std::unique_ptr<CLowlaDBCollectionImpl>>::iterator coll_iterator;
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
    typedef std::vector<CLowlaDBBsonImpl>::iterator atomIterator;
    
    CLowlaDBPullDataImpl();
    void appendAtom(const char *atomBson);
    void setSequence(int sequence);
    atomIterator atomsBegin();
    atomIterator atomsEnd();
    atomIterator eraseAtom(atomIterator walk);
    void eraseAtom(const utf16string &id);
    void setRequestMore(const utf16string &requestMore);
    bool hasRequestMore();
    utf16string getRequestMore();

    bool isComplete();
    int getSequenceForNextRequest();
    
private:
    std::vector<CLowlaDBBsonImpl> m_atoms;
    int m_sequence;
    int m_processedSequence;
    utf16string m_requestMore;
};

class CLowlaDBImpl {
public:
    CLowlaDBImpl(sqlite3 *pDb);
    ~CLowlaDBImpl();
    
    std::unique_ptr<CLowlaDBCollectionImpl> createCollection(const utf16string &name);
    void collectionNames(std::vector<utf16string> *plstNames);
    
    SqliteCursor::ptr openCursor(int root);
    Btree *btree();

private:
    sqlite3 *m_pDb;
};

class CLowlaDBSyncManagerImpl {
public:
    CLowlaDBSyncManagerImpl();
    
    void setNsHasOutgoingChanges(const utf16string &ns, bool hasOutgoingChanges);
    bool hasOutgoingChanges();
    utf16string nsWithChanges();
    void setWriteCallback(LowlaDBWriteCallback wcb);
    void runWriteCallback();
    
private:
    std::set<utf16string> m_nsWithChanges;
    LowlaDBWriteCallback m_wcb;
    sqlite3_mutex *m_mutex;
};

class CLowlaDBWriteResultImpl {
public:
    CLowlaDBWriteResultImpl();
    int getDocumentCount();
    const char *getDocument(int n);
    
    void appendDocument(std::unique_ptr<CLowlaDBBsonImpl> doc);
    
private:
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
    
    bool containsKey(const utf16string &key);
    void appendDouble(const utf16string &key, double value);
    void appendString(const utf16string &key, const utf16string &value);
    void appendObject(const utf16string &key, const char *value);
    void appendOid(const utf16string &key, const bson_oid_t *oid);
    void appendBool(const utf16string &key, bool value);
    void appendDate(const utf16string &key, int64_t value);
    void appendInt(const utf16string &key, int value);
    void appendLong(const utf16string &key, int64_t value);
    void appendAll(CLowlaDBBsonImpl *bson);
    void appendElement(const CLowlaDBBsonImpl *src, const utf16string &key);
    
    void startArray(const utf16string &key);
    void finishArray();
    void startObject(const utf16string &key);
    void finishObject();
    
    bool doubleForKey(const utf16string &key, double *ret);
    utf16string stringForKey(const utf16string &key) const;
    bool objectForKey(const utf16string &key, const char **ret);
    bool arrayForKey(const utf16string &key, const char **ret);
    bool oidForKey(const utf16string &key, bson_oid_t *ret);
    bool boolForKey(const utf16string &key, bool *ret) const;
    bool dateForKey(const utf16string &key, bson_date_t *ret);
    bool intForKey(const utf16string &key, int *ret);
    bool longForKey(const utf16string &key, int64_t *ret);
    
    const char *valueForKey(const char *key, long *count);
    
    const char *data();
    size_t size();
    
    void finish();
};

class CLowlaDBCollectionImpl {
public:
    CLowlaDBCollectionImpl(CLowlaDBImpl *db, const utf16string &name, int root, int logRoot);
    std::unique_ptr<CLowlaDBWriteResultImpl> insert(CLowlaDBBsonImpl *obj, const utf16string &lowlaId);
    std::unique_ptr<CLowlaDBWriteResultImpl> insert(std::vector<CLowlaDBBsonImpl> &arr);
    std::unique_ptr<CLowlaDBWriteResultImpl> save(CLowlaDBBsonImpl *obj);
    std::unique_ptr<CLowlaDBWriteResultImpl> update(CLowlaDBBsonImpl *query, CLowlaDBBsonImpl *object, bool upsert, bool multi);
    
    SqliteCursor::ptr openCursor();
    CLowlaDBImpl *db();
    bool shouldPullDocument(const CLowlaDBBsonImpl *atom);
    std::unique_ptr<CLowlaDBSyncDocumentLocation> locateDocumentForId(const utf16string &id);
    utf16string getName();

    void updateDocument(SqliteCursor *cursor, int64_t id, CLowlaDBBsonImpl *obj, CLowlaDBBsonImpl *oldObj, CLowlaDBBsonImpl *oldMeta);

private:
    bool isReplaceObject(CLowlaDBBsonImpl *update);
    std::unique_ptr<CLowlaDBBsonImpl> applyUpdate(CLowlaDBBsonImpl *update, CLowlaDBBsonImpl *original);
    
    enum {
        LOGTYPE_INSERT = 1,
        LOGTYPE_UPDATE,
        LOGTYPE_REMOVE
    };
    
    CLowlaDBImpl *m_db;
    int m_root;
    int m_logRoot;
    utf16string m_name;
};

static std::unique_ptr<CLowlaDBImpl> lowla_db_open(const utf16string &name);

class Tx
{
public:
    Tx(Btree *pBt, bool readOnly);
    ~Tx();
    
    void commit();
    void detach();
    
private:
    static const int TRANS_READONLY = 0;
    static const int TRANS_READWRITE = 1;
    
    Btree *m_pBt;
    bool m_ownTx;
};

class CLowlaDBCursorImpl {
public:
    CLowlaDBCursorImpl(CLowlaDBCollectionImpl *coll, std::shared_ptr<CLowlaDBBsonImpl> query, std::shared_ptr<CLowlaDBBsonImpl> keys);
    CLowlaDBCursorImpl(const CLowlaDBCursorImpl &other);
    
    std::unique_ptr<CLowlaDBCursorImpl> limit(int limit);
    std::unique_ptr<CLowlaDBCursorImpl> skip(int skip);
    std::unique_ptr<CLowlaDBCursorImpl> sort(std::shared_ptr<CLowlaDBBsonImpl> sort);
    
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
    std::unique_ptr<Tx> m_tx;
    SqliteCursor::ptr m_cursor;
    
    CLowlaDBCollectionImpl *m_coll;
    std::shared_ptr<CLowlaDBBsonImpl> m_query;
    std::shared_ptr<CLowlaDBBsonImpl> m_keys;
    std::shared_ptr<CLowlaDBBsonImpl> m_sort;
    std::vector<std::pair<std::vector<utf16string>, int>> m_parsedSort;
    
    int m_unsortedOffset;
    std::vector<int64_t> m_sortedIds;
    std::vector<int64_t>::iterator m_walkSorted;
    
    int m_limit;
    int m_skip;
    bool m_showDiskLoc;
};

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
    int rc = SQLITE_OK;
    if (readOnly) {
        if (sqlite3BtreeIsInReadTrans(m_pBt)) {
            m_ownTx = false;
        }
        else {
            rc = beginTransWithRetry(m_pBt, TRANS_READONLY);
            m_ownTx = true;
        }
    }
    else if (sqlite3BtreeIsInTrans(m_pBt)) {
        m_ownTx = false;
    }
    else {
        rc = beginTransWithRetry(m_pBt, TRANS_READWRITE);
        m_ownTx = true;
    }
    if (SQLITE_OK != rc) {
        sqlite3_mutex_leave(pBt->db->mutex);
        // TODO report somehow
    }
}

Tx::~Tx()
{
    if (m_ownTx) {
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

CLowlaDB::ptr CLowlaDB::create(std::unique_ptr<CLowlaDBImpl> &pimpl) {
    return CLowlaDB::ptr(new CLowlaDB(pimpl));
}

CLowlaDB::CLowlaDB(std::unique_ptr<CLowlaDBImpl> &pimpl) : m_pimpl(std::move(pimpl)) {
}

CLowlaDBImpl::CLowlaDBImpl(sqlite3 *pDb) : m_pDb(pDb) {
}

CLowlaDBImpl::~CLowlaDBImpl() {
    sqlite3_close(m_pDb);
}

static std::unique_ptr<CLowlaDBImpl> lowla_db_open(const utf16string &name) {
    static sqlite3_mutex *mutex = sqlite3_mutex_alloc(SQLITE_MUTEX_RECURSIVE);
    sqlite3_mutex_enter(mutex);
    
    utf16string filePath = getFullPath(name);
    sqlite3 *pDb;
    int rc = sqlite3_open_v2(filePath.c_str(), &pDb, SQLITE_OPEN_READWRITE, 0);
    if (SQLITE_OK == rc) {
        std::unique_ptr<CLowlaDBImpl> pimpl(new CLowlaDBImpl(pDb));
        sqlite3_mutex_leave(mutex);
        return pimpl;
    }
    rc = createDatabase(filePath);
    if (SQLITE_OK == rc) {
        rc = sqlite3_open_v2(filePath.c_str(), &pDb, SQLITE_OPEN_READWRITE, 0);
    }
    if (SQLITE_OK == rc) {
        std::unique_ptr<CLowlaDBImpl> pimpl(new CLowlaDBImpl(pDb));
        sqlite3_mutex_leave(mutex);
        return pimpl;
    }
    sqlite3_mutex_leave(mutex);
    return std::unique_ptr<CLowlaDBImpl>();
}

CLowlaDB::ptr CLowlaDB::open(const utf16string &name) {
    std::unique_ptr<CLowlaDBImpl> pimpl = lowla_db_open(name);
    if (pimpl) {
        return CLowlaDB::create(pimpl);
    }
    return CLowlaDB::ptr();
}

CLowlaDBCollection::ptr CLowlaDB::createCollection(const utf16string &name) {
    std::unique_ptr<CLowlaDBCollectionImpl> pimpl = m_pimpl->createCollection(name);
    return CLowlaDBCollection::create(pimpl);
}

void CLowlaDB::collectionNames(std::vector<utf16string> *plstNames) {
    m_pimpl->collectionNames(plstNames);
}

std::unique_ptr<CLowlaDBCollectionImpl> CLowlaDBImpl::createCollection(const utf16string &name) {
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
            bson_destroy(data);
            
            if (0 == strcmp(collName, foundName) && -1 != collRoot && -1 != collLogRoot) {
                headerCursor.close();
                return std::unique_ptr<CLowlaDBCollectionImpl>(new CLowlaDBCollectionImpl(this, name, collRoot, collLogRoot));
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
    
    bson data[1];
    bson_init(data);
    
    bson_append_string(data, "collName", collName);
    bson_append_int(data, "collRoot", collRoot);
    bson_append_int(data, "collLogRoot", collLogRoot);
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
    return std::unique_ptr<CLowlaDBCollectionImpl>(new CLowlaDBCollectionImpl(this, name, collRoot, collLogRoot));
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

CLowlaDBCollection::ptr CLowlaDBCollection::create(std::unique_ptr<CLowlaDBCollectionImpl> &pimpl) {
    return CLowlaDBCollection::ptr(new CLowlaDBCollection(pimpl));
}

CLowlaDBCollectionImpl *CLowlaDBCollection::pimpl() {
    return m_pimpl.get();
}

CLowlaDBCollection::CLowlaDBCollection(std::unique_ptr<CLowlaDBCollectionImpl> &pimpl) : m_pimpl(std::move(pimpl)) {
}

CLowlaDBWriteResult::ptr CLowlaDBCollection::insert(const char *bsonData) {
    CLowlaDBBsonImpl bson(bsonData, CLowlaDBBsonImpl::REF);
    std::unique_ptr<CLowlaDBWriteResultImpl> pimpl = m_pimpl->insert(&bson, "");
    return CLowlaDBWriteResult::create(pimpl);
}

CLowlaDBWriteResult::ptr CLowlaDBCollection::insert(const std::vector<const char *> &bsonData) {
    std::vector<CLowlaDBBsonImpl> bsonArr;
    for (const char *bson : bsonData) {
        bsonArr.emplace_back(bson, CLowlaDBBsonImpl::REF);
    }
    std::unique_ptr<CLowlaDBWriteResultImpl> pimpl = m_pimpl->insert(bsonArr);
    return CLowlaDBWriteResult::create(pimpl);
}

CLowlaDBWriteResult::ptr CLowlaDBCollection::save(const char *bsonData) {
    CLowlaDBBsonImpl bson(bsonData, CLowlaDBBsonImpl::REF);
    std::unique_ptr<CLowlaDBWriteResultImpl> pimpl = m_pimpl->save(&bson);
    return CLowlaDBWriteResult::create(pimpl);
}

CLowlaDBWriteResult::ptr CLowlaDBCollection::update(const char *queryBson, const char *objectBson, bool upsert, bool multi) {
    CLowlaDBBsonImpl query(queryBson, CLowlaDBBsonImpl::REF);
    CLowlaDBBsonImpl object(objectBson, CLowlaDBBsonImpl::REF);
    std::unique_ptr<CLowlaDBWriteResultImpl> pimpl = m_pimpl->update(&query, &object, upsert, multi);
    return CLowlaDBWriteResult::create(pimpl);
}

static utf16string generateLowlaId(CLowlaDBCollectionImpl *coll, CLowlaDBBsonImpl *obj) {
    return coll->getName() + "$" + obj->stringForKey("_id");
}

CLowlaDBCollectionImpl::CLowlaDBCollectionImpl(CLowlaDBImpl *db, const utf16string &name, int root, int logRoot) : m_db(db), m_name(name), m_root(root), m_logRoot(logRoot) {
}

static void throwIfDocumentInvalidForInsertion(CLowlaDBBsonImpl const *obj) {
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

std::unique_ptr<CLowlaDBWriteResultImpl> CLowlaDBCollectionImpl::insert(CLowlaDBBsonImpl *obj, const utf16string &lowlaId) {
    
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
    if (lowlaId.isEmpty()) {
        meta.appendString("id", lowlaId);
    }
    else {
        meta.appendString("id", generateLowlaId(this, obj));
    }
    meta.finish();
    rc = cursor->insert(NULL, newId, obj->data(), (int)obj->size(), (int)meta.size(), true, 0);
    if (SQLITE_OK == rc) {
        rc = cursor->movetoUnpacked(nullptr, newId, 0, &res);
        cursor->putData((int)obj->size(), (int)meta.size(), meta.data());
        SqliteCursor::ptr logCursor = m_db->openCursor(m_logRoot);
        static char logData[] = {0, 0, 0, 0, LOGTYPE_INSERT};
        rc = logCursor->insert(NULL, newId, logData, sizeof(logData), 0, true, 0);
    }
    if (obj->ownsData) {
        obj->ownsData = false;
        answer->appendDocument(std::unique_ptr<CLowlaDBBsonImpl>(new CLowlaDBBsonImpl(obj->data(), CLowlaDBBsonImpl::OWN)));
    }
    else {
        answer->appendDocument(std::unique_ptr<CLowlaDBBsonImpl>(new CLowlaDBBsonImpl(obj->data(), CLowlaDBBsonImpl::REF)));
    }
    
    rc = cursor->close();
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
        meta.appendString("id", generateLowlaId(this, obj));
        meta.finish();
        rc = cursor->insert(NULL, newId, obj->data(), (int)obj->size(), (int)meta.size(), true, 0);
        if (SQLITE_OK == rc) {
            rc = cursor->movetoUnpacked(nullptr, newId, 0, &res);
            cursor->putData((int)obj->size(), (int)meta.size(), meta.data());
            static char logData[] = {0, 0, 0, 0, LOGTYPE_INSERT};
            rc = logCursor->insert(NULL, newId, logData, sizeof(logData), 0, true, 0);
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
    tx.commit();
    
    return answer;
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

    Tx tx(m_db->btree());

    // The cursor needs a shared_ptr so we create a new ClowlaDBBsonImpl using the incoming data
    std::shared_ptr<CLowlaDBBsonImpl> cursorQuery(new CLowlaDBBsonImpl(query->data(), CLowlaDBBsonImpl::REF));
    std::unique_ptr<CLowlaDBCursorImpl> cursor(new CLowlaDBCursorImpl(this, cursorQuery, nullptr));
    if (!multi) {
        cursor = cursor->limit(1);
    }
    
    std::unique_ptr<CLowlaDBBsonImpl> found = cursor->next();
    if (!found && upsert) {
        return insert(object, "");
    }
    int count = 0;
    while (found) {
        ++count;
        int64_t id = cursor->currentId();
        std::unique_ptr<CLowlaDBBsonImpl> bsonToWrite = applyUpdate(object, found.get());
        updateDocument(cursor->sqliteCursor().get(), id, bsonToWrite.get(), found.get(), cursor->currentMeta().get());
        
        found = cursor->next();
    }
    cursor.reset();
    tx.commit();
    std::unique_ptr<CLowlaDBWriteResultImpl> wr(new CLowlaDBWriteResultImpl);
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
        std::unique_ptr<CLowlaDBBsonImpl> set;
        std::unique_ptr<CLowlaDBBsonImpl> unset;
        if (update->objectForKey("$set", &tmp)) {
            set.reset(new CLowlaDBBsonImpl(tmp, CLowlaDBBsonImpl::REF));
        }
        else {
            set.reset(new CLowlaDBBsonImpl);
        }
        if (update->objectForKey("$unset", &tmp)) {
            unset.reset(new CLowlaDBBsonImpl(tmp, CLowlaDBBsonImpl::REF));
        }
        else {
            unset.reset(new CLowlaDBBsonImpl);
        }
        answer->appendElement(original, "_id");
        bson_iterator it[1];
        bson_iterator_init(it, update);
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
            if (!answer->containsKey(key)) {
                bson_append_element(answer.get(), nullptr, it);
            }
        }
        answer->finish();
    }
    return answer;
}

void CLowlaDBCollectionImpl::updateDocument(SqliteCursor *cursor, int64_t id, CLowlaDBBsonImpl *obj, CLowlaDBBsonImpl *oldObj, CLowlaDBBsonImpl *oldMeta) {
    int rc = cursor->insert(NULL, id, obj->data(), (int)obj->size(), (int)oldMeta->size(), false, 0);
    if (SQLITE_OK == rc) {
        rc = cursor->putData((int)obj->size(), (int)oldMeta->size(), oldMeta->data());
    }
    if (SQLITE_OK == rc) {
        SqliteCursor::ptr logCursor = m_db->openCursor(m_logRoot);
        int res;
        rc = logCursor->movetoUnpacked(nullptr, id, 0, &res);
        if (SQLITE_OK == rc && 0 != res) {
            logCursor->insert(nullptr, id, oldObj->data(), (int)oldObj->size(), 0, false, res);
        }
    }
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

utf16string CLowlaDBCollectionImpl::getName() {
    return m_name;
}

CLowlaDBImpl *CLowlaDBCollectionImpl::db() {
    return m_db;
}

std::unique_ptr<CLowlaDBSyncDocumentLocation> CLowlaDBCollectionImpl::locateDocumentForId(const utf16string &id) {
    std::unique_ptr<CLowlaDBSyncDocumentLocation> answer(new CLowlaDBSyncDocumentLocation);

    // Locate the document by its lowla key
    Tx tx(db()->btree());
    SqliteCursor::ptr cursor = openCursor();
    answer->m_cursor = cursor;
    int res;
    int rc = cursor->first(&res);
    while (SQLITE_OK == rc && 0 ==res) {
        u32 dataSize;
        cursor->dataSize(&dataSize);
        int objSize = 0;
        int cbRequired = 4;
        bson_little_endian32(&objSize, cursor->dataFetch(&cbRequired));
        int metaSize = dataSize - objSize;
        char meta[metaSize];
        cursor->data(objSize, metaSize, meta);
        CLowlaDBBsonImpl metaBson(meta, CLowlaDBBsonImpl::REF);
        utf16string foundKey = metaBson.stringForKey("id");
        if (foundKey == id) {
            char *data = (char *)bson_malloc(objSize);
            cursor->data(0, objSize, data);
            answer->m_found.reset(new CLowlaDBBsonImpl(data, CLowlaDBBsonImpl::OWN));
            // Copy the metaBson into malloced data so we can return it safely. We only do this once
            // we've found the match to avoid lots of slow heap access.
            char *meta = (char *)bson_malloc(metaSize);
            memcpy(meta, metaBson.data(), metaBson.size());
            answer->m_foundMeta.reset(new CLowlaDBBsonImpl(meta, CLowlaDBBsonImpl::OWN));
            cursor->keySize(&answer->m_sqliteId);
            answer->m_logCursor = m_db->openCursor(m_logRoot);
            int resLog;
            int rcLog = answer->m_logCursor->movetoUnpacked(nullptr, answer->m_sqliteId, 0, &resLog);
            answer->m_logFound = (SQLITE_OK == rcLog && 0 == resLog);
            return answer;
        }
        rc = cursor->next(&res);
    }
    answer->m_logFound = false;
    return answer;
}

bool CLowlaDBCollectionImpl::shouldPullDocument(const CLowlaDBBsonImpl *atom) {
    
    Tx tx(m_db->btree());
    
    std::unique_ptr<CLowlaDBSyncDocumentLocation> loc = locateDocumentForId(atom->stringForKey("id"));

    // If found
    if (loc->m_found) {
        // See if there's an outgoing record in the log. If so, don't pull
        if (loc->m_logFound) {
            return false;
        }
        // If the found record has the same version then don't want to pull
        const char *_lowlaObj;
        if (loc->m_found->objectForKey("_lowla", &_lowlaObj)) {
            CLowlaDBBsonImpl _lowla(_lowlaObj, CLowlaDBBsonImpl::REF);
            utf16string foundVersion = _lowla.stringForKey("version");
            utf16string atomVersion = atom->stringForKey("version");
            if (foundVersion == atomVersion) {
                return false;
            }
        }
        // Good to pull
        return true;
    }
    // So it wasn't found. There may be an outgoing deletion, but we have no easy way to find it.
    // This is rare, so we can allow the pull to go ahead and then when the push gets processed the
    // adapter will sort out the conflict
    return true;
}

CLowlaDBWriteResult::ptr CLowlaDBWriteResult::create(std::unique_ptr<CLowlaDBWriteResultImpl> &pimpl) {
    return CLowlaDBWriteResult::ptr(new CLowlaDBWriteResult(pimpl));
}

int CLowlaDBWriteResult::documentCount() {
    return m_pimpl->getDocumentCount();
}

CLowlaDBBson::ptr CLowlaDBWriteResult::document(int n) {
    return CLowlaDBBson::create(m_pimpl->getDocument(n), false);
}

CLowlaDBWriteResult::CLowlaDBWriteResult(std::unique_ptr<CLowlaDBWriteResultImpl> &pimpl) : m_pimpl(std::move(pimpl)) {
}

CLowlaDBWriteResultImpl::CLowlaDBWriteResultImpl() {
}

int CLowlaDBWriteResultImpl::getDocumentCount() {
    return (int)m_docs.size();
}

const char *CLowlaDBWriteResultImpl::getDocument(int n) {
    return m_docs[n]->data();
}

void CLowlaDBWriteResultImpl::appendDocument(std::unique_ptr<CLowlaDBBsonImpl> doc) {
    m_docs.push_back(std::move(doc));
}

const size_t CLowlaDBBson::OID_SIZE = sizeof(bson_oid_t);
const size_t CLowlaDBBson::OID_STRING_SIZE = 2 * OID_SIZE + 1;

CLowlaDBBson::ptr CLowlaDBBson::create() {
    std::unique_ptr<CLowlaDBBsonImpl> pimpl(new CLowlaDBBsonImpl);
    return CLowlaDBBson::ptr(new CLowlaDBBson(pimpl));
}

CLowlaDBBson::ptr CLowlaDBBson::create(std::unique_ptr<CLowlaDBBsonImpl> &pimpl) {
    if (pimpl) {
        return CLowlaDBBson::ptr(new CLowlaDBBson(pimpl));
    }
    else {
        return CLowlaDBBson::ptr();
    }
}

CLowlaDBBson::ptr CLowlaDBBson::create(const char *bson, bool ownsData) {
    CLowlaDBBsonImpl::Mode mode = ownsData ? CLowlaDBBsonImpl::OWN : CLowlaDBBsonImpl::REF;
    std::unique_ptr<CLowlaDBBsonImpl> pimpl(new CLowlaDBBsonImpl(bson, mode));
    return CLowlaDBBson::ptr(new CLowlaDBBson(pimpl));
}

CLowlaDBBsonImpl *CLowlaDBBson::pimpl() {
    return m_pimpl.get();
}

void CLowlaDBBson::appendDouble(const utf16string &key, double value) {
    m_pimpl->appendDouble(key, value);
}

void CLowlaDBBson::appendString(const utf16string &key, const utf16string &value) {
    m_pimpl->appendString(key, value);
}

void CLowlaDBBson::appendObject(const utf16string &key, const char *value) {
    m_pimpl->appendObject(key, value);
}

void CLowlaDBBson::appendOid(const utf16string &key, const void *value) {
    m_pimpl->appendOid(key, (const bson_oid_t *)value);
}

void CLowlaDBBson::appendBool(const utf16string &key, bool value) {
    m_pimpl->appendBool(key, value);
}

void CLowlaDBBson::appendDate(const utf16string &key, int64_t value) {
    m_pimpl->appendDate(key, value);
}

void CLowlaDBBson::appendInt(const utf16string &key, int value) {
    m_pimpl->appendInt(key, value);
}

void CLowlaDBBson::appendLong(const utf16string &key, int64_t value) {
    m_pimpl->appendLong(key, value);
}

void CLowlaDBBson::startArray(const utf16string &key) {
    m_pimpl->startArray(key);
}

void CLowlaDBBson::finishArray() {
    m_pimpl->finishArray();
}

void CLowlaDBBson::startObject(const utf16string &key) {
    m_pimpl->startObject(key);
}

void CLowlaDBBson::finishObject() {
    m_pimpl->finishObject();
}

void CLowlaDBBson::finish() {
    m_pimpl->finish();
}

bool CLowlaDBBson::containsKey(const utf16string &key) {
    return m_pimpl->containsKey(key);
}

bool CLowlaDBBson::doubleForKey(const utf16string &key, double *ret) {
    return m_pimpl->doubleForKey(key, ret);
}

utf16string CLowlaDBBson::stringForKey(const utf16string &key) {
    return m_pimpl->stringForKey(key);
}

bool CLowlaDBBson::objectForKey(const utf16string &key, CLowlaDBBson::ptr *ret) {
    const char *objBson;
    if (m_pimpl->objectForKey(key, &objBson)) {
        *ret = create(objBson, false);
        return true;
    }
    return false;
}

bool CLowlaDBBson::arrayForKey(const utf16string &key, CLowlaDBBson::ptr *ret) {
    const char *objBson;
    if (m_pimpl->arrayForKey(key, &objBson)) {
        *ret = create(objBson, false);
        return true;
    }
    return false;
}

bool CLowlaDBBson::oidForKey(const utf16string &key, char *ret) {
    return m_pimpl->oidForKey(key, (bson_oid_t *)ret);
}

bool CLowlaDBBson::boolForKey(const utf16string &key, bool *ret) {
    return m_pimpl->boolForKey(key, ret);
}

bool CLowlaDBBson::dateForKey(const utf16string &key, int64_t *ret) {
    return m_pimpl->dateForKey(key, ret);
}

bool CLowlaDBBson::intForKey(const utf16string &key, int *ret) {
    return m_pimpl->intForKey(key, ret);
}

bool CLowlaDBBson::longForKey(const utf16string &key, int64_t *ret) {
    return m_pimpl->longForKey(key, ret);
}

void CLowlaDBBson::oidToString(const char *oid, char *buffer) {
    bson_oid_to_string((const bson_oid_t *)oid, buffer);
}

void CLowlaDBBson::oidGenerate(char *buffer) {
    bson_oid_gen((bson_oid_t *)buffer);
}

const char *CLowlaDBBson::data() {
    return m_pimpl->data();
}

size_t CLowlaDBBson::size() {
    return m_pimpl->size();
}

CLowlaDBBson::CLowlaDBBson(std::unique_ptr<CLowlaDBBsonImpl> &pimpl) : m_pimpl(std::move(pimpl)) {
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

const char *CLowlaDBBsonImpl::data() {
    if (!finished) {
        throw TeamstudioException("Attempt to access data from unfinished bson");
    }
    return bson_data(this);
}

size_t CLowlaDBBsonImpl::size() {
    return bson_size(this);
}

void CLowlaDBBsonImpl::finish() {
    bson_finish(this);
}

bool CLowlaDBBsonImpl::containsKey(const utf16string &key) {
    bson_iterator it[1];
    bson_type type = bson_find(it, this, key.c_str(utf16string::UTF8));
    return BSON_EOO != type;
}

void CLowlaDBBsonImpl::appendDouble(const utf16string &key, double value) {
    bson_append_double(this, key.c_str(utf16string::UTF8), value);
}

void CLowlaDBBsonImpl::appendString(const utf16string &key, const utf16string &value) {
    bson_append_string(this, key.c_str(utf16string::UTF8), value.c_str(utf16string::UTF8));
}

void CLowlaDBBsonImpl::appendObject(const utf16string &key, const char *value) {
    bson_append_start_object(this, key.c_str(utf16string::UTF8));
    CLowlaDBBsonImpl obj(value, CLowlaDBBsonImpl::REF);
    appendAll(&obj);
    bson_append_finish_object(this);
}

void CLowlaDBBsonImpl::appendOid(const utf16string &key, const bson_oid_t *oid) {
    bson_append_oid(this, key.c_str(utf16string::UTF8), oid);
}

void CLowlaDBBsonImpl::appendBool(const utf16string &key, bool value) {
    bson_append_bool(this, key.c_str(utf16string::UTF8), value);
}

void CLowlaDBBsonImpl::appendDate(const utf16string &key, int64_t value) {
    bson_append_date(this, key.c_str(utf16string::UTF8), value);
}

void CLowlaDBBsonImpl::appendInt(const utf16string &key, int value) {
    bson_append_int(this, key.c_str(utf16string::UTF8), value);
}

void CLowlaDBBsonImpl::appendLong(const utf16string &key, int64_t value) {
    bson_append_long(this, key.c_str(utf16string::UTF8), value);
}

void CLowlaDBBsonImpl::appendAll(CLowlaDBBsonImpl *bson) {
    bson_iterator it[1];
    bson_iterator_init(it, bson);
    while (bson_iterator_next(it)) {
        bson_append_element(this, nullptr, it);
    }
}

void CLowlaDBBsonImpl::appendElement(const CLowlaDBBsonImpl *src, const utf16string &key) {
    bson_iterator it[1];
    bson_type type = bson_find(it, src, key.c_str(utf16string::UTF8));
    if (BSON_EOO != type) {
        bson_append_element(this, nullptr, it);
    }
}

void CLowlaDBBsonImpl::startArray(const utf16string &key) {
    bson_append_start_array(this, key.c_str(utf16string::UTF8));
}

void CLowlaDBBsonImpl::finishArray() {
    bson_append_finish_array(this);
}

void CLowlaDBBsonImpl::startObject(const utf16string &key) {
    bson_append_start_object(this, key.c_str(utf16string::UTF8));
}

void CLowlaDBBsonImpl::finishObject() {
    bson_append_finish_object(this);
}

bool CLowlaDBBsonImpl::doubleForKey(const utf16string &key, double *ret) {
    bson_iterator it[1];
    bson_type type = bson_find(it, this, key.c_str(utf16string::UTF8));
    if (BSON_DOUBLE == type) {
        *ret = bson_iterator_double(it);
        return true;
    }
    return false;
}

utf16string CLowlaDBBsonImpl::stringForKey(const utf16string &key) const {
    bson_iterator it[1];
    bson_type type = bson_find(it, this, key.c_str(utf16string::UTF8));
    if (BSON_STRING == type) {
        return utf16string(bson_iterator_string(it), utf16string::UTF8);
    }
    return "";
}

bool CLowlaDBBsonImpl::objectForKey(const utf16string &key, const char **ret) {
    bson_iterator it[1];
    bson_type type = bson_find(it, this, key.c_str(utf16string::UTF8));
    if (BSON_OBJECT == type) {
        bson sub[1];
        bson_iterator_subobject_init(it, sub, false);
        *ret = bson_data(sub);
        return true;
    }
    return false;
}

bool CLowlaDBBsonImpl::arrayForKey(const utf16string &key, const char **ret) {
    bson_iterator it[1];
    bson_type type = bson_find(it, this, key.c_str(utf16string::UTF8));
    if (BSON_ARRAY == type) {
        bson sub[1];
        bson_iterator_subobject_init(it, sub, false);
        *ret = bson_data(sub);
        return true;
    }
    return false;
}

bool CLowlaDBBsonImpl::oidForKey(const utf16string &key, bson_oid_t *ret) {
    bson_iterator it[1];
    bson_type type = bson_find(it, this, key.c_str(utf16string::UTF8));
    if (BSON_OID == type) {
        *ret = *bson_iterator_oid(it);
        return true;
    }
    return false;
}

bool CLowlaDBBsonImpl::boolForKey(const utf16string &key, bool *ret) const {
    bson_iterator it[1];
    bson_type type = bson_find(it, this, key.c_str(utf16string::UTF8));
    if (BSON_BOOL == type) {
        *ret = bson_iterator_bool_raw(it);
        return true;
    }
    return false;
}

bool CLowlaDBBsonImpl::dateForKey(const utf16string &key, bson_date_t *ret) {
    bson_iterator it[1];
    bson_type type = bson_find(it, this, key.c_str(utf16string::UTF8));
    if (BSON_DATE == type) {
        *ret = bson_iterator_date(it);
        return true;
    }
    return false;
}

bool CLowlaDBBsonImpl::intForKey(const utf16string &key, int *ret) {
    bson_iterator it[1];
    bson_type type = bson_find(it, this, key.c_str(utf16string::UTF8));
    if (BSON_INT == type) {
        *ret = bson_iterator_int_raw(it);
        return true;
    }
    return false;
}

bool CLowlaDBBsonImpl::longForKey(const utf16string &key, int64_t *ret) {
    bson_iterator it[1];
    bson_type type = bson_find(it, this, key.c_str(utf16string::UTF8));
    if (BSON_LONG == type) {
        *ret = bson_iterator_long_raw(it);
        return true;
    }
    return false;
}

const char *CLowlaDBBsonImpl::valueForKey(const char *key, long *count) {
    bson_iterator it[1];
    bson_type type = bson_find(it, this, key);
    if (BSON_EOO == type) {
        *count = 0;
        return nullptr;
    }
    const char *answer = bson_iterator_value(it);
    bson_iterator_next(it);
    *count = it->cur - answer;
    return answer;
}

CLowlaDBCursor::ptr CLowlaDBCursor::create(std::unique_ptr<CLowlaDBCursorImpl> &pimpl) {
    return CLowlaDBCursor::ptr(new CLowlaDBCursor(pimpl));
}

CLowlaDBCursorImpl *CLowlaDBCursor::pimpl() {
    return m_pimpl.get();
}

CLowlaDBCursor::ptr CLowlaDBCursor::create(CLowlaDBCollection::ptr coll, const char *query) {
    // Cursors have a complex lifetime so this is one of the few cases where we need to copy the bson
    std::shared_ptr<CLowlaDBBsonImpl> bsonQuery;
    if (query) {
        bsonQuery.reset(new CLowlaDBBsonImpl(query, CLowlaDBBsonImpl::COPY));
    }
    
    std::unique_ptr<CLowlaDBCursorImpl> pimpl(new CLowlaDBCursorImpl(coll->pimpl(), bsonQuery, std::shared_ptr<CLowlaDBBsonImpl>()));
    return create(pimpl);
}

CLowlaDBCursor::ptr CLowlaDBCursor::limit(int limit) {
    std::unique_ptr<CLowlaDBCursorImpl> pimpl = m_pimpl->limit(limit);
    return create(pimpl);
}

CLowlaDBCursor::ptr CLowlaDBCursor::skip(int skip) {
    std::unique_ptr<CLowlaDBCursorImpl> pimpl = m_pimpl->skip(skip);
    return create(pimpl);
}

CLowlaDBCursor::ptr CLowlaDBCursor::sort(const char *sort) {
    std::shared_ptr<CLowlaDBBsonImpl> bsonSort(new CLowlaDBBsonImpl(sort, CLowlaDBBsonImpl::COPY));

    std::unique_ptr<CLowlaDBCursorImpl> pimpl = m_pimpl->sort(bsonSort);
    return create(pimpl);
}

CLowlaDBBson::ptr CLowlaDBCursor::next() {
    std::unique_ptr<CLowlaDBBsonImpl> answer = m_pimpl->next();
    return CLowlaDBBson::create(answer);
}

int64_t CLowlaDBCursor::count() {
    return m_pimpl->count();
}

CLowlaDBCursor::CLowlaDBCursor(std::unique_ptr<CLowlaDBCursorImpl> &pimpl) : m_pimpl(std::move(pimpl)) {
}

CLowlaDBCursorImpl::CLowlaDBCursorImpl(const CLowlaDBCursorImpl &other) : m_coll(other.m_coll), m_query(other.m_query), m_keys(other.m_keys), m_sort(other.m_sort), m_limit(other.m_limit), m_skip(other.m_skip), m_showDiskLoc(other.m_showDiskLoc) {
}

CLowlaDBCursorImpl::CLowlaDBCursorImpl(CLowlaDBCollectionImpl *coll, std::shared_ptr<CLowlaDBBsonImpl> query, std::shared_ptr<CLowlaDBBsonImpl> keys) : m_coll(coll), m_query(query), m_keys(keys), m_limit(0), m_skip(0), m_showDiskLoc(false) {
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
        CLowlaDBBsonImpl found(data, CLowlaDBBsonImpl::REF);
        if (nullptr == m_query || matches(&found)) {
            ++m_unsortedOffset;
            if (m_skip < m_unsortedOffset && (0 == m_limit || m_unsortedOffset <= m_skip + m_limit)) {
                i64 id;
                m_cursor->keySize(&id);
                std::unique_ptr<CLowlaDBBsonImpl> answer = project(&found, id);
                return answer;
            }
        }
        bson_free(data);
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
        CLowlaDBBsonImpl found(data, CLowlaDBBsonImpl::REF);
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
        if (!found->containsKey(key)) {
            return false;
        }
        long foundLength;
        const char *foundValue = found->valueForKey(key, &foundLength);
        long queryLength;
        const char *queryValue = m_query->valueForKey(key, &queryLength);
        if (foundLength != queryLength || 0 != memcmp(foundValue, queryValue, foundLength)) {
            return false;
        }
    }
    return true;
}

std::unique_ptr<CLowlaDBBsonImpl> CLowlaDBCursorImpl::project(CLowlaDBBsonImpl *found, i64 id) {
    if (nullptr == m_keys && !m_showDiskLoc) {
        std::unique_ptr<CLowlaDBBsonImpl> answer(new CLowlaDBBsonImpl(found->data(), CLowlaDBBsonImpl::OWN));
        return answer;
    }
    std::unique_ptr<CLowlaDBBsonImpl> answer(new CLowlaDBBsonImpl());
    answer->appendElement(found, "_id");
    if (m_showDiskLoc) {
        CLowlaDBBsonImpl diskLoc[1];
        diskLoc->appendInt("file", 0);
        diskLoc->appendLong("offset", id);
        diskLoc->finish();
        answer->appendObject("$diskLoc", diskLoc->data());
    }
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

CLowlaDBSyncManagerImpl::CLowlaDBSyncManagerImpl() : m_wcb(nullptr) {
    m_mutex = sqlite3_mutex_alloc(SQLITE_MUTEX_FAST);
}

void CLowlaDBSyncManagerImpl::setNsHasOutgoingChanges(const utf16string &ns, bool hasOutgoingChanges) {
    sqlite3_mutex_enter(m_mutex);
    if (hasOutgoingChanges) {
        m_nsWithChanges.insert(ns);
    }
    else {
        m_nsWithChanges.erase(ns);
    }
    sqlite3_mutex_leave(m_mutex);
}

bool CLowlaDBSyncManagerImpl::hasOutgoingChanges() {
    sqlite3_mutex_enter(m_mutex);
    bool answer = !m_nsWithChanges.empty();
    sqlite3_mutex_leave(m_mutex);
    return answer;
}

utf16string CLowlaDBSyncManagerImpl::nsWithChanges() {
    utf16string answer;
    sqlite3_mutex_enter(m_mutex);
    if (m_nsWithChanges.empty()) {
        answer = "";
    }
    else {
        answer = *m_nsWithChanges.begin();
    }
    sqlite3_mutex_leave(m_mutex);
    return answer;
}

void CLowlaDBSyncManagerImpl::setWriteCallback(LowlaDBWriteCallback wcb) {
    m_wcb = wcb;
}

void CLowlaDBSyncManagerImpl::runWriteCallback() {
    if (m_wcb) {
        (*m_wcb)();
    }
}

CLowlaDBPullData::ptr CLowlaDBPullData::create(std::unique_ptr<CLowlaDBPullDataImpl> &pimpl) {
    return CLowlaDBPullData::ptr(new CLowlaDBPullData(pimpl));
}

CLowlaDBPullDataImpl *CLowlaDBPullData::pimpl() {
    return m_pimpl.get();
}

CLowlaDBPullData::CLowlaDBPullData(std::unique_ptr<CLowlaDBPullDataImpl> &pimpl) : m_pimpl(std::move(pimpl)) {
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
    m_atoms.emplace_back(atomBson, CLowlaDBBsonImpl::REF);
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

void CLowlaDBPullDataImpl::eraseAtom(const utf16string &id) {
    for (atomIterator walk = m_atoms.begin() ; walk != m_atoms.end() ; ++walk) {
        if (walk->stringForKey("id").equals(id)) {
            m_atoms.erase(walk);
            return;
        }
    }
}

void CLowlaDBPullDataImpl::setRequestMore(const utf16string &requestMore) {
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
    m_atoms[0].intForKey("sequence", &answer);
    return answer;
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
    std::unique_ptr<CLowlaDBPullDataImpl> pd(new CLowlaDBPullDataImpl);
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

static bool shouldPullDocument(const CLowlaDBBsonImpl &atom, CLowlaDBNsCache &cache) {
    bool deleted;
    if (atom.boolForKey("deleted", &deleted) && deleted) {
        return false;
    }
    utf16string ns = atom.stringForKey("clientNs");
    CLowlaDBCollectionImpl *coll = cache.collectionForNs(ns.c_str(utf16string::UTF8));
    if (coll) {
        return coll->shouldPullDocument(&atom);
    }
    return false;
}

CLowlaDBBson::ptr lowladb_create_pull_request(CLowlaDBPullData::ptr pd) {
    CLowlaDBPullDataImpl *pullData = pd->pimpl();
    CLowlaDBNsCache cacheNs;
    std::unique_ptr<CLowlaDBBsonImpl> answer(new CLowlaDBBsonImpl);
    if (pullData->hasRequestMore()) {
        answer->appendString("requestMore", pullData->getRequestMore());
        answer->finish();
        return CLowlaDBBson::create(answer);
    }
    int i = 0;
    answer->startObject("ids");
    CLowlaDBPullDataImpl::atomIterator walk = pullData->atomsBegin();
    CLowlaDBPullDataImpl::atomIterator end = pullData->atomsEnd();
    while (walk != end) {
        const CLowlaDBBsonImpl &atom = *walk;
        if (shouldPullDocument(atom, cacheNs)) {
            answer->appendString(utf16string::valueOf(i++), atom.stringForKey("id"));
        }
        if (PULL_BATCH_SIZE == i) {
            break;
        }
        ++walk;
    }
    answer->finishObject();
    answer->finish();
    return CLowlaDBBson::create(answer);
}

void processLeadingDeletions(CLowlaDBPullDataImpl *pullData, CLowlaDBNsCache &cache) {
    CLowlaDBPullDataImpl::atomIterator walk = pullData->atomsBegin();
    CLowlaDBPullDataImpl::atomIterator end = pullData->atomsEnd();
    while (walk != end) {
        const CLowlaDBBsonImpl &atom = *walk;
        bool isDeletion;
        if (atom.boolForKey("deleted", &isDeletion) && isDeletion) {
            utf16string ns = atom.stringForKey("clientNs");
            CLowlaDBCollectionImpl *coll = cache.collectionForNs(ns.c_str(utf16string::UTF8));
            if (coll) {
                std::unique_ptr<CLowlaDBSyncDocumentLocation> loc = coll->locateDocumentForId(atom.stringForKey("id"));
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

void lowladb_apply_pull_response(const std::vector<CLowlaDBBson::ptr> response, CLowlaDBPullData::ptr pd) {
    CLowlaDBPullDataImpl *pullData = pd->pimpl();
    CLowlaDBNsCache cache;
    int i = 0;
    while (i < response.size()) {
        processLeadingDeletions(pullData, cache);
        CLowlaDBBson::ptr metaBson = response[i];
        if (metaBson->containsKey("requestMore")) {
            pullData->setRequestMore(metaBson->stringForKey("requestMore"));
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

        utf16string ns = metaBson->stringForKey("clientNs");
        CLowlaDBCollectionImpl *coll = cache.collectionForNs(ns.c_str(utf16string::UTF8));
        if (!coll) {
            ++i;
            continue;
        }
        Tx tx(coll->db()->btree());
        std::unique_ptr<CLowlaDBSyncDocumentLocation> loc = coll->locateDocumentForId(metaBson->stringForKey("id"));
        // Don't do anything if there's an outgoing log document
        if (loc->m_logFound) {
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
                coll->updateDocument(loc->m_cursor.get(), loc->m_sqliteId, dataBson->pimpl(), loc->m_found.get(), loc->m_foundMeta.get());
            }
            else {
                coll->insert(dataBson->pimpl(), metaBson->stringForKey("id"));
            }
        }
        loc->m_cursor->close();
        tx.commit();
        // Clear the id from the todo list
        pullData->eraseAtom(metaBson->stringForKey("id"));
        ++i;
    }
    processLeadingDeletions(pullData, cache);
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

CLowlaDBBson::ptr lowladb_json_to_bson(const utf16string &json) {
    Json::Value root;
    Json::Reader reader;
    if (reader.parse(json.c_str(utf16string::UTF8), root, false)) {
        CLowlaDBBson::ptr answer = CLowlaDBBson::create();
        for (Json::Value::iterator it = root.begin() ; it != root.end() ; ++it) {
            appendJsonValueToBson(answer->pimpl(), it.memberName(), *it);
        }
        answer->finish();
        return answer;
    }
    return CLowlaDBBson::ptr();
}
