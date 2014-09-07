#include "cstdio"
#include "set"

#include "bson/bson.h"
#include "integration.h"
#include "SqliteCursor.h"
#include "TeamstudioException.h"

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
    bool m_logFound;
};

class CLowlaDBPullDataImpl {
public:
    typedef std::vector<CLowlaDBBsonImpl>::const_iterator atomIterator;
    
    CLowlaDBPullDataImpl();
    void appendAtom(const char *atomBson);
    void setSequence(int sequence);
    atomIterator atomsBegin();
    atomIterator atomsEnd();
    void setRequestMore(const utf16string &requestMore);
    bool hasRequestMore();
    utf16string getRequestMore();
    
private:
    std::vector<CLowlaDBBsonImpl> m_atoms;
    int m_sequence;
    utf16string m_requestMore;
};

class CLowlaDBImpl {
public:
    CLowlaDBImpl(sqlite3 *pDb);
    ~CLowlaDBImpl();
    
    std::unique_ptr<CLowlaDBCollectionImpl> createCollection(const utf16string &name);
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
    
    bool isUpdateOfExisting();
    void setUpdateOfExisting(bool updateOfExisting);
    bson_oid_t getUpsertedId();
    void setUpsertedId(bson_oid_t *oid);
    int getN();
    void setN(int n);

private:
    bool m_updateOfExisting;
    bson_oid_t m_upsertedId;
    int m_n;
};

class CLowlaDBBsonImpl : public bson {
public:
    CLowlaDBBsonImpl();
    CLowlaDBBsonImpl(const char *data, bool ownsData);
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
    CLowlaDBCollectionImpl(CLowlaDBImpl *db, int root, int logRoot);
    std::unique_ptr<CLowlaDBWriteResultImpl> insert(CLowlaDBBsonImpl *obj);
    std::unique_ptr<CLowlaDBWriteResultImpl> save(CLowlaDBBsonImpl *obj);
    std::unique_ptr<CLowlaDBWriteResultImpl> update(CLowlaDBBsonImpl *query, CLowlaDBBsonImpl *object, bool upsert, bool multi);
    
    SqliteCursor::ptr openCursor();
    CLowlaDBImpl *db();
    bool shouldPullDocument(const CLowlaDBBsonImpl *atom);
    std::unique_ptr<CLowlaDBSyncDocumentLocation> locateDocumentForAtom(const CLowlaDBBsonImpl *atom);
    
private:
    bool isReplaceObject(CLowlaDBBsonImpl *update);
    std::unique_ptr<CLowlaDBBsonImpl> applyUpdate(CLowlaDBBsonImpl *update, CLowlaDBBsonImpl *original);
    void updateDocument(SqliteCursor *cursor, int64_t id, CLowlaDBBsonImpl *obj, CLowlaDBBsonImpl *oldObj);
    
    enum {
        LOGTYPE_INSERT = 1,
        LOGTYPE_UPDATE,
        LOGTYPE_REMOVE
    };
    
    CLowlaDBImpl *m_db;
    int m_root;
    int m_logRoot;
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
    const int TRANS_READONLY = 0;
    const int TRANS_READWRITE = 1;
    
    Btree *m_pBt;
    bool m_ownTx;
};

class CLowlaDBCursorImpl {
public:
    CLowlaDBCursorImpl(CLowlaDBCollectionImpl *coll, CLowlaDBBsonImpl *query, CLowlaDBBsonImpl *keys);
    CLowlaDBCursorImpl(const CLowlaDBCursorImpl &other);
    
    std::unique_ptr<CLowlaDBCursorImpl> limit(int limit);
    std::unique_ptr<CLowlaDBCursorImpl> showDiskLoc();
    std::unique_ptr<CLowlaDBBsonImpl> next();
    
    SqliteCursor::ptr sqliteCursor();
    int64_t currentId();
    
private:
    bool matches(CLowlaDBBsonImpl *found);
    std::unique_ptr<CLowlaDBBsonImpl> project(CLowlaDBBsonImpl *found, int64_t id);
    
    // The cursor has to come after the tx so that it is destructed (closed) before we end the tx
    std::unique_ptr<Tx> m_tx;
    SqliteCursor::ptr m_cursor;
    
    CLowlaDBCollectionImpl *m_coll;
    CLowlaDBBsonImpl *m_query;
    CLowlaDBBsonImpl *m_keys;
    
    int m_limit;
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
    utf16string filePath = getFullPath(name);
    sqlite3 *pDb;
    int rc = sqlite3_open_v2(filePath.c_str(), &pDb, SQLITE_OPEN_READWRITE, 0);
    if (SQLITE_OK == rc) {
        std::unique_ptr<CLowlaDBImpl> pimpl(new CLowlaDBImpl(pDb));
        return pimpl;
    }
    rc = createDatabase(filePath);
    if (SQLITE_OK == rc) {
        rc = sqlite3_open_v2(filePath.c_str(), &pDb, SQLITE_OPEN_READWRITE, 0);
    }
    if (SQLITE_OK == rc) {
        std::unique_ptr<CLowlaDBImpl> pimpl(new CLowlaDBImpl(pDb));
        return pimpl;
    }
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
                collRoot = bson_iterator_int(it);
            }
            bson_destroy(data);
            
            if (0 == strcmp(collName, foundName) && -1 != collRoot && -1 != collLogRoot) {
                return std::unique_ptr<CLowlaDBCollectionImpl>(new CLowlaDBCollectionImpl(this, collRoot, collLogRoot));
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
    return std::unique_ptr<CLowlaDBCollectionImpl>(new CLowlaDBCollectionImpl(this, collRoot, collLogRoot));
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
    std::unique_ptr<CLowlaDBBsonImpl> bson(new CLowlaDBBsonImpl(bsonData, false));
    std::unique_ptr<CLowlaDBWriteResultImpl> pimpl = m_pimpl->insert(bson.get());
    return CLowlaDBWriteResult::create(pimpl);
}

CLowlaDBWriteResult::ptr CLowlaDBCollection::save(const char *bsonData) {
    std::unique_ptr<CLowlaDBBsonImpl> bson(new CLowlaDBBsonImpl(bsonData, false));
    std::unique_ptr<CLowlaDBWriteResultImpl> pimpl = m_pimpl->save(bson.get());
    return CLowlaDBWriteResult::create(pimpl);
}

CLowlaDBWriteResult::ptr CLowlaDBCollection::update(const char *queryBson, const char *objectBson, bool upsert, bool multi) {
    std::unique_ptr<CLowlaDBBsonImpl> query(new CLowlaDBBsonImpl(queryBson, false));
    std::unique_ptr<CLowlaDBBsonImpl> object(new CLowlaDBBsonImpl(objectBson, false));
    std::unique_ptr<CLowlaDBWriteResultImpl> pimpl = m_pimpl->update(query.get(), object.get(), upsert, multi);
    return CLowlaDBWriteResult::create(pimpl);
}

CLowlaDBCollectionImpl::CLowlaDBCollectionImpl(CLowlaDBImpl *db, int root, int logRoot) : m_db(db), m_root(root), m_logRoot(logRoot) {
}

std::unique_ptr<CLowlaDBWriteResultImpl> CLowlaDBCollectionImpl::insert(CLowlaDBBsonImpl *obj) {
    if (!obj->containsKey("_id")) {
        CLowlaDBBsonImpl fixed[1];
        bson_oid_t newOid;
        bson_oid_gen(&newOid);
        fixed->appendOid("_id", &newOid);
        fixed->appendAll(obj);
        fixed->finish();
        std::unique_ptr<CLowlaDBWriteResultImpl> answer = insert(fixed);
        answer->setUpsertedId(&newOid);
        return answer;
    }
    
    std::unique_ptr<CLowlaDBWriteResultImpl> answer(new CLowlaDBWriteResultImpl);
    answer->setUpdateOfExisting(false);
    
    Tx tx(m_db->btree());
    
    SqliteCursor::ptr cursor = m_db->openCursor(m_root);
    int res = 0;
    int rc = cursor->last(&res);
    i64 lastInternalId = 0;
    if (SQLITE_OK == rc && 0 == res) {
        rc = cursor->keySize(&lastInternalId);
    }
    i64 newId = lastInternalId + 1;
    rc = cursor->insert(NULL, newId, obj->data(), (int)obj->size(), 0, true, 0);
    if (SQLITE_OK == rc) {
        SqliteCursor::ptr logCursor = m_db->openCursor(m_logRoot);
        static char logData[] = {0, 0, 0, 0, LOGTYPE_INSERT};
        rc = logCursor->insert(NULL, newId, logData, sizeof(logData), 0, true, 0);
    }
    rc = cursor->close();
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
        return insert(obj);
    }
}

std::unique_ptr<CLowlaDBWriteResultImpl> CLowlaDBCollectionImpl::update(CLowlaDBBsonImpl *query, CLowlaDBBsonImpl *object, bool upsert, bool multi) {

    Tx tx(m_db->btree());
    std::unique_ptr<CLowlaDBCursorImpl> cursor(new CLowlaDBCursorImpl(this, query, nullptr));
    if (!multi) {
        cursor = cursor->limit(1);
    }
    
    std::unique_ptr<CLowlaDBBsonImpl> found = cursor->next();
    if (!found && upsert) {
        return insert(object);
    }
    int count = 0;
    while (found) {
        ++count;
        int64_t id = cursor->currentId();
        std::unique_ptr<CLowlaDBBsonImpl> bsonToWrite = applyUpdate(object, found.get());
        updateDocument(cursor->sqliteCursor().get(), id, bsonToWrite.get(), found.get());
        
        found = cursor->next();
    }
    cursor.reset();
    tx.commit();
    std::unique_ptr<CLowlaDBWriteResultImpl> wr(new CLowlaDBWriteResultImpl);
    wr->setUpdateOfExisting(true);
    wr->setN(count);
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
            set.reset(new CLowlaDBBsonImpl(tmp, false));
        }
        else {
            set.reset(new CLowlaDBBsonImpl);
        }
        if (update->objectForKey("$unset", &tmp)) {
            unset.reset(new CLowlaDBBsonImpl(tmp, false));
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

void CLowlaDBCollectionImpl::updateDocument(SqliteCursor *cursor, int64_t id, CLowlaDBBsonImpl *obj, CLowlaDBBsonImpl *oldObj) {
    int rc = cursor->insert(NULL, id, obj->data(), (int)obj->size(), 0, false, 0);
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

CLowlaDBImpl *CLowlaDBCollectionImpl::db() {
    return m_db;
}

std::unique_ptr<CLowlaDBSyncDocumentLocation> CLowlaDBCollectionImpl::locateDocumentForAtom(const CLowlaDBBsonImpl *atom) {
    std::unique_ptr<CLowlaDBSyncDocumentLocation> answer(new CLowlaDBSyncDocumentLocation);

    // Locate the document by its lowla key
    CLowlaDBBsonImpl query;
    query.startObject("_lowla");
    query.appendElement(atom, "id");
    query.finishObject();
    query.finish();
    CLowlaDBCursorImpl cursor(this, &query, nullptr);

    answer->m_found = cursor.next();
    answer->m_cursor = cursor.sqliteCursor();
    if (answer->m_found) {
        int64_t id = cursor.currentId();
        answer->m_logCursor = m_db->openCursor(m_logRoot);
        int res;
        int rc = answer->m_logCursor->movetoUnpacked(nullptr, id, 0, &res);
        answer->m_logFound = (SQLITE_OK == rc && 0 == res);
    }
    else {
        answer->m_logFound = false;
    }
    return answer;
}

bool CLowlaDBCollectionImpl::shouldPullDocument(const CLowlaDBBsonImpl *atom) {
    
    Tx tx(m_db->btree());
    
    std::unique_ptr<CLowlaDBSyncDocumentLocation> loc = locateDocumentForAtom(atom);

    // If found
    if (loc->m_found) {
        // See if there's an outgoing record in the log. If so, don't pull
        if (loc->m_logFound) {
            return false;
        }
        // If the found record has the same version then don't want to pull
        const char *_lowlaObj;
        if (loc->m_found->objectForKey("_lowla", &_lowlaObj)) {
            CLowlaDBBsonImpl _lowla(_lowlaObj, false);
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

bool CLowlaDBWriteResult::getUpsertedId(char *buffer) {
    bson_oid_t answer = m_pimpl->getUpsertedId();
    memcpy(buffer, &answer, CLowlaDBBson::OID_SIZE);
    return 0 != answer.ints[0] || 0 != answer.ints[1] || 0 != answer.ints[2];
}

bool CLowlaDBWriteResult::isUpdateOfExisting() {
    return m_pimpl->isUpdateOfExisting();
}

int CLowlaDBWriteResult::getN() {
    return m_pimpl->getN();
}

CLowlaDBWriteResult::CLowlaDBWriteResult(std::unique_ptr<CLowlaDBWriteResultImpl> &pimpl) : m_pimpl(std::move(pimpl)) {
}

CLowlaDBWriteResultImpl::CLowlaDBWriteResultImpl() : m_updateOfExisting(false), m_n(1) {
    memset(&m_upsertedId, 0, sizeof(m_upsertedId));
}

bool CLowlaDBWriteResultImpl::isUpdateOfExisting() {
    return m_updateOfExisting;
}

void CLowlaDBWriteResultImpl::setUpdateOfExisting(bool updateOfExisting) {
    m_updateOfExisting = updateOfExisting;
}

bson_oid_t CLowlaDBWriteResultImpl::getUpsertedId() {
    return m_upsertedId;
}

void CLowlaDBWriteResultImpl::setUpsertedId(bson_oid_t *oid) {
    m_upsertedId = *oid;
}

int CLowlaDBWriteResultImpl::getN() {
    return m_n;
}

void CLowlaDBWriteResultImpl::setN(int n) {
    m_n = n;
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
    std::unique_ptr<CLowlaDBBsonImpl> pimpl(new CLowlaDBBsonImpl(bson, ownsData));
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

CLowlaDBBsonImpl::CLowlaDBBsonImpl(const char *data, bool ownsData) {
    bson_init_finished_data(this, (char *)data, ownsData);
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
    CLowlaDBBsonImpl obj(value, false);
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

CLowlaDBCursor::ptr CLowlaDBCursor::create(CLowlaDBCollection::ptr coll) {
    std::unique_ptr<CLowlaDBCursorImpl> pimpl(new CLowlaDBCursorImpl(coll->pimpl(), nullptr, nullptr));
    return create(pimpl);
}

CLowlaDBCursor::ptr CLowlaDBCursor::limit(int limit) {
    std::unique_ptr<CLowlaDBCursorImpl> pimpl = m_pimpl->limit(limit);
    return create(pimpl);
}

CLowlaDBBson::ptr CLowlaDBCursor::next() {
    std::unique_ptr<CLowlaDBBsonImpl> answer = m_pimpl->next();
    return CLowlaDBBson::create(answer);
}

CLowlaDBCursor::CLowlaDBCursor(std::unique_ptr<CLowlaDBCursorImpl> &pimpl) : m_pimpl(std::move(pimpl)) {
}

CLowlaDBCursorImpl::CLowlaDBCursorImpl(const CLowlaDBCursorImpl &other) : m_coll(other.m_coll), m_query(other.m_query), m_keys(other.m_keys), m_limit(other.m_limit), m_showDiskLoc(other.m_showDiskLoc) {
}

CLowlaDBCursorImpl::CLowlaDBCursorImpl(CLowlaDBCollectionImpl *coll, CLowlaDBBsonImpl *query, CLowlaDBBsonImpl *keys) : m_coll(coll), m_query(query), m_keys(keys), m_limit(0), m_showDiskLoc(false) {
}

std::unique_ptr<CLowlaDBCursorImpl> CLowlaDBCursorImpl::limit(int limit) {
    std::unique_ptr<CLowlaDBCursorImpl> answer(new CLowlaDBCursorImpl(*this));
    answer->m_limit = limit;
    return answer;
}

std::unique_ptr<CLowlaDBCursorImpl> CLowlaDBCursorImpl::showDiskLoc() {
    std::unique_ptr<CLowlaDBCursorImpl> answer(new CLowlaDBCursorImpl(*this));
    answer->m_showDiskLoc = true;
    return answer;
}

std::unique_ptr<CLowlaDBBsonImpl> CLowlaDBCursorImpl::next() {
    int rc;
    int res;
    if (!m_tx) {
        m_tx.reset(new Tx(m_coll->db()->btree()));
        m_cursor = m_coll->openCursor();
        rc = m_cursor->first(&res);
    }
    else {
        rc = m_cursor->next(&res);
    }
    while (SQLITE_OK == res && 0 == rc) {
        u32 size;
        m_cursor->dataSize(&size);
        char *data = (char *)bson_malloc(size);
        m_cursor->data(0, size, data);
        CLowlaDBBsonImpl found(data, false);
        if (nullptr == m_query || matches(&found)) {
            i64 id;
            m_cursor->keySize(&id);
            std::unique_ptr<CLowlaDBBsonImpl> answer = project(&found, id);
            return answer;
        }
        rc = m_cursor->next(&res);
    }
    return std::unique_ptr<CLowlaDBBsonImpl>();
}

SqliteCursor::ptr CLowlaDBCursorImpl::sqliteCursor() {
    return m_cursor;
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
    bson_iterator_init(it, m_query);
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
        std::unique_ptr<CLowlaDBBsonImpl> answer(new CLowlaDBBsonImpl(found->data(), true));
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

CLowlaDBPullDataImpl::CLowlaDBPullDataImpl() : m_sequence(0) {
}

void CLowlaDBPullDataImpl::appendAtom(const char *atomBson) {
    m_atoms.emplace_back(atomBson, false);
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

void CLowlaDBPullDataImpl::setRequestMore(const utf16string &requestMore) {
    m_requestMore = requestMore;
}

bool CLowlaDBPullDataImpl::hasRequestMore() {
    return !m_requestMore.isEmpty();
}

utf16string CLowlaDBPullDataImpl::getRequestMore() {
    return m_requestMore;
}

utf16string lowladb_get_version() {
    return "0.1.0";
}

void lowladb_db_delete(const utf16string &name) {
    utf16string filePath = getFullPath(name);

    remove(filePath.c_str());
}

CLowlaDBPullData::ptr lowladb_parse_syncer_response(const char *bsonData) {
    CLowlaDBBsonImpl bson(bsonData, false);
    std::unique_ptr<CLowlaDBPullDataImpl> pd(new CLowlaDBPullDataImpl);
    int sequence;
    if (bson.intForKey("sequence", &sequence)) {
        pd->setSequence(sequence);
    }
    const char *atoms;
    if (bson.arrayForKey("atoms", &atoms)) {
        CLowlaDBBsonImpl array(atoms, false);
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
                std::unique_ptr<CLowlaDBSyncDocumentLocation> loc = coll->locateDocumentForAtom(&atom);
                // We only process the deletion if there is no outgoing record
                if (!loc->m_logFound && loc->m_found) {
                    loc->m_cursor->deleteCurrent();
                }
            }
        }
        ++walk;
    }
}

void lowladb_apply_pull_response(const char *bson, CLowlaDBPullData::ptr pd) {
    CLowlaDBPullDataImpl *pullData = pd->pimpl();
    CLowlaDBNsCache cache;
    processLeadingDeletions(pullData, cache);
    CLowlaDBBsonImpl response(bson, false);
    pullData->setRequestMore(response.stringForKey("requestMore"));
    const char *docsBson;
    if (!response.arrayForKey("documents", &docsBson)) {
        return;
    }
    int i = 0;
    CLowlaDBBsonImpl docs(docsBson, false);
    const char *docBson;
    while (docs.objectForKey(utf16string::valueOf(i), &docBson)) {
        CLowlaDBBsonImpl doc(docBson, false);
        const char *docBodyBson;
        if (doc.objectForKey("document", &docBodyBson)) {
            CLowlaDBBsonImpl docBody(docBodyBson, false);
            const char *lowlaBson;
            if (docBody.objectForKey("_lowla", &lowlaBson)) {
                CLowlaDBBsonImpl lowla(lowlaBson, false);
                utf16string ns = doc.stringForKey("clientNs");
                CLowlaDBCollectionImpl *coll = cache.collectionForNs(ns.c_str(utf16string::UTF8));
                if (coll) {
                    std::unique_ptr<CLowlaDBSyncDocumentLocation> loc = coll->locateDocumentForAtom(&lowla);
                    // Don't do anything if there's an outgoing log document
                    if (!loc->m_logFound) {
                        // Deletion?
                        bool isDeletion;
                        if (doc.boolForKey("deleted", &isDeletion) && isDeletion) {
                            if (loc->m_found) {
                                loc->m_cursor->deleteCurrent();
                            }
                        }
                        // Nope, need to insert/update the document
                        if (loc->m_found) {
                            // TODO
                        }
                    }
                }
            }
        }
        ++i;
        processLeadingDeletions(pullData, cache);
    }
    
}
