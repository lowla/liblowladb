//
//  lowladb.h
//  liblowladb-apple-tests
//
//  Created by mark on 7/3/14.
//  Copyright (c) 2014 Lowla. All rights reserved.
//

#ifndef liblowladb__lowladb_h
#define liblowladb__lowladb_h

#include <vector>
#include "utf16string.h"

class CLowlaDBBsonImpl;
class CLowlaDBBsonIteratorImpl;
class CLowlaDBBsonOidImpl;
class CLowlaDBImpl;
class CLowlaDBCollectionImpl;
class CLowlaDBCursorImpl;
class CLowlaDBWriteResultImpl;
class CLowlaDBPullDataImpl;

class CLowlaDBBson {
public:
    typedef std::shared_ptr<CLowlaDBBson> ptr;
    
    static CLowlaDBBson::ptr create();
    static CLowlaDBBson::ptr create(const char *bson, bool ownsData);
    static CLowlaDBBson::ptr create(std::unique_ptr<CLowlaDBBsonImpl> &pimpl);
    CLowlaDBBsonImpl *pimpl();
    
    void appendDouble(const utf16string &key, double value);
    void appendString(const utf16string &key, const utf16string &value);
    void appendObject(const utf16string &key, const char *value);
    void appendOid(const utf16string &key, const void *value);
    void appendBool(const utf16string &key, bool value);
    void appendDate(const utf16string &key, int64_t value);
    void appendInt(const utf16string &key, int value);
    void appendLong(const utf16string &key, int64_t value);

    void startArray(const utf16string &key);
    void finishArray();
    void startObject(const utf16string &key);
    void finishObject();
    
    bool containsKey(const utf16string &key);
    bool doubleForKey(const utf16string &key, double *ret);
    utf16string stringForKey(const utf16string &key);
    bool objectForKey(const utf16string &key, CLowlaDBBson::ptr *ret);
    bool arrayForKey(const utf16string &key, CLowlaDBBson::ptr *ret);
    bool oidForKey(const utf16string &key, char *ret);
    bool boolForKey(const utf16string &key, bool *ret);
    bool dateForKey(const utf16string &key, int64_t *ret);
    bool intForKey(const utf16string &key, int *ret);
    bool longForKey(const utf16string &key, int64_t *ret);
    
    const char *data();
    size_t size();
    
    void finish();
    
    static const size_t OID_SIZE;
    static const size_t OID_STRING_SIZE;
    static void oidToString(const char *oid, char *buffer);
    static void oidGenerate(char *buffer);
        
private:
    std::unique_ptr<CLowlaDBBsonImpl> m_pimpl;
    
    CLowlaDBBson(std::unique_ptr<CLowlaDBBsonImpl> &pimpl);
};

class CLowlaDBWriteResult {
public:
    typedef std::shared_ptr<CLowlaDBWriteResult> ptr;

    static CLowlaDBWriteResult::ptr create(std::unique_ptr<CLowlaDBWriteResultImpl> &pimpl);
    CLowlaDBWriteResultImpl *pimpl();

    bool isUpdateOfExisting();
    bool getUpsertedId(char *buffer);
    int getN();
    
private:
    std::unique_ptr<CLowlaDBWriteResultImpl> m_pimpl;
    
    CLowlaDBWriteResult(std::unique_ptr<CLowlaDBWriteResultImpl> &pimpl);
};

class CLowlaDBCollection {
public:
    typedef std::shared_ptr<CLowlaDBCollection> ptr;

    static CLowlaDBCollection::ptr create(std::unique_ptr<CLowlaDBCollectionImpl> &pimpl);
    CLowlaDBCollectionImpl *pimpl();
    
    CLowlaDBWriteResult::ptr insert(const char *bsonData);
    CLowlaDBWriteResult::ptr insert(const std::vector<const char *> &bsonData);
    CLowlaDBWriteResult::ptr save(const char *bsonData);
    CLowlaDBWriteResult::ptr update(const char *queryBson, const char *objectBson, bool upsert, bool multi);
    
private:
    std::unique_ptr<CLowlaDBCollectionImpl> m_pimpl;
    
    CLowlaDBCollection(std::unique_ptr<CLowlaDBCollectionImpl> &pimpl);
};

class CLowlaDB {
public:
    typedef std::shared_ptr<CLowlaDB> ptr;
 
    static CLowlaDB::ptr create(std::unique_ptr<CLowlaDBImpl> &pimpl);
    CLowlaDBImpl *pimpl();
    
    static CLowlaDB::ptr open(const utf16string &name);
    
    CLowlaDBCollection::ptr createCollection(const utf16string &name);
    void collectionNames(std::vector<utf16string> *plstNames);
    
private:
    std::unique_ptr<CLowlaDBImpl> m_pimpl;
    
    CLowlaDB(std::unique_ptr<CLowlaDBImpl> &pimpl);
};

class CLowlaDBCursor {
public:
    typedef std::shared_ptr<CLowlaDBCursor> ptr;
    
    static CLowlaDBCursor::ptr create(std::unique_ptr<CLowlaDBCursorImpl> &pimpl);
    CLowlaDBCursorImpl *pimpl();

    static CLowlaDBCursor::ptr create(CLowlaDBCollection::ptr coll, const char *query);
    CLowlaDBCursor::ptr limit(int limit);
    CLowlaDBCursor::ptr skip(int skip);
    CLowlaDBBson::ptr next();
    int64_t count();
    
private:
    std::unique_ptr<CLowlaDBCursorImpl> m_pimpl;
    
    CLowlaDBCursor(std::unique_ptr<CLowlaDBCursorImpl> &pimpl);
};

class CLowlaDBPullData {
public:
    typedef std::shared_ptr<CLowlaDBPullData> ptr;
    
    static CLowlaDBPullData::ptr create(std::unique_ptr<CLowlaDBPullDataImpl> &pimpl);
    CLowlaDBPullDataImpl *pimpl();
    
    bool hasRequestMore();
    utf16string getRequestMore();
    
    bool isComplete();
    int getSequenceForNextRequest();
    
private:
    std::unique_ptr<CLowlaDBPullDataImpl> m_pimpl;
    CLowlaDBPullData(std::unique_ptr<CLowlaDBPullDataImpl> &pimpl);
};

utf16string lowladb_get_version();
void lowladb_list_databases(std::vector<utf16string> *plstdb);
void lowladb_db_delete(const utf16string &name);

typedef void (*LowlaDBWriteCallback)();
void lowladb_set_write_callback(LowlaDBWriteCallback wbc);

CLowlaDBPullData::ptr lowladb_parse_syncer_response(const char *bson);
CLowlaDBBson::ptr lowladb_create_push_request();
void lowladb_apply_push_response(const char *bson);
CLowlaDBBson::ptr lowladb_create_pull_request(CLowlaDBPullData::ptr pd);
void lowladb_apply_pull_response(const std::vector<CLowlaDBBson::ptr> response, CLowlaDBPullData::ptr pd);

CLowlaDBBson::ptr lowladb_json_to_bson(const utf16string &json);
utf16string lowladb_bson_to_json(const char *bson);

#endif
