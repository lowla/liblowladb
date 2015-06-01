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
class CLowlaDBImpl;
class CLowlaDBCollectionImpl;
class CLowlaDBCursorImpl;
class CLowlaDBWriteResultImpl;
class CLowlaDBPullDataImpl;
class CLowlaDBPushDataImpl;

class CLowlaDBBson {
public:
    typedef std::shared_ptr<CLowlaDBBson> ptr;
    
    static CLowlaDBBson::ptr create();
    static CLowlaDBBson::ptr create(const char *bson, bool ownsData);
    static CLowlaDBBson::ptr create(std::shared_ptr<CLowlaDBBsonImpl> pimpl);
    static CLowlaDBBson::ptr empty();
    std::shared_ptr<CLowlaDBBsonImpl> pimpl();
    
    void appendDouble(const char *key, double value);
    void appendString(const char *key, const char *value);
    void appendObject(const char *key, const char *value);
    void appendOid(const char *key, const void *value);
    void appendBool(const char *key, bool value);
    void appendDate(const char *key, int64_t value);
    void appendInt(const char *key, int value);
    void appendLong(const char *key, int64_t value);

    void startArray(const char *key);
    void finishArray();
    void startObject(const char *key);
    void finishObject();
    
    bool containsKey(const char *key);
    bool doubleForKey(const char *key, double *ret);
    bool stringForKey(const char *key, const char **ret);
    bool objectForKey(const char *key, CLowlaDBBson::ptr *ret);
    bool arrayForKey(const char *key, CLowlaDBBson::ptr *ret);
    bool oidForKey(const char *key, char *ret);
    bool boolForKey(const char *key, bool *ret);
    bool dateForKey(const char *key, int64_t *ret);
    bool intForKey(const char *key, int *ret);
    bool longForKey(const char *key, int64_t *ret);
    
    bool equalValues(const char *key, CLowlaDBBson::ptr other, const char *otherKey);
    
    const char *data();
    size_t size();
    
    void finish();
    
	// OID_SIZE is sizeof(bson_oid_t) but we need to define it here (rather than in the .cpp file)
	// to be able to compile in Visual Studio. We don't want to expose bson.h to our callers.
    static const size_t OID_SIZE = 12;
    static const size_t OID_STRING_SIZE = 2 * OID_SIZE + 1;

    static void oidToString(const char *oid, char *buffer);
    static void oidFromString(char *oid, const char *str);
    static void oidGenerate(char *buffer);
    
private:
    std::shared_ptr<CLowlaDBBsonImpl> m_pimpl;
    
    CLowlaDBBson(std::shared_ptr<CLowlaDBBsonImpl> pimpl);
};

class CLowlaDBWriteResult {
public:
    typedef std::shared_ptr<CLowlaDBWriteResult> ptr;

    static CLowlaDBWriteResult::ptr create(std::shared_ptr<CLowlaDBWriteResultImpl> pimpl);
    std::shared_ptr<CLowlaDBWriteResultImpl> pimpl();

    int documentCount();
    CLowlaDBBson::ptr document(int n);
    
private:
    std::shared_ptr<CLowlaDBWriteResultImpl> m_pimpl;
    
    CLowlaDBWriteResult(std::shared_ptr<CLowlaDBWriteResultImpl> pimpl);
};

class CLowlaDBCollection {
public:
    typedef std::shared_ptr<CLowlaDBCollection> ptr;

    static CLowlaDBCollection::ptr create(std::shared_ptr<CLowlaDBCollectionImpl> pimpl);
    std::shared_ptr<CLowlaDBCollectionImpl> pimpl();
    
    CLowlaDBWriteResult::ptr insert(const char *bsonData);
    CLowlaDBWriteResult::ptr insert(const char *bsonData, const char *lowlaId);
    CLowlaDBWriteResult::ptr insert(const std::vector<const char *> &bsonData);
    CLowlaDBWriteResult::ptr remove(const char *queryBson);
    CLowlaDBWriteResult::ptr save(const char *bsonData);
    CLowlaDBWriteResult::ptr update(const char *queryBson, const char *objectBson, bool upsert, bool multi);
    
private:
    std::shared_ptr<CLowlaDBCollectionImpl> m_pimpl;
    
    CLowlaDBCollection(std::shared_ptr<CLowlaDBCollectionImpl> pimpl);
};

class CLowlaDB {
public:
    typedef std::shared_ptr<CLowlaDB> ptr;
 
    static CLowlaDB::ptr create(std::shared_ptr<CLowlaDBImpl> pimpl);
    std::shared_ptr<CLowlaDBImpl> pimpl();
    
    static CLowlaDB::ptr open(const utf16string &name);
    
    CLowlaDBCollection::ptr createCollection(const utf16string &name);
    void collectionNames(std::vector<utf16string> *plstNames);
    
private:
    std::shared_ptr<CLowlaDBImpl> m_pimpl;
    
    CLowlaDB(std::shared_ptr<CLowlaDBImpl> pimpl);
};

class CLowlaDBCursor {
public:
    typedef std::shared_ptr<CLowlaDBCursor> ptr;
    
    static CLowlaDBCursor::ptr create(std::shared_ptr<CLowlaDBCursorImpl> pimpl);
    std::shared_ptr<CLowlaDBCursorImpl> pimpl();

    static CLowlaDBCursor::ptr create(CLowlaDBCollection::ptr coll, const char *query);
    CLowlaDBCursor::ptr limit(int limit);
    CLowlaDBCursor::ptr skip(int skip);
    CLowlaDBCursor::ptr sort(const char *sort);
    CLowlaDBCursor::ptr showPending();
    
    CLowlaDBBson::ptr next();
    int64_t count();
    
private:
    std::shared_ptr<CLowlaDBCursorImpl> m_pimpl;
    
    CLowlaDBCursor(std::shared_ptr<CLowlaDBCursorImpl> pimpl);
};

class CLowlaDBPullData {
public:
    typedef std::shared_ptr<CLowlaDBPullData> ptr;
    
    static CLowlaDBPullData::ptr create(std::shared_ptr<CLowlaDBPullDataImpl> pimpl);
    std::shared_ptr<CLowlaDBPullDataImpl> pimpl();
    
    bool hasRequestMore();
    utf16string getRequestMore();
    
    bool isComplete();
    int getSequenceForNextRequest();
    
private:
    std::shared_ptr<CLowlaDBPullDataImpl> m_pimpl;
    CLowlaDBPullData(std::shared_ptr<CLowlaDBPullDataImpl> pimpl);
};

class CLowlaDBPushData {
public:
    typedef std::shared_ptr<CLowlaDBPushData> ptr;
    
    static CLowlaDBPushData::ptr create(std::shared_ptr<CLowlaDBPushDataImpl> pimpl);
    std::shared_ptr<CLowlaDBPushDataImpl> pimpl();
    
    bool isComplete();
    
private:
    std::shared_ptr<CLowlaDBPushDataImpl> m_pimpl;
    CLowlaDBPushData(std::shared_ptr<CLowlaDBPushDataImpl> pimpl);
};

utf16string lowladb_get_version();
void lowladb_list_databases(std::vector<utf16string> *plstdb);
void lowladb_db_delete(const utf16string &name);

CLowlaDBPullData::ptr lowladb_parse_syncer_response(const char *bson);
CLowlaDBPushData::ptr lowladb_collect_push_data();
CLowlaDBBson::ptr lowladb_create_push_request(CLowlaDBPushData::ptr pd);
void lowladb_apply_push_response(std::vector<CLowlaDBBson::ptr> &response, CLowlaDBPushData::ptr pd);

CLowlaDBBson::ptr lowladb_create_pull_request(CLowlaDBPullData::ptr pd);
void lowladb_apply_pull_response(const std::vector<CLowlaDBBson::ptr> &response, CLowlaDBPullData::ptr pd);

// The first version assumes the data is null-terminated. If not, use the second!
CLowlaDBBson::ptr lowladb_json_to_bson(const char *json);
CLowlaDBBson::ptr lowladb_json_to_bson(const char *json, size_t length);

utf16string lowladb_bson_to_json(const char *bson);

void lowladb_load_json(const char *json);
void lowladb_apply_json_pull_response(const char *json, CLowlaDBPullData::ptr pd);
void lowladb_apply_json_push_response(const char *json, CLowlaDBPushData::ptr pd);

typedef void (*LowlaDbCollectionListener)(void *user, const char *ns);
void lowladb_add_collection_listener(LowlaDbCollectionListener l, void *user);
void lowladb_remove_collection_listener(LowlaDbCollectionListener l);


#endif
