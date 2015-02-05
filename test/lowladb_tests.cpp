//
//  lowladb_tests.cpp
//  liblowladb-apple-tests
//
//  Created by mark on 7/3/14.
//  Copyright (c) 2014 Lowla. All rights reserved.
//

#include "gtest.h"

#include "lowladb.h"

class DbTestFixture : public ::testing::Test {
public:
    DbTestFixture();
    ~DbTestFixture();
    
protected:
    CLowlaDB::ptr db;
    CLowlaDBCollection::ptr coll;
};

DbTestFixture::DbTestFixture() {
    lowladb_db_delete("mydb");
    db = CLowlaDB::open("mydb");
    coll = db->createCollection("mycoll");
}

DbTestFixture::~DbTestFixture() {
    lowladb_db_delete("mydb");
}

TEST_F(DbTestFixture, test_collection_names) {
    std::vector<utf16string> lstNames;
    
    db->collectionNames(&lstNames);
    EXPECT_EQ(1, lstNames.size());
    EXPECT_EQ("mycoll", lstNames.front());
    
    db->createCollection("coll2.sub");
    lstNames.clear();
    db->collectionNames(&lstNames);
    EXPECT_EQ(2, lstNames.size());
    EXPECT_EQ("mycoll", lstNames.front());
    EXPECT_EQ("coll2.sub", lstNames[1]);
}

TEST_F(DbTestFixture, test_can_create_single_string_documents) {
    CLowlaDBBson::ptr bson = CLowlaDBBson::create();
    bson->appendString("myfield", "mystring");
    bson->finish();
    CLowlaDBWriteResult::ptr wr = coll->insert(bson->data());
    char buffer[CLowlaDBBson::OID_SIZE];
    EXPECT_TRUE(wr->getUpsertedId(buffer));
}

TEST_F(DbTestFixture, test_create_new_id_for_each_document) {
    CLowlaDBBson::ptr bson = CLowlaDBBson::create();
    bson->appendString("myfield", "mystring");
    bson->finish();
    CLowlaDBWriteResult::ptr wr1 = coll->insert(bson->data());
    CLowlaDBWriteResult::ptr wr2 = coll->insert(bson->data());
    char buf1[CLowlaDBBson::OID_SIZE];
    char buf2[CLowlaDBBson::OID_SIZE];
    EXPECT_TRUE(wr1->getUpsertedId(buf1));
    EXPECT_TRUE(wr2->getUpsertedId(buf2));
    EXPECT_NE(0, memcmp(buf1, buf2, CLowlaDBBson::OID_SIZE));
}

TEST_F(DbTestFixture, test_insert_multiple_documents_with_success) {
    CLowlaDBBson::ptr bson = CLowlaDBBson::create();
    bson->appendString("myfield", "mystring");
    bson->finish();
    std::vector<const char *> arr;
    arr.push_back(bson->data());
    arr.push_back(bson->data());
    CLowlaDBWriteResult::ptr wr = coll->insert(arr);
    CLowlaDBCursor::ptr cursor = CLowlaDBCursor::create(coll, nullptr);
    CLowlaDBBson::ptr doc1 = cursor->next();
    CLowlaDBBson::ptr doc2 = cursor->next();
    EXPECT_FALSE(!!cursor->next());
    
    char buf1[CLowlaDBBson::OID_SIZE];
    char buf2[CLowlaDBBson::OID_SIZE];
    EXPECT_TRUE(doc1->containsKey("myfield"));
    EXPECT_TRUE(doc1->oidForKey("_id", buf1));
    EXPECT_TRUE(doc2->containsKey("myfield"));
    EXPECT_TRUE(doc2->oidForKey("_id", buf2));
    EXPECT_NE(0, memcmp(buf1, buf2, CLowlaDBBson::OID_SIZE));
}

TEST_F(DbTestFixture, test_update_replace_single_doc) {
    CLowlaDBBson::ptr bson = CLowlaDBBson::create();
    bson->appendString("myfield", "mystring");
    bson->finish();
    CLowlaDBWriteResult::ptr wr1 = coll->insert(bson->data());
    coll->insert(bson->data());
    char buf1[CLowlaDBBson::OID_SIZE];
    EXPECT_TRUE(wr1->getUpsertedId(buf1));
    
    CLowlaDBBson::ptr query = CLowlaDBBson::create();
    query->appendOid("_id", buf1);
    query->finish();
    CLowlaDBBson::ptr update = CLowlaDBBson::create();
    update->appendString("myfield2", "mystring2");
    update->finish();
    
    CLowlaDBWriteResult::ptr wrUpdate = coll->update(query->data(), update->data(), false, true);
    EXPECT_EQ(1, wrUpdate->getN());
    EXPECT_TRUE(wrUpdate->isUpdateOfExisting());
    
    // Make sure we updated the document correctly, replacing existing fields and not changing _id
    CLowlaDBCursor::ptr cursor = CLowlaDBCursor::create(coll, nullptr);
    CLowlaDBBson::ptr check = cursor->next();
    EXPECT_TRUE(check->containsKey("myfield2"));
    EXPECT_EQ("mystring2", check->stringForKey("myfield2"));
    EXPECT_FALSE(check->containsKey("myfield"));
    char bufCheck[CLowlaDBBson::OID_SIZE];
    EXPECT_TRUE(check->oidForKey("_id", bufCheck));
    EXPECT_TRUE(0 == memcmp(bufCheck, buf1, CLowlaDBBson::OID_SIZE));
    
    // Make sure we didn't touch the next document
    check = cursor->next();
    EXPECT_TRUE(check->containsKey("myfield"));
    EXPECT_EQ("mystring", check->stringForKey("myfield"));
    EXPECT_FALSE(check->containsKey("myfield2"));
    
    // Make sure we didn't create any unexpected documents
    check = cursor->next();
    EXPECT_FALSE(check);
}

TEST_F(DbTestFixture, test_update_set_single_doc) {
    CLowlaDBBson::ptr bson = CLowlaDBBson::create();
    bson->appendString("myfield", "mystring");
    bson->finish();
    CLowlaDBWriteResult::ptr wr1 = coll->insert(bson->data());
    coll->insert(bson->data());
    char buf1[CLowlaDBBson::OID_SIZE];
    EXPECT_TRUE(wr1->getUpsertedId(buf1));
    
    CLowlaDBBson::ptr query = CLowlaDBBson::create();
    query->appendOid("_id", buf1);
    query->finish();
    CLowlaDBBson::ptr update = CLowlaDBBson::create();
    CLowlaDBBson::ptr updateSet = CLowlaDBBson::create();
    updateSet->appendString("myfield", "mystringMod");
    updateSet->appendString("myfield2", "mystring2");
    updateSet->finish();
    update->appendObject("$set", updateSet->data());
    update->finish();
    
    CLowlaDBWriteResult::ptr wrUpdate = coll->update(query->data(), update->data(), false, true);
    EXPECT_EQ(1, wrUpdate->getN());
    EXPECT_TRUE(wrUpdate->isUpdateOfExisting());
    
    // Make sure we updated the document correctly, replacing existing fields and not changing _id
    CLowlaDBCursor::ptr cursor = CLowlaDBCursor::create(coll, nullptr);
    CLowlaDBBson::ptr check = cursor->next();
    EXPECT_TRUE(check->containsKey("myfield"));
    EXPECT_EQ("mystringMod", check->stringForKey("myfield"));
    EXPECT_TRUE(check->containsKey("myfield2"));
    EXPECT_EQ("mystring2", check->stringForKey("myfield2"));
    char bufCheck[CLowlaDBBson::OID_SIZE];
    EXPECT_TRUE(check->oidForKey("_id", bufCheck));
    EXPECT_TRUE(0 == memcmp(bufCheck, buf1, CLowlaDBBson::OID_SIZE));
    
    // Make sure we didn't touch the next document
    check = cursor->next();
    EXPECT_TRUE(check->containsKey("myfield"));
    EXPECT_EQ("mystring", check->stringForKey("myfield"));
    EXPECT_FALSE(check->containsKey("myfield2"));
    
    // Make sure we didn't create any unexpected documents
    check = cursor->next();
    EXPECT_FALSE(check);
}

class CountTestFixture : public DbTestFixture {
public:
    CountTestFixture();
};

CountTestFixture::CountTestFixture() {
    CLowlaDBBson::ptr bson = CLowlaDBBson::create();
    bson->appendInt("a", 1);
    bson->finish();
    coll->insert(bson->data());
    bson = CLowlaDBBson::create();
    bson->appendInt("a", 2);
    bson->finish();
    coll->insert(bson->data());
    bson = CLowlaDBBson::create();
    bson->appendInt("a", 3);
    bson->finish();
    coll->insert(bson->data());
}

TEST_F(CountTestFixture, test_cursor_count_all) {
    CLowlaDBCursor::ptr cursor = CLowlaDBCursor::create(coll, nullptr);
    EXPECT_EQ(3, cursor->count());
}

TEST_F(CountTestFixture, test_cursor_count_emptyFilter) {
    CLowlaDBBson::ptr filter = CLowlaDBBson::create();
    filter->finish();
    CLowlaDBCursor::ptr cursor = CLowlaDBCursor::create(coll, filter->data());
    EXPECT_EQ(3, cursor->count());
}

TEST_F(CountTestFixture, test_cursor_count_filter) {
    CLowlaDBBson::ptr filter = CLowlaDBBson::create();
    filter->appendInt("a", 2);
    filter->finish();
    CLowlaDBCursor::ptr cursor = CLowlaDBCursor::create(coll, filter->data());
    EXPECT_EQ(1, cursor->count());
}

TEST_F(CountTestFixture, test_cursor_count_filter_nomatch) {
    CLowlaDBBson::ptr filter = CLowlaDBBson::create();
    filter->appendInt("z", 2);
    filter->finish();
    CLowlaDBCursor::ptr cursor = CLowlaDBCursor::create(coll, filter->data());
    EXPECT_EQ(0, cursor->count());
}

TEST_F(CountTestFixture, test_cursor_count_limit) {
    CLowlaDBCursor::ptr cursor = CLowlaDBCursor::create(coll, nullptr);
    cursor = cursor->limit(2);
    EXPECT_EQ(2, cursor->count());
}

TEST_F(CountTestFixture, test_cursor_count_skip) {
    CLowlaDBCursor::ptr cursor = CLowlaDBCursor::create(coll, nullptr);
    cursor = cursor->skip(1);
    EXPECT_EQ(2, cursor->count());
}

TEST_F(CountTestFixture, test_cursor_count_skip_limit) {
    CLowlaDBCursor::ptr cursor = CLowlaDBCursor::create(coll, nullptr);
    cursor = cursor->skip(1)->limit(1);
    EXPECT_EQ(1, cursor->count());
}

TEST_F(DbTestFixture, test_parse_syncer_response) {
    CLowlaDBBson::ptr syncResponse = CLowlaDBBson::create();
    
    syncResponse->appendInt("sequence", 2);
    syncResponse->startArray("atoms");
    syncResponse->startObject("0");
    syncResponse->appendString("id", "1234");
    syncResponse->appendInt("sequence", 1);
    syncResponse->appendInt("version", 1);
    syncResponse->appendBool("deleted", false);
    syncResponse->finishObject();
    syncResponse->finishArray();
    syncResponse->finish();
    
    CLowlaDBPullData::ptr pd = lowladb_parse_syncer_response(syncResponse->data());
    EXPECT_FALSE(pd->hasRequestMore());
    EXPECT_FALSE(pd->isComplete());
    EXPECT_FALSE(pd->hasRequestMore());
    EXPECT_EQ("", pd->getRequestMore());
    EXPECT_EQ(1, pd->getSequenceForNextRequest());
}

TEST_F(DbTestFixture, test_pull_new_document) {
    CLowlaDBBson::ptr syncResponse = lowladb_json_to_bson("{\"sequence\" : 2, \"atoms\" : [ {\"id\" : \"serverdb.servercoll$1234\", \"sequence\" : 1, \"version\" : 1, \"deleted\" : false }]}");
    
    CLowlaDBPullData::ptr pd = lowladb_parse_syncer_response(syncResponse->data());

    CLowlaDBBson::ptr meta = CLowlaDBBson::create();
    CLowlaDBBson::ptr data = CLowlaDBBson::create();
    
    meta->appendString("id", "serverdb.servercoll$1234");
    meta->appendString("clientNs", "mydb.mycoll");
    meta->finish();
    
    data->appendString("myfield", "mystring");
    data->appendString("_id", "1234");
    data->finish();
    
    std::vector<CLowlaDBBson::ptr> response;
    response.push_back(meta);
    response.push_back(data);
    lowladb_apply_pull_response(response, pd);
    
    // Make sure we created the document
    CLowlaDBCursor::ptr cursor = CLowlaDBCursor::create(coll, nullptr);
    CLowlaDBBson::ptr check = cursor->next();
    EXPECT_TRUE(check->containsKey("myfield"));
    EXPECT_EQ("mystring", check->stringForKey("myfield"));
    
    // Make sure we updated the pull data
    EXPECT_TRUE(pd->isComplete());
    EXPECT_EQ(2, pd->getSequenceForNextRequest());
}

TEST_F(DbTestFixture, test_pull_existing_document) {
    
}

TEST_F(DbTestFixture, test_pull_modified_document) {
    
}

TEST_F(DbTestFixture, test_parse_syncer_response_json) {
    CLowlaDBBson::ptr syncResponse = lowladb_json_to_bson("{\"sequence\" : 2, \"atoms\" : [ {\"id\" : \"1234\", \"sequence\" : 1, \"version\" : 1, \"deleted\" : false }]}");
    
    CLowlaDBPullData::ptr pd = lowladb_parse_syncer_response(syncResponse->data());
    EXPECT_FALSE(pd->hasRequestMore());
    EXPECT_FALSE(pd->isComplete());
    EXPECT_FALSE(pd->hasRequestMore());
    EXPECT_EQ("", pd->getRequestMore());
    EXPECT_EQ(1, pd->getSequenceForNextRequest());
}

TEST(BsonToJson, testString) {
    CLowlaDBBson::ptr value = CLowlaDBBson::create();
    value->appendString("mystring", "my value");
    value->finish();
    
    EXPECT_EQ("{\n   \"mystring\" : \"my value\"\n}\n", lowladb_bson_to_json(value->data()));
}

TEST(JsonToBson, testString) {
    CLowlaDBBson::ptr bson = lowladb_json_to_bson("{\"mystring\" : \"myvalue\"}");
    EXPECT_EQ("myvalue", bson->stringForKey("mystring"));
}

TEST(JsonToBson, testInt) {
    CLowlaDBBson::ptr bson = lowladb_json_to_bson("{\"myint\" : 7}");
    int check = 0;
    EXPECT_TRUE(bson->intForKey("myint", &check));
    EXPECT_EQ(7, check);
}

TEST(JsonToBson, testArray) {
    CLowlaDBBson::ptr bson = lowladb_json_to_bson("{\"myarr\" : [13,27]}");
    CLowlaDBBson::ptr arr;
    EXPECT_TRUE(bson->arrayForKey("myarr", &arr));
    int check = 0;
    EXPECT_TRUE(arr->intForKey("0", &check));
    EXPECT_EQ(13, check);
    EXPECT_TRUE(arr->intForKey("1", &check));
    EXPECT_EQ(27, check);
    EXPECT_FALSE(arr->intForKey("2", &check));
}

TEST(JsonToBson, testObject) {
    CLowlaDBBson::ptr bson = lowladb_json_to_bson("{\"myobj\" : {\"b\" : 3, \"a\" : 4}}");
    CLowlaDBBson::ptr obj;
    EXPECT_TRUE(bson->objectForKey("myobj", &obj));
    int check = 0;
    EXPECT_TRUE(obj->intForKey("a", &check));
    EXPECT_EQ(4, check);
    EXPECT_TRUE(obj->intForKey("b", &check));
    EXPECT_EQ(3, check);
}

TEST(JsonToBson, testDate) {
    CLowlaDBBson::ptr bson = lowladb_json_to_bson("{\"mydate\" : {\"_bsonType\" : \"Date\", \"millis\" : 1414782231657}}");
    int64_t millis;
    EXPECT_TRUE(bson->dateForKey("mydate", &millis));
    EXPECT_EQ(1414782231657, millis);
}