//
//  lowladb_tests.cpp
//  liblowladb-apple-tests
//
//  Created by mark on 7/3/14.
//  Copyright (c) 2014 Lowla. All rights reserved.
//

#include "gtest.h"

#include "TeamstudioException.h"
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
    EXPECT_EQ(1, wr->documentCount());
    EXPECT_TRUE(wr->document(0)->oidForKey("_id", buffer));
}

TEST_F(DbTestFixture, test_create_new_id_for_each_document) {
    CLowlaDBBson::ptr bson = CLowlaDBBson::create();
    bson->appendString("myfield", "mystring");
    bson->finish();
    CLowlaDBWriteResult::ptr wr1 = coll->insert(bson->data());
    CLowlaDBWriteResult::ptr wr2 = coll->insert(bson->data());
    char buf1[CLowlaDBBson::OID_SIZE];
    char buf2[CLowlaDBBson::OID_SIZE];
    EXPECT_EQ(1, wr1->documentCount());
    EXPECT_TRUE(wr1->document(0)->oidForKey("_id", buf1));
    EXPECT_EQ(1, wr2->documentCount());
    EXPECT_TRUE(wr2->document(0)->oidForKey("_id", buf2));

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

TEST_F(DbTestFixture, test_cannot_insert_dollar_field) {
    CLowlaDBBson::ptr bson = CLowlaDBBson::create();
    bson->appendString("$myfield", "mystring");
    bson->finish();
    try {
        CLowlaDBWriteResult::ptr wr1 = coll->insert(bson->data());
        FAIL();
    }
    catch (TeamstudioException const &e) {
        EXPECT_TRUE(nullptr != strstr(e.what(), "$myfield"));
    }
    CLowlaDBCursor::ptr cursor = CLowlaDBCursor::create(coll, nullptr);
    EXPECT_EQ(0, cursor->count());
}

TEST_F(DbTestFixture, test_cannot_insert_dollar_field_via_array) {
    CLowlaDBBson::ptr bson = CLowlaDBBson::create();
    bson->appendString("myfield", "mystring");
    bson->finish();
    CLowlaDBBson::ptr bsonBad = CLowlaDBBson::create();
    bsonBad->appendString("$myfield", "mystring");
    bsonBad->finish();
    std::vector<const char *> arr;
    arr.push_back(bson->data());
    arr.push_back(bsonBad->data());
    try {
        CLowlaDBWriteResult::ptr wr = coll->insert(arr);
        FAIL();
    }
    catch (TeamstudioException &e) {
        EXPECT_TRUE(nullptr != strstr(e.what(), "$myfield"));
    }
    // We don't insert any records even if only one of them is bad
    CLowlaDBCursor::ptr cursor = CLowlaDBCursor::create(coll, nullptr);
    EXPECT_EQ(0, cursor->count());
}

TEST_F(DbTestFixture, test_update_replace_single_doc) {
    CLowlaDBBson::ptr bson = CLowlaDBBson::create();
    bson->appendString("myfield", "mystring");
    bson->finish();
    CLowlaDBWriteResult::ptr wr1 = coll->insert(bson->data());
    coll->insert(bson->data());
    char buf1[CLowlaDBBson::OID_SIZE];
    EXPECT_EQ(1, wr1->documentCount());
    EXPECT_TRUE(wr1->document(0)->oidForKey("_id", buf1));
    
    CLowlaDBBson::ptr query = CLowlaDBBson::create();
    query->appendOid("_id", buf1);
    query->finish();
    CLowlaDBBson::ptr update = CLowlaDBBson::create();
    update->appendString("myfield2", "mystring2");
    update->finish();
    
    CLowlaDBWriteResult::ptr wrUpdate = coll->update(query->data(), update->data(), false, true);
    
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
    bson->appendString("myfield3", "mystring3");
    bson->finish();
    CLowlaDBWriteResult::ptr wr1 = coll->insert(bson->data());
    coll->insert(bson->data());
    EXPECT_EQ(1, wr1->documentCount());
    char buf1[CLowlaDBBson::OID_SIZE];
    EXPECT_TRUE(wr1->document(0)->oidForKey("_id", buf1));
    
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
    
    // Make sure we updated the document correctly, replacing existing fields and not changing _id
    CLowlaDBCursor::ptr cursor = CLowlaDBCursor::create(coll, nullptr);
    CLowlaDBBson::ptr check = cursor->next();
    EXPECT_TRUE(check->containsKey("myfield"));
    EXPECT_EQ("mystringMod", check->stringForKey("myfield"));
    EXPECT_TRUE(check->containsKey("myfield2"));
    EXPECT_EQ("mystring2", check->stringForKey("myfield2"));
    EXPECT_TRUE(check->containsKey("myfield3"));
    EXPECT_EQ("mystring3", check->stringForKey("myfield3"));
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

TEST_F(DbTestFixture, test_collection_cant_update_dollar_field)
{
    CLowlaDBBson::ptr update = CLowlaDBBson::create();
    update->appendInt("$badfield", 2);
    update->finish();
    
    CLowlaDBBson::ptr query = CLowlaDBBson::empty();
    
    try {
        coll->update(query->data(), update->data(), false, true);
    }
    catch (TeamstudioException const &e) {
        EXPECT_TRUE(nullptr != strstr(e.what(), "$badfield"));
    }
}

TEST_F(DbTestFixture, test_collection_cant_update_dollar_field_via_set)
{
    CLowlaDBBson::ptr update = CLowlaDBBson::create();
    CLowlaDBBson::ptr updateSet = CLowlaDBBson::create();
    updateSet->appendInt("$badfield", 2);
    updateSet->finish();
    update->appendObject("$set", updateSet->data());
    update->finish();
    
    CLowlaDBBson::ptr query = CLowlaDBBson::empty();
    
    try {
        coll->update(query->data(), update->data(), false, true);
    }
    catch (TeamstudioException const &e) {
        EXPECT_TRUE(nullptr != strstr(e.what(), "$badfield"));
    }
}

TEST_F(DbTestFixture, test_collection_cant_mix_dollar_first)
{
    CLowlaDBBson::ptr update = CLowlaDBBson::create();
    CLowlaDBBson::ptr updateSet = CLowlaDBBson::create();
    updateSet->appendInt("sub", 2);
    updateSet->finish();
    update->appendObject("$set", updateSet->data());
    update->appendString("notDollar", "mystring");
    update->finish();
    
    CLowlaDBBson::ptr query = CLowlaDBBson::empty();
    
    try {
        coll->update(query->data(), update->data(), false, true);
    }
    catch (TeamstudioException const &e) {
        EXPECT_TRUE(nullptr != strstr(e.what(), "Can not mix"));
    }
}

TEST_F(DbTestFixture, test_collection_cant_mix_dollar_second)
{
    CLowlaDBBson::ptr update = CLowlaDBBson::create();
    update->appendString("notDollar", "mystring");
    CLowlaDBBson::ptr updateSet = CLowlaDBBson::create();
    updateSet->appendInt("sub", 2);
    updateSet->finish();
    update->appendObject("$set", updateSet->data());
    update->finish();
    
    CLowlaDBBson::ptr query = CLowlaDBBson::empty();
    
    try {
        coll->update(query->data(), update->data(), false, true);
    }
    catch (TeamstudioException const &e) {
        EXPECT_TRUE(nullptr != strstr(e.what(), "Can not mix"));
    }
}

TEST_F(DbTestFixture, test_collection_can_remove_a_document)
{
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

    bson = CLowlaDBBson::create();
    bson->appendInt("a", 2);
    bson->finish();
    
    CLowlaDBWriteResult::ptr wr = coll->remove(bson->data());
    EXPECT_EQ(1, wr->documentCount());
    
    CLowlaDBCursor::ptr cursor = CLowlaDBCursor::create(coll, nullptr);
    CLowlaDBBson::ptr doc = cursor->next();
    int val;
    EXPECT_TRUE(doc->intForKey("a", &val));
    EXPECT_EQ(1, val);
    doc = cursor->next();
    EXPECT_TRUE(doc->intForKey("a", &val));
    EXPECT_EQ(3, val);
    EXPECT_FALSE(cursor->next());
}

TEST_F(DbTestFixture, test_collection_can_remove_zero_documents)
{
    CLowlaDBBson::ptr bson = CLowlaDBBson::create();
    bson->appendInt("a", 1);
    bson->finish();
    coll->insert(bson->data());
    bson = CLowlaDBBson::create();
    bson->appendInt("b", 2);
    bson->finish();
    coll->insert(bson->data());
    bson = CLowlaDBBson::create();
    bson->appendInt("c", 3);
    bson->finish();
    coll->insert(bson->data());
    
    bson = CLowlaDBBson::create();
    bson->appendInt("d", 4);
    bson->finish();
    
    CLowlaDBWriteResult::ptr wr = coll->remove(bson->data());
    EXPECT_EQ(0, wr->documentCount());
    
    CLowlaDBCursor::ptr cursor = CLowlaDBCursor::create(coll, nullptr);
    CLowlaDBBson::ptr doc = cursor->next();
    int val;
    EXPECT_TRUE(doc->intForKey("a", &val));
    EXPECT_EQ(1, val);
    doc = cursor->next();
    EXPECT_TRUE(doc->intForKey("b", &val));
    EXPECT_EQ(2, val);
    doc = cursor->next();
    EXPECT_TRUE(doc->intForKey("c", &val));
    EXPECT_EQ(3, val);
    EXPECT_FALSE(cursor->next());
}

TEST_F(DbTestFixture, test_collection_can_remove_all_documents)
{
    CLowlaDBBson::ptr bson = CLowlaDBBson::create();
    bson->appendInt("a", 1);
    bson->finish();
    coll->insert(bson->data());
    bson = CLowlaDBBson::create();
    bson->appendInt("b", 2);
    bson->finish();
    coll->insert(bson->data());
    bson = CLowlaDBBson::create();
    bson->appendInt("c", 3);
    bson->finish();
    coll->insert(bson->data());
    
    CLowlaDBWriteResult::ptr wr = coll->remove(nullptr);
    EXPECT_EQ(3, wr->documentCount());
    
    CLowlaDBCursor::ptr cursor = CLowlaDBCursor::create(coll, nullptr);
    EXPECT_FALSE(cursor->next());
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

TEST_F(DbTestFixture, test_cursor_sort_int_ascending) {
    CLowlaDBBson::ptr bson = CLowlaDBBson::create();
    bson->appendInt("a", 2);
    bson->finish();
    coll->insert(bson->data());
    bson = CLowlaDBBson::create();
    bson->appendInt("a", 1);
    bson->finish();
    coll->insert(bson->data());
    
    bson = CLowlaDBBson::create();
    bson->appendInt("a", 1);
    bson->finish();
    
    CLowlaDBCursor::ptr cursor = CLowlaDBCursor::create(coll, nullptr);
    cursor = cursor->sort(bson->data());
    CLowlaDBBson::ptr doc = cursor->next();
    int val;
    EXPECT_TRUE(doc->intForKey("a", &val));
    EXPECT_EQ(1, val);
    doc = cursor->next();
    EXPECT_TRUE(doc->intForKey("a", &val));
    EXPECT_EQ(2, val);
    EXPECT_FALSE(cursor->next());
}

TEST_F(DbTestFixture, test_cursor_sort_int_descending) {
    CLowlaDBBson::ptr bson = CLowlaDBBson::create();
    bson->appendInt("a", 1);
    bson->finish();
    coll->insert(bson->data());
    bson = CLowlaDBBson::create();
    bson->appendInt("a", 2);
    bson->finish();
    coll->insert(bson->data());
    
    bson = CLowlaDBBson::create();
    bson->appendInt("a", -1);
    bson->finish();
    
    CLowlaDBCursor::ptr cursor = CLowlaDBCursor::create(coll, nullptr);
    cursor = cursor->sort(bson->data());
    CLowlaDBBson::ptr doc = cursor->next();
    int val;
    EXPECT_TRUE(doc->intForKey("a", &val));
    EXPECT_EQ(2, val);
    doc = cursor->next();
    EXPECT_TRUE(doc->intForKey("a", &val));
    EXPECT_EQ(1, val);
    EXPECT_FALSE(cursor->next());
}

TEST_F(DbTestFixture, test_cursor_sort_string_ascending) {
    CLowlaDBBson::ptr bson = CLowlaDBBson::create();
    bson->appendString("a", "beta");
    bson->finish();
    coll->insert(bson->data());
    bson = CLowlaDBBson::create();
    bson->appendString("a", "alpha");
    bson->finish();
    coll->insert(bson->data());
    
    bson = CLowlaDBBson::create();
    bson->appendInt("a", 1);
    bson->finish();
    
    CLowlaDBCursor::ptr cursor = CLowlaDBCursor::create(coll, nullptr);
    cursor = cursor->sort(bson->data());
    CLowlaDBBson::ptr doc = cursor->next();
    EXPECT_EQ("alpha", doc->stringForKey("a"));
    doc = cursor->next();
    EXPECT_EQ("beta", doc->stringForKey("a"));
    EXPECT_FALSE(cursor->next());
}

TEST_F(DbTestFixture, test_cursor_sort_numbers_ascending) {
    CLowlaDBBson::ptr bson = CLowlaDBBson::create();
    bson->appendInt("a", 2);
    bson->finish();
    coll->insert(bson->data());
    bson = CLowlaDBBson::create();
    bson->appendDouble("a", 1.5);
    bson->finish();
    coll->insert(bson->data());
    bson = CLowlaDBBson::create();
    bson->appendLong("a", 1L);
    bson->finish();
    coll->insert(bson->data());
    
    bson = CLowlaDBBson::create();
    bson->appendInt("a", 1);
    bson->finish();
    
    CLowlaDBCursor::ptr cursor = CLowlaDBCursor::create(coll, nullptr);
    cursor = cursor->sort(bson->data());
    CLowlaDBBson::ptr doc = cursor->next();
    int iVal;
    int64_t lVal;
    double dVal;
    EXPECT_TRUE(doc->longForKey("a", &lVal));
    EXPECT_EQ(1, lVal);
    doc = cursor->next();
    EXPECT_TRUE(doc->doubleForKey("a", &dVal));
    EXPECT_EQ(1.5, dVal);
    doc = cursor->next();
    EXPECT_TRUE(doc->intForKey("a", &iVal));
    EXPECT_EQ(2, iVal);
    EXPECT_FALSE(cursor->next());
}

TEST_F(DbTestFixture, test_cursor_sort_int_before_string) {
    CLowlaDBBson::ptr bson = CLowlaDBBson::create();
    bson->appendString("a", "beta");
    bson->finish();
    coll->insert(bson->data());
    bson = CLowlaDBBson::create();
    bson->appendInt("a", 2);
    bson->finish();
    coll->insert(bson->data());
    
    bson = CLowlaDBBson::create();
    bson->appendInt("a", 1);
    bson->finish();
    
    CLowlaDBCursor::ptr cursor = CLowlaDBCursor::create(coll, nullptr);
    cursor = cursor->sort(bson->data());
    CLowlaDBBson::ptr doc = cursor->next();
    int val;
    EXPECT_TRUE(doc->intForKey("a", &val));
    EXPECT_EQ(2, val);
    doc = cursor->next();
    EXPECT_EQ("beta", doc->stringForKey("a"));
    EXPECT_FALSE(cursor->next());
}

TEST_F(DbTestFixture, test_cursor_sort_missing_before_int) {
    CLowlaDBBson::ptr bson = CLowlaDBBson::create();
    bson->appendString("b", "beta");
    bson->finish();
    coll->insert(bson->data());
    bson = CLowlaDBBson::create();
    bson->appendInt("a", 2);
    bson->finish();
    coll->insert(bson->data());
    
    bson = CLowlaDBBson::create();
    bson->appendInt("a", 1);
    bson->finish();
    
    CLowlaDBCursor::ptr cursor = CLowlaDBCursor::create(coll, nullptr);
    cursor = cursor->sort(bson->data());
    CLowlaDBBson::ptr doc = cursor->next();
    EXPECT_EQ("beta", doc->stringForKey("b"));
    doc = cursor->next();
    int val;
    EXPECT_TRUE(doc->intForKey("a", &val));
    EXPECT_EQ(2, val);
    EXPECT_FALSE(cursor->next());
}

TEST_F(DbTestFixture, test_cursor_sort_int_ascending_ascending) {
    CLowlaDBBson::ptr bson = CLowlaDBBson::create();
    bson->appendInt("a", 1);
    bson->appendInt("b", 10);
    bson->finish();
    coll->insert(bson->data());
    bson = CLowlaDBBson::create();
    bson->appendInt("a", 2);
    bson->appendInt("b", 30);
    bson->finish();
    coll->insert(bson->data());
    bson = CLowlaDBBson::create();
    bson->appendInt("a", 2);
    bson->appendInt("b", 20);
    bson->finish();
    coll->insert(bson->data());
    
    bson = CLowlaDBBson::create();
    bson->appendInt("a", 1);
    bson->appendInt("b", 1);
    bson->finish();
    
    CLowlaDBCursor::ptr cursor = CLowlaDBCursor::create(coll, nullptr);
    cursor = cursor->sort(bson->data());
    CLowlaDBBson::ptr doc = cursor->next();
    int val;
    EXPECT_TRUE(doc->intForKey("a", &val));
    EXPECT_EQ(1, val);
    doc = cursor->next();
    EXPECT_TRUE(doc->intForKey("a", &val));
    EXPECT_EQ(2, val);
    EXPECT_TRUE(doc->intForKey("b", &val));
    EXPECT_EQ(20, val);
    doc = cursor->next();
    EXPECT_TRUE(doc->intForKey("a", &val));
    EXPECT_EQ(2, val);
    EXPECT_TRUE(doc->intForKey("b", &val));
    EXPECT_EQ(30, val);
    EXPECT_FALSE(cursor->next());
}

TEST_F(DbTestFixture, test_cursor_sort_int_ascending_descending) {
    CLowlaDBBson::ptr bson = CLowlaDBBson::create();
    bson->appendInt("a", 1);
    bson->appendInt("b", 10);
    bson->finish();
    coll->insert(bson->data());
    bson = CLowlaDBBson::create();
    bson->appendInt("a", 2);
    bson->appendInt("b", 20);
    bson->finish();
    coll->insert(bson->data());
    bson = CLowlaDBBson::create();
    bson->appendInt("a", 2);
    bson->appendInt("b", 30);
    bson->finish();
    coll->insert(bson->data());
    
    bson = CLowlaDBBson::create();
    bson->appendInt("a", 1);
    bson->appendInt("b", -1);
    bson->finish();
    
    CLowlaDBCursor::ptr cursor = CLowlaDBCursor::create(coll, nullptr);
    cursor = cursor->sort(bson->data());
    CLowlaDBBson::ptr doc = cursor->next();
    int val;
    EXPECT_TRUE(doc->intForKey("a", &val));
    EXPECT_EQ(1, val);
    doc = cursor->next();
    EXPECT_TRUE(doc->intForKey("a", &val));
    EXPECT_EQ(2, val);
    EXPECT_TRUE(doc->intForKey("b", &val));
    EXPECT_EQ(30, val);
    doc = cursor->next();
    EXPECT_TRUE(doc->intForKey("a", &val));
    EXPECT_EQ(2, val);
    EXPECT_TRUE(doc->intForKey("b", &val));
    EXPECT_EQ(20, val);
    EXPECT_FALSE(cursor->next());
}

TEST_F(DbTestFixture, test_cursor_sort_int_descending_ascending) {
    CLowlaDBBson::ptr bson = CLowlaDBBson::create();
    bson->appendInt("a", 1);
    bson->appendInt("b", 10);
    bson->finish();
    coll->insert(bson->data());
    bson = CLowlaDBBson::create();
    bson->appendInt("a", 2);
    bson->appendInt("b", 20);
    bson->finish();
    coll->insert(bson->data());
    bson = CLowlaDBBson::create();
    bson->appendInt("a", 2);
    bson->appendInt("b", 30);
    bson->finish();
    coll->insert(bson->data());
    
    bson = CLowlaDBBson::create();
    bson->appendInt("a", -1);
    bson->appendInt("b", 1);
    bson->finish();
    
    CLowlaDBCursor::ptr cursor = CLowlaDBCursor::create(coll, nullptr);
    cursor = cursor->sort(bson->data());
    CLowlaDBBson::ptr doc = cursor->next();
    int val;
    EXPECT_TRUE(doc->intForKey("a", &val));
    EXPECT_EQ(2, val);
    EXPECT_TRUE(doc->intForKey("b", &val));
    EXPECT_EQ(20, val);
    doc = cursor->next();
    EXPECT_TRUE(doc->intForKey("a", &val));
    EXPECT_EQ(2, val);
    EXPECT_TRUE(doc->intForKey("b", &val));
    EXPECT_EQ(30, val);
    doc = cursor->next();
    EXPECT_TRUE(doc->intForKey("a", &val));
    EXPECT_EQ(1, val);
    EXPECT_FALSE(cursor->next());
}

TEST_F(DbTestFixture, test_cursor_sort_int_descending_descending) {
    CLowlaDBBson::ptr bson = CLowlaDBBson::create();
    bson->appendInt("a", 1);
    bson->appendInt("b", 10);
    bson->finish();
    coll->insert(bson->data());
    bson = CLowlaDBBson::create();
    bson->appendInt("a", 2);
    bson->appendInt("b", 20);
    bson->finish();
    coll->insert(bson->data());
    bson = CLowlaDBBson::create();
    bson->appendInt("a", 2);
    bson->appendInt("b", 30);
    bson->finish();
    coll->insert(bson->data());
    
    bson = CLowlaDBBson::create();
    bson->appendInt("a", -1);
    bson->appendInt("b", -1);
    bson->finish();
    
    CLowlaDBCursor::ptr cursor = CLowlaDBCursor::create(coll, nullptr);
    cursor = cursor->sort(bson->data());
    CLowlaDBBson::ptr doc = cursor->next();
    int val;
    EXPECT_TRUE(doc->intForKey("a", &val));
    EXPECT_EQ(2, val);
    EXPECT_TRUE(doc->intForKey("b", &val));
    EXPECT_EQ(30, val);
    doc = cursor->next();
    EXPECT_TRUE(doc->intForKey("a", &val));
    EXPECT_EQ(2, val);
    EXPECT_TRUE(doc->intForKey("b", &val));
    EXPECT_EQ(20, val);
    doc = cursor->next();
    EXPECT_TRUE(doc->intForKey("a", &val));
    EXPECT_EQ(1, val);
    EXPECT_FALSE(cursor->next());
}

TEST_F(DbTestFixture, test_cursor_sort_skip_limit) {
    CLowlaDBBson::ptr bson = CLowlaDBBson::create();
    bson->appendInt("a", 1);
    bson->appendInt("b", 10);
    bson->finish();
    coll->insert(bson->data());
    bson = CLowlaDBBson::create();
    bson->appendInt("a", 2);
    bson->appendInt("b", 20);
    bson->finish();
    coll->insert(bson->data());
    bson = CLowlaDBBson::create();
    bson->appendInt("a", 2);
    bson->appendInt("b", 30);
    bson->finish();
    coll->insert(bson->data());
    
    bson = CLowlaDBBson::create();
    bson->appendInt("a", -1);
    bson->appendInt("b", -1);
    bson->finish();
    
    CLowlaDBCursor::ptr cursor = CLowlaDBCursor::create(coll, nullptr);
    cursor = cursor->sort(bson->data());
    cursor = cursor->skip(1)->limit(1);
    
    CLowlaDBBson::ptr doc = cursor->next();
    int val;
    EXPECT_TRUE(doc->intForKey("a", &val));
    EXPECT_EQ(2, val);
    EXPECT_TRUE(doc->intForKey("b", &val));
    EXPECT_EQ(20, val);
    EXPECT_FALSE(cursor->next());
}

TEST_F(DbTestFixture, test_cursor_unsorted_skip_limit) {
    CLowlaDBBson::ptr bson = CLowlaDBBson::create();
    bson->appendInt("a", 1);
    bson->appendInt("b", 10);
    bson->finish();
    coll->insert(bson->data());
    bson = CLowlaDBBson::create();
    bson->appendInt("a", 2);
    bson->appendInt("b", 20);
    bson->finish();
    coll->insert(bson->data());
    bson = CLowlaDBBson::create();
    bson->appendInt("a", 2);
    bson->appendInt("b", 30);
    bson->finish();
    coll->insert(bson->data());
    
    CLowlaDBCursor::ptr cursor = CLowlaDBCursor::create(coll, nullptr);
    cursor = cursor->skip(1)->limit(1);
    
    CLowlaDBBson::ptr doc = cursor->next();
    int val;
    EXPECT_TRUE(doc->intForKey("a", &val));
    EXPECT_EQ(2, val);
    EXPECT_TRUE(doc->intForKey("b", &val));
    EXPECT_EQ(20, val);
    EXPECT_FALSE(cursor->next());
    
    cursor = CLowlaDBCursor::create(coll, nullptr)->skip(1);
    doc = cursor->next();
    EXPECT_TRUE(!!cursor->next());
    EXPECT_FALSE(cursor->next());
    
    cursor = CLowlaDBCursor::create(coll, nullptr)->skip(3);
    doc = cursor->next();
    EXPECT_FALSE(doc);
    
    cursor = CLowlaDBCursor::create(coll, nullptr)->limit(2);
    EXPECT_TRUE(!!cursor->next());
    EXPECT_TRUE(!!cursor->next());
    EXPECT_FALSE(cursor->next());
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

TEST(BsonToJson, testDouble) {
    CLowlaDBBson::ptr value = CLowlaDBBson::create();
    value->appendDouble("pi", 3.14);
    value->finish();
    
    EXPECT_EQ("{\n   \"pi\" : 3.14\n}\n", lowladb_bson_to_json(value->data()));
}

TEST(BsonToJson, testString) {
    CLowlaDBBson::ptr value = CLowlaDBBson::create();
    value->appendString("mystring", "my value");
    value->finish();
    
    EXPECT_EQ("{\n   \"mystring\" : \"my value\"\n}\n", lowladb_bson_to_json(value->data()));
}

TEST(BsonToJson, testObject) {
    CLowlaDBBson::ptr value = CLowlaDBBson::create();
    value->startObject("myobject");
    value->appendString("mystring", "my value");
    value->appendDouble("pi", 3.14);
    value->finishObject();
    value->finish();
    
    EXPECT_EQ("{\n   \"myobject\" : {\n      \"mystring\" : \"my value\",\n      \"pi\" : 3.14\n   }\n}\n", lowladb_bson_to_json(value->data()));
}

TEST(BsonToJson, testArray) {
    CLowlaDBBson::ptr value = CLowlaDBBson::create();
    value->startArray("myarray");
    value->appendString("0", "my value");
    value->appendDouble("1", 3.14);
    value->finishArray();
    value->finish();
    
    EXPECT_EQ("{\n   \"myarray\" : [ \"my value\", 3.14 ]\n}\n", lowladb_bson_to_json(value->data()));
}

TEST(BsonToJson, testBool) {
    CLowlaDBBson::ptr value = CLowlaDBBson::create();
    value->appendBool("mytrue", true);
    value->appendBool("myfalse", false);
    value->finish();
    
    EXPECT_EQ("{\n   \"myfalse\" : false,\n   \"mytrue\" : true\n}\n", lowladb_bson_to_json(value->data()));
}

TEST(BsonToJson, testObjectId) {
    CLowlaDBBson::ptr value = CLowlaDBBson::create();
    char oid[CLowlaDBBson::OID_SIZE];
    CLowlaDBBson::oidGenerate(oid);
    value->appendOid("_id", oid);
    value->finish();
    
    char buf[CLowlaDBBson::OID_STRING_SIZE];
    CLowlaDBBson::oidToString(oid, buf);
    
    utf16string check("{\n   \"_id\" : {\n      \"_bsonType\" : \"ObjectId\",\n      \"hexString\" : \"%hex%\"\n   }\n}\n");
    check = check.replace("%hex%", buf);
    EXPECT_EQ(check, lowladb_bson_to_json(value->data()));
    
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

TEST(JsonToBson, testObjectId) {
    CLowlaDBBson::ptr bson = lowladb_json_to_bson("{\"_id\" : {\"_bsonType\" : \"ObjectId\", \"hexString\" : \"0123456789ABCDEF01234567\"}}");
    char oid[CLowlaDBBson::OID_SIZE];
    EXPECT_TRUE(bson->oidForKey("_id", oid));
    char buf[CLowlaDBBson::OID_STRING_SIZE];
    CLowlaDBBson::oidToString(oid, buf);
    EXPECT_STREQ("0123456789abcdef01234567", buf);
}