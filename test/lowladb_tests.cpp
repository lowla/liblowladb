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
    CLowlaDBCursor::ptr cursor = CLowlaDBCursor::create(coll);
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
    CLowlaDBCursor::ptr cursor = CLowlaDBCursor::create(coll);
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
