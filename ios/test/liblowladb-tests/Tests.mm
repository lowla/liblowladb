//
//  Tests.mm
//  liblowladb-tests
//
//  Created by mark on 8/27/14.
//  Copyright (c) 2014 Lowla. All rights reserved.
//

#import <XCTest/XCTest.h>
#import "gtest.h"

@interface GoogleTestSuite : XCTestSuite
@end

@interface GoogleTests : XCTestCase
{
@private NSString *_name;
}
- (id)initWithName:(NSString *)name;
- (NSString *)name;

@end

class GTestRunCollector : public ::testing::EmptyTestEventListener
{
public:
    GTestRunCollector(XCTestSuiteRun *gtsr);
    
    virtual void OnTestProgramStart(const ::testing::UnitTest& unit_test);
    virtual void OnTestStart(const ::testing::TestInfo& test_info);
    virtual void OnTestPartResult(const ::testing::TestPartResult& test_part_result);
    virtual void OnTestEnd(const ::testing::TestInfo& test_info);
    virtual void OnTestProgramEnd(const ::testing::UnitTest& unit_test);
    
private:
    XCTestSuiteRun *m_gtsr;
    XCTest *m_tc;
    XCTestCaseRun *m_tcr;
};

GTestRunCollector::GTestRunCollector(XCTestSuiteRun *gtsr) : m_gtsr(gtsr)
{
}

void GTestRunCollector::OnTestProgramStart(const testing::UnitTest &unit_test)
{
    [m_gtsr start];
}

void GTestRunCollector::OnTestStart(const ::testing::TestInfo &test_info)
{
    NSString *testName = [NSString stringWithFormat:@"%s.%s", test_info.test_case_name(), test_info.name()];
    m_tc = [[GoogleTests alloc] initWithName:testName];
    m_tcr = [XCTestCaseRun testRunWithTest:m_tc];
    [m_tcr start];
}

void GTestRunCollector::OnTestPartResult(const ::testing::TestPartResult &test_part_result)
{
    if (test_part_result.failed()) {
        [m_tcr recordFailureWithDescription:@(test_part_result.message()) inFile:@(test_part_result.file_name()) atLine:test_part_result.line_number() expected:YES];
    }
}

void GTestRunCollector::OnTestEnd(const ::testing::TestInfo &test_info)
{
    [m_tcr stop];
    [m_gtsr addTestRun:m_tcr];
}

void GTestRunCollector::OnTestProgramEnd(const ::testing::UnitTest &unit_test)
{
    [m_gtsr stop];
}

@implementation GoogleTestSuite

+ (id)testSuite {
    return [[GoogleTestSuite alloc] init];
}

// This is the name written to the test run output log. Xcode doesn't display it.
// XCode displays the value of [GoogleTests testClassName]
- (NSString *)name {
    return @"GoogleTests";
}

- (void)performTest:(XCTestRun *)run {
    int argc = 0;
    char **argv = nullptr;
    
    ::testing::InitGoogleTest(&argc, argv);
    
    ::testing::TestEventListeners& listeners = ::testing::UnitTest::GetInstance()->listeners();
    // Remove the standard output writer
    delete listeners.Release(listeners.default_result_printer());
    // Add our own XCUnit-compatible version.  Google Test takes ownership.
    listeners.Append(new GTestRunCollector((XCTestSuiteRun *)run));
    
    RUN_ALL_TESTS();
}

- (XCTestRun *)run {
    return [super run];
}

@end

@implementation GoogleTests

+ (XCTestSuite *)defaultTestSuite {
    XCTestSuite *answer = [GoogleTestSuite testSuite];
    return answer;
}

- (id)initWithName:(NSString *)name {
    if (self = [super init]) {
        _name = name;
    }
    return self;
}

// This is the name written to the test run output log. Xcode doesn't display it.
// XCode displays the value of testMethodName.
- (NSString *)name {
    return [NSString stringWithFormat:@"-[GoogleTests %@]", _name];
}

- (NSString *)testClassName {
    return @"GoogleTests";
}

- (NSString *)testMethodName {
    return _name;
}

@end
