//
//  Tests.mm
//  liblowladb-tests
//
//  Created by mark on 8/27/14.
//  Copyright (c) 2014 Lowla. All rights reserved.
//

#import <objc/runtime.h>
#import <XCTest/XCTest.h>
#import "gtest.h"

@interface _GTestCase : XCTestCase
@property NSString *name;

- (id)initWithName:(NSString *)name fromSuite:(NSString *)suiteName;
+ (id)testCaseWithName:(NSString *)name fromSuite:(NSString *)suiteName;

@end

@implementation _GTestCase

+ (void)load
{
    [[NSUserDefaults standardUserDefaults] setValue:@"XCTestLog,XCTestRunCaptureTestObserver"
                                             forKey:@"XCTestObserverClass"];
}

- (id)initWithName:(NSString *)name fromSuite:(NSString *)suiteName
{
    if (self = [super init]) {
        _name = [NSString stringWithFormat:@"-[%@ %@]", suiteName, name];
    }
    return self;
}

+ (id)testCaseWithName:(NSString *)name fromSuite:(NSString *)suiteName
{
    return [[self alloc] initWithName:name fromSuite:suiteName];
}

@end

class XCUnitPrinter : public ::testing::EmptyTestEventListener
{
public:
    XCUnitPrinter(XCTestSuiteRun *gtsr);
    
    virtual void OnTestProgramStart(const ::testing::UnitTest& unit_test);
    virtual void OnTestCaseStart(const ::testing::TestCase& test_case);
    virtual void OnTestStart(const ::testing::TestInfo& test_info);
    virtual void OnTestPartResult(const ::testing::TestPartResult& test_part_result);
    virtual void OnTestEnd(const ::testing::TestInfo& test_info);
    virtual void OnTestCaseEnd(const ::testing::TestCase& test_case);
    virtual void OnTestProgramEnd(const ::testing::UnitTest& unit_test);
    
private:
    XCTestSuiteRun *m_gtsr;
    XCTestSuite *m_ts;
    XCTestSuiteRun *m_tsr;
    XCTestCase *m_tc;
    XCTestCaseRun *m_tcr;
};

XCUnitPrinter::XCUnitPrinter(XCTestSuiteRun *gtsr) : m_gtsr(gtsr)
{
}

void XCUnitPrinter::OnTestProgramStart(const testing::UnitTest &unit_test)
{
    [m_gtsr start];
}

void XCUnitPrinter::OnTestCaseStart(const ::testing::TestCase &test_case)
{
    m_ts = [XCTestSuite testSuiteWithName:@(test_case.name())];
    m_tsr = [XCTestSuiteRun testRunWithTest:m_ts];
    [m_tsr start];
}

void XCUnitPrinter::OnTestStart(const ::testing::TestInfo &test_info)
{
    m_tc = [_GTestCase testCaseWithName:@(test_info.name()) fromSuite:[m_ts name]];
    m_tcr = [XCTestCaseRun testRunWithTest:m_tc];
    [m_ts addTest:m_tc];
    [m_tsr addTestRun:m_tcr];
    [m_tcr start];
}

void XCUnitPrinter::OnTestPartResult(const ::testing::TestPartResult &test_part_result)
{
    if (test_part_result.failed()) {
        [m_tcr recordFailureInTest:m_tc withDescription:@(test_part_result.message()) inFile:@(test_part_result.file_name()) atLine:test_part_result.line_number() expected:YES];
    }
}

void XCUnitPrinter::OnTestEnd(const ::testing::TestInfo &test_info)
{
    [m_tcr stop];
}

void XCUnitPrinter::OnTestCaseEnd(const ::testing::TestCase& test_case)
{
    [m_tsr stop];
    [(XCTestSuite *)[m_gtsr test] addTest:m_ts];
    [m_gtsr addTestRun:m_tsr];
}

void XCUnitPrinter::OnTestProgramEnd(const ::testing::UnitTest &unit_test)
{
    [m_gtsr stop];
}

@interface XCTestRunCaptureTestObserver : XCTestObserver
@end

@implementation XCTestRunCaptureTestObserver

- (void)testSuiteDidStop:(XCTestRun *)testRun
{
    if (![testRun.test.name hasPrefix:@"_"]) {
        return;
    }
    int argc = 0;
    char **argv = nullptr;
    
    ::testing::InitGoogleTest(&argc, argv);
    
    XCTestSuiteRun *tsr = (XCTestSuiteRun *)testRun;
    
    XCTestSuite *gts = [XCTestSuite testSuiteWithName:@"Google Tests"];
    XCTestSuiteRun *gtsr = [XCTestSuiteRun testRunWithTest:gts];

    ::testing::TestEventListeners& listeners = ::testing::UnitTest::GetInstance()->listeners();
    // Remove the standard output writer
    delete listeners.Release(listeners.default_result_printer());
    // Add our own XCUnit-compatible version.  Google Test takes ownership.
    listeners.Append(new XCUnitPrinter(gtsr));
    
    RUN_ALL_TESTS();

    [(XCTestSuite *)[tsr test] addTest:gts];
    [tsr addTestRun:gtsr];
}

@end
