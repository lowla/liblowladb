//
//  AppleSpecificTests.m
//  liblowladb-tests
//
//  Created by mark on 9/15/14.
//  Copyright (c) 2014 Teamstudio, Inc. All rights reserved.
//

#import <XCTest/XCTest.h>
#import <lowladb.h>

@interface AppleSpecificTests : XCTestCase

@end

@implementation AppleSpecificTests

-(void)testVersionMatchesCocoaPods
{
    NSString *cpVersion = [NSString stringWithFormat:@"%d.%d.%d", COCOAPODS_VERSION_MAJOR_liblowladb,
                           COCOAPODS_VERSION_MINOR_liblowladb, COCOAPODS_VERSION_PATCH_liblowladb];
    XCTAssertEqualObjects(cpVersion, @(lowladb_get_version().c_str()));
}

@end
