#!/usr/bin/env python
"""
A test suite that runs a number of tests that depend 
on eachother.

"""

import unittest
import time

from Test import preTests
from Test import postTests
from UnitTests.JobState_t import JobStateUnitTests
from UnitTests.JobState_t2 import JobStateUnitTests2
from UnitTests.FwkJobReport_t import FwkJobReportTests 
from IntTests.ErrorHandler_Message_t import ComponentServerTest

print "Make sure ONLY the ErrorHandler component is running"
testSuite = unittest.TestSuite()
preTests()
# this is the order in which they will be tested
testSuite.addTest(JobStateUnitTests2())
testSuite.addTest(FwkJobReportTests())
testSuite.addTest(JobStateUnitTests())
testSuite.addTest(ComponentServerTest())
testSuite.addTest(JobStateUnitTests2())
testResult= unittest.TestResult()
testResult=testSuite.run(testResult)
postTests(testResult)

