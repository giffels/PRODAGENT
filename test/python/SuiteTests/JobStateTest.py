#!/usr/bin/env python
"""
A test suite that runs a number of tests that depend 
on eachother.

"""
import time
import unittest


from Test import preTests
from Test import postTests
from UnitTests.JobState_t import JobStateUnitTests
from UnitTests.JobState_t2 import JobStateUnitTests2

testSuite = unittest.TestSuite()
preTests()
# this is the order in which they will be tested
testSuite.addTest(JobStateUnitTests2())
testSuite.addTest(JobStateUnitTests())
testSuite.addTest(JobStateUnitTests2())

testResult= unittest.TestResult()
testResult=testSuite.run(testResult)
postTests(testResult)
