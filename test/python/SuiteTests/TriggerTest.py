#!/usr/bin/env python
"""
A test suite that runs a number of tests that depend 
on eachother.

"""

import unittest

from Test import preTests
from Test import postTests
from UnitTests.Trigger_t import TriggerUnitTests

testSuite = unittest.TestSuite()
preTests()
# this is the order in which they will be tested
testSuite.addTest(TriggerUnitTests())

testResult= unittest.TestResult()
testResult=testSuite.run(testResult)
postTests(testResult)

