#!/usr/bin/env python
"""
A test suite that runs a number of tests that depend 
on eachother.

"""
import time
import unittest

from Test import preTests
from Test import postTests
from UnitTests.Trigger_t import TriggerUnitTests
from UnitTests.Trigger_t2 import TriggerUnitTests2

testSuite = unittest.TestSuite()
preTests()

# this is the order in which they will be tested
testSuite.addTest(TriggerUnitTests2())
testSuite.addTest(TriggerUnitTests())
testSuite.addTest(TriggerUnitTests2())

testResult= unittest.TestResult()
testResult=testSuite.run(testResult)
postTests(testResult)

