#!/usr/bin/env python
"""
A test suite that runs a number of tests that depend 
on eachother.

"""

import sys
import time
import unittest

from Test import preTests
from Test import postTests
from IntTests.JobCleanup_Message_t import ComponentServerTest


from UnitTests.JobState_t2 import JobStateUnitTests2
from UnitTests.Trigger_t2 import TriggerUnitTests2


preTests()

userInput=raw_input("Is the cleanup component (and only the cleanup "+\
                    " component running? Y/n ")
if userInput!="Y":
   print "Make sure ONLY the JobCleanup component is running!"
   sys.exit()

testSuite = unittest.TestSuite()
# this is the order in which they will be tested
#testSuite.addTest(JobStateUnitTests2())
#testSuite.addTest(TriggerUnitTests2())
testSuite.addTest(ComponentServerTest())
testResult= unittest.TestResult()
testResult=testSuite.run(testResult)
postTests(testResult)

