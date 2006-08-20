#!/usr/bin/env python
"""
A test suite that runs a number of tests that depend 
on eachother.

"""

import sys
import unittest

from Test import preTests
from Test import postTests
from IntTests.ErrorHandler_t import ComponentServerTest

preTests()

userInput=raw_input("are the error handler and cleanup (and only these)"+\
                    " components running? Y/n ")
if userInput!="Y":
   print "Make sure ONLY the ErrorHandler and JobCleanup components are running!"
   sys.exit()

testSuite = unittest.TestSuite()
testSuite.addTest(ComponentServerTest())
testResult= unittest.TestResult()
testResult=testSuite.run(testResult)
postTests(testResult)

