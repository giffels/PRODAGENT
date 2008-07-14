#!/usr/bin/env python
"""
Unittest JobState.JobStateAPI module
"""


import unittest

from JobState.JobStateAPI import JobStateChangeAPI
from JobState.JobStateAPI import JobStateInfoAPI


class JobStateUnitTests2(unittest.TestCase):
    """
    TestCase for JobStateAPI module 
    """

    def setUp(self):
        print "\n**************Start JobStateUnitTests2**********"
        print "\nPurging job states from database"


    def testA(self):
        """change state test"""
        try:
          for i in [1,2]:
              JobStateChangeAPI.cleanout("jobClassID"+str(i))
        except StandardError, ex:
            msg = "Failed State Change TestA:\n"
            msg += str(ex)
            self.fail(msg)

    def testB(self):
         try:
            JobStateChangeAPI.purgeStates()
         except StandardError, ex:
            msg = "Failed State Change TestB:\n"
            msg += str(ex)
            self.fail(msg)

    def runTest(self):
        self.testA()
        self.testB()
            
if __name__ == '__main__':
    unittest.main()
