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

    def testA(self):
        """change state test"""
        try:
         for i in [1,2,3,4,5,6,7,8,9,10]:
              try:
                   JobStateChangeAPI.cleanout("jobClassID"+str(i))
              except Exception,ex:
                   self.assertEqual(ex[1],'This jobspec with ID jobClassID'+str(i)+' does not exist')
        except StandardError, ex:
            msg = "Failed State Change TestA:\n"
            msg += str(ex)
            self.fail(msg)

    def runTest(self):
        self.testA()
            
if __name__ == '__main__':
    unittest.main()
