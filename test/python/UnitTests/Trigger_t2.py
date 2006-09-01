#!/usr/bin/env python
"""
Unittest Trigger.TriggerAPI module
"""


import unittest

from JobState.JobStateAPI import JobStateChangeAPI
from MessageService.MessageService import MessageService
from Trigger.TriggerAPI.TriggerAPI import TriggerAPI


class TriggerUnitTests2(unittest.TestCase):
    """
    TestCase for TriggerAPI module 
    """

    def setUp(self):
        print "\n**************Start TriggerUnitTests2**********"
        print "\nPurging triggers from database"
 
        self.ms=MessageService()
        self.ms.registerAs("TriggerUnitTest2")
        self.trigger=TriggerAPI(self.ms)
        self.triggers=2
        self.jobspecs=2
        self.flags=2

    def testA(self):
        try:
            for j in xrange(0,self.jobspecs):
                self.trigger.cleanout("jobSpec"+str(j))
                JobStateChangeAPI.cleanout("jobSpec"+str(j))
        except StandardError, ex:
            raise
            msg = "Failed TestA:\n"
            msg += str(ex)
            self.fail(msg)

    def testB(self):
        try:
            JobStateChangeAPI.purgeStates()
        except StandardError, ex:
            msg = "Failed TestB:\n"
            msg += str(ex)
            self.fail(msg)
        
    def runTest(self):
        self.testA()
        self.testB()
           
if __name__ == '__main__':
    unittest.main()
