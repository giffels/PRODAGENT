#!/usr/bin/env python
"""
Unittest JobState.JobStateAPI module
"""


import unittest

from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdAgent.WorkflowEntities import JobState
from ProdCommon.Core.ProdException import ProdException
from ProdCommon.Database import Session


class JobStateUnitTests2(unittest.TestCase):
    """
    TestCase for JobStateAPI module 
    """

    def setUp(self):
        print "\n**************Start JobStateUnitTests2**********"
        print "\nPurging job states from database"


    def testA(self):
        """change state test"""
        Session.set_database(dbConfig)
        Session.connect()
        Session.start_transaction()
        try:
          for i in [1,2]:
              JobState.cleanout("jobClassID"+str(i))
        except StandardError, ex:
            msg = "Failed State Change TestA:\n"
            msg += str(ex)
            self.fail(msg)
        Session.commit_all()
        Session.close_all()


    def testB(self):
         Session.set_database(dbConfig)
         Session.connect()
         Session.start_transaction()
         try:
            JobState.purgeStates()
         except StandardError, ex:
            msg = "Failed State Change TestB:\n"
            msg += str(ex)
            self.fail(msg)
         Session.commit_all()
         Session.close_all()

    def runTest(self):
        self.testA()
        self.testB()
            
if __name__ == '__main__':
    unittest.main()
