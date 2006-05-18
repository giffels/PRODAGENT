#!/usr/bin/env python
"""
Unittest Trigger.TriggerAPI module
"""


import unittest

from JobState.JobStateAPI import JobStateChangeAPI
from MessageService.MessageService import MessageService
from Trigger.TriggerAPI.TriggerAPI import TriggerAPI


class TriggerUnitTests(unittest.TestCase):
    """
    TestCase for TriggerAPI module 
    """

    _triggerSet = False

    def setUp(self):

        if not TriggerUnitTests._triggerSet:
           print "\n**************Start TriggerUnitTests**********"
           self.ms=MessageService()
           self.ms.registerAs("TriggerTest")
           self.trigger=TriggerAPI(self.ms)
           self.triggers=5
           self.jobspecs=5
           self.flags=5
           TriggerUnitTests._triggerSet=True

    def testA(self):
        try:
           print("\nCreate job spec ids")
           for j in xrange(0,self.jobspecs):
               JobStateChangeAPI.register("jobSpec"+str(j),"processing",3,1)
        except StandardError, ex:
            msg = "Failed TestA:\n"
            msg += str(ex)
            self.fail(msg)

    def testB(self):
        try:
           print("\nCreate Triggers")
           for i in xrange(0,self.triggers):
               for j in xrange(0,self.jobspecs):
                   for k in xrange(0,self.flags):
                      self.trigger.addFlag("trigger"+str(i),\
                          "jobSpec"+str(j),"flag"+str(k))
                   self.trigger.setAction("jobSpec"+str(j),\
                       "trigger"+str(i),"testAction")

        except StandardError, ex:
            msg = "Failed TestB:\n"
            msg += str(ex)
            self.fail(msg)

    def testFlags1(self,value):
        try:
            print("\nTest if flags are set")
            for i in xrange(0,self.triggers):
                for j in xrange(0,self.jobspecs):
                    for k in xrange(0,self.flags):
                       self.assertEqual(self.trigger.flagSet("trigger"+str(i),\
                           "jobSpec"+str(j),"flag"+str(k)),value)
        except StandardError, ex:
            msg = "Failed Test Flags1:\n"
            msg += str(ex)
            self.fail(msg)

    def testFlags2(self,value):
        try:
            print("\nTest if flags are set")
            for i in xrange(0,self.triggers):
                for j in xrange(0,self.jobspecs):
                   self.assertEqual(self.trigger.allFlagSet("trigger"+str(i),\
                       "jobSpec"+str(j)),value)
        except StandardError, ex:
            msg = "Failed Test Flags2:\n"
            msg += str(ex)
            self.fail(msg)

    def testC(self):
        try:
           print("\nCreate Duplicate Triggers (to test exceptions)")
           for i in xrange(0,self.triggers):
               for j in xrange(0,self.jobspecs):
                   for k in xrange(0,self.flags):
                      try:
                         self.trigger.addFlag("trigger"+str(i),"jobSpec"+str(j),"flag"+str(k))
                      except Exception, ex:
                         print(">>>Test suceeded for duplicate trigger exception\n")
        except Exception , ex:
            msg = "Failed Test C:\n"
            msg += str(ex)
            self.fail(msg)

    def testD(self):
        try:
           print("\nSet Some (not all) Flags")
           for i in xrange(0,self.triggers):
               for j in xrange(0,self.jobspecs):
                   for k in xrange(0,(self.flags-1)):
                      self.trigger.setFlag("trigger"+str(i),\
                          "jobSpec"+str(j),"flag"+str(k))

        except StandardError, ex:
            msg = "Failed Test D:\n"
            msg += str(ex)
            self.fail(msg)

    def testE(self):
        try:
           print("\nReset Triggers")
           for i in xrange(0,self.triggers):
               for j in xrange(0,self.jobspecs):
                   for k in xrange(0,(self.flags-1)):
                      self.trigger.resetFlag("trigger"+str(i),\
                          "jobSpec"+str(j),"flag"+str(k))

        except StandardError, ex:
            msg = "Failed Test E:\n"
            msg += str(ex)
            self.fail(msg)

    def testF(self):
        try:
           print("\nSet Flags that do not exist (to test exceptions)")
           for i in xrange(0,self.triggers):
               for j in xrange(0,self.jobspecs):
                   for k in xrange(0,(self.flags)):
                      try:
                          self.trigger.setFlag("not_exists_trigger"+str(i),\
                              "not_exist_jobSpec"+str(j),"not_exist_flag"+str(k))
                      except Exception,ex:
                         print(">>>Test succeeded for flag not exist exception\n")
        except StandardError, ex:
            msg = "Failed Test F:\n"
            msg += str(ex)
            self.fail(msg)

    def testG(self):
        try:
           print("\nSet All Flags")
           for i in xrange(0,self.triggers):
               for j in xrange(0,self.jobspecs):
                   for k in xrange(0,self.flags):
                      self.trigger.setFlag("trigger"+str(i),\
                          "jobSpec"+str(j),"flag"+str(k))

        except StandardError, ex:
            msg = "Failed Test F:\n"
            msg += str(ex)
            self.fail(msg)

    def testH(self):
        try:
           print("\nSet All Flags")
           for i in xrange(0,self.triggers):
               for j in xrange(0,self.jobspecs):
                   for k in xrange(0,self.flags):
                      self.trigger.setFlag("trigger"+str(i),\
                          "jobSpec"+str(j),"flag"+str(k))

        except StandardError, ex:
            msg = "Failed Test F:\n"
            msg += str(ex)
            self.fail(msg)

    def runTest(self):
        self.testA()
        self.testB()
        self.testFlags1(False)
        self.testFlags2(False)
        self.testC()
        self.testD()
        self.testE()
        self.testF()
        self.testG()
        self.testFlags1(True)
        self.testFlags2(True)
           
if __name__ == '__main__':
    unittest.main()
