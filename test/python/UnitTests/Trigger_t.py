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
        self.ms=MessageService()
        self.ms.registerAs("TriggerTest")
        self.trigger=TriggerAPI(self.ms)
        self.triggers=2
        self.jobspecs=2
        self.flags=2

        if not TriggerUnitTests._triggerSet:
           print "**************NOTE TriggerUnitTests***********"
           print "Make sure the test input does not conflict"
           print "with the data in the database!"
           print " "
           print "Make sure the database (and client) are properly"
           print "configured."
           print " "
           TriggerUnitTests._triggerSet=True

    def testA(self):
        try:
           print("\nCreate job spec ids")
           for j in xrange(0,self.jobspecs):
               JobStateChangeAPI.register("jobSpec"+str(j),"processing",3,1)
        except StandardError, ex:
            msg = "Failed Test A:\n"
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
            msg = "Failed Test B:\n"
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
                         self.assertEqual(str(ex[1]),"Flag trigger"+str(i)+",jobSpec"+str(j)+",flag"+str(k)+" already exists")
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
                         self.assertEqual(str(ex[1]),"Flag not_exists_trigger"+str(i)+",not_exist_jobSpec"+str(j)+",not_exist_flag"+str(k)+" does not exists")
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
            for j in xrange(0,self.jobspecs):
                self.trigger.cleanout("jobSpec"+str(j))
        except StandardError, ex:
            msg = "Failed Test H:\n"
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
        self.testH()
           
if __name__ == '__main__':
    unittest.main()
