#!/usr/bin/env python
"""
_ServerTest_


"""
import os
import unittest
import time

from JobState.JobStateAPI import JobStateChangeAPI
from MessageService.MessageService import MessageService
from Trigger.TriggerAPI.TriggerAPI import TriggerAPI

class ComponentServerTest(unittest.TestCase):
    """
    TestCase implementation for ServerTest
    """

    _triggerSet = False
    def setUp(self):

        if not ComponentServerTest._triggerSet:
           print "\n****Start ComponentServerTest (JobCleanup)*******"
           # we use this for event publication.
           self.ms=MessageService()
           self.ms.registerAs("JobCleanupTest")
           self.jobSpecs=10
           self.location='/tmp/prodAgent/cacheDirs'
           self.failureJobSpecs=10
           self.flags=5
           self.trigger=TriggerAPI(self.ms)
           # create some directories in tmp
           print('\nCreating directories in the /tmp area to serve '+ \
                     'as job cache dirs')
           for i in xrange(0,self.jobSpecs):
               try:
                  os.makedirs(self.location+'/jobSpecDir_'+str(i))
                  # create some files (some of which should not be deleted,
                  # by a partial cleanup)
                  file1=open(self.location+'/jobSpecDir_'+str(i)+'/JobSpec.xml','w')
                  file1.close()
                  file2=open(self.location+'/jobSpecDir_'+str(i)+'/FrameworkJobReport.xml','w')
                  file2.close()
                  file3=open(self.location+'/jobSpecDir_'+str(i)+'/JobTarFile.tar.gz','w')
                  file3.close()
                  file4=open(self.location+'/jobSpecDir_'+str(i)+'/Pretend2BeADir1.txt','w')
                  file4.close()
                  file5=open(self.location+'/jobSpecDir_'+str(i)+'/Pretend2BeADir2.txt','w')
                  file5.close()
               except:
                  raise
           # create jobcaches that need to be tarred and then removed:
           for i in xrange(0,self.failureJobSpecs):
               try:
                  os.makedirs(self.location+'/failureJobSpecDir_'+str(i))
                  file1=open(self.location+'/failureJobSpecDir_'+str(i)+'/JobSpec.xml','w')
                  file1.close()
                  file2=open(self.location+'/failureJobSpecDir_'+str(i)+'/FrameworkJobReport.xml','w')
                  file2.close()
                  file3=open(self.location+'/failureJobSpecDir_'+str(i)+'/JobTarFile.tar.gz','w')
                  file3.close()
                  file4=open(self.location+'/failureJobSpecDir_'+str(i)+'/aFile.txt','w')
                  file4.close()
                  os.makedirs(self.location+'/failureJobSpecDir_'+str(i)+'/aDir1')
                  file5=open(self.location+'/failureJobSpecDir_'+str(i)+'/aDir1/File.txt','w')
                  file5.close()
                  os.makedirs(self.location+'/failureJobSpecDir_'+str(i)+'/aDir2')
                  file6=open(self.location+'/failureJobSpecDir_'+str(i)+'/aDir2/aFile.txt','w')
                  file6.close()
                  os.makedirs(self.location+'/failureJobSpecDir_'+str(i)+'/aDir3')
                  file7=open(self.location+'/failureJobSpecDir_'+str(i)+'/aDir3/aFile.txt','w')
                  file7.close()
               except:
                  raise
           ComponentServerTest._triggerSet=True

    def testA(self):
        print("""\npublish events to turn JobCleanup logging on""")
        try:
            self.ms.publish("JobCleanup:StartDebug", "none")
            self.ms.commit()
        except StandardError, ex:
            msg = "Failed testA:\n"
            msg += str(ex)
            self.fail(msg)

    def testB(self):
        print("""\nSet the job cache (used for job cleanup)""")
        try:
            for i in xrange(0,self.jobSpecs):
                JobStateChangeAPI.register("jobSpec"+str(i),"Processing",2,2)
                JobStateChangeAPI.create("jobSpec"+str(i),self.location+"/jobSpecDir_"+str(i))
        except StandardError, ex:
            msg = "Failed testB:\n"
            msg += str(ex)
            self.fail(msg)

    def testC(self):
        print("""\nEmit partial cleanup events to test the partialCleanupHandler""")
        for i in xrange(0,self.jobSpecs):
            payload="jobSpec"+str(i)+",SubmitJob,jobSpec"+str(i)
            self.ms.publish("PartialJobCleanup", payload)
            self.ms.commit()
        print("""\nSleep for several seconds""")
        time.sleep(3)


    def testD(self):
        print("""\nCreate and set triggers to activate job cleanup""")
        try:
            for i in xrange(0,self.jobSpecs):
                for k in xrange(0,self.flags):
                    self.trigger.addFlag("jobCleanupTrigger"+str(i),\
                        "jobSpec"+str(i),"flag"+str(k))
                self.trigger.setAction("jobSpec"+str(i),\
                    "jobCleanupTrigger"+str(i),"jobCleanAction")
        except StandardError, ex:
            msg = "Failed testC:\n"
            msg += str(ex)
            self.fail(msg)

    def testE(self):
        try:
            for i in xrange(0,self.jobSpecs):
                for k in xrange(0,self.flags):
                    self.trigger.setFlag("jobCleanupTrigger"+str(i),\
                        "jobSpec"+str(i),"flag"+str(k))
        except StandardError, ex:
            msg = "Failed testD:\n"
            msg += str(ex)
            self.fail(msg)

    def testF(self):
        print("""\nSet the job cache (used for failure job cleanup)""")
        try:
            for i in xrange(0,self.failureJobSpecs):
                JobStateChangeAPI.register("failureJobSpec"+str(i),"Processing",2,2)
                JobStateChangeAPI.create("failureJobSpec"+str(i),self.location+"/failureJobSpecDir_"+str(i))
        except StandardError, ex:
            msg = "Failed testB:\n"
            msg += str(ex)
            self.fail(msg)

    def testG(self):
        print("""\nEmit failure cleanup events to test the failureCleanupHandler""")
        for i in xrange(0,self.failureJobSpecs):
            payload="failureJobSpec"+str(i)
            self.ms.publish("FailureCleanup", payload)
            self.ms.commit()
        print("""\nSleep for several seconds""")
        time.sleep(3)

    def testH(self):
        print("""\nCleanup the prodagent database""")
        print("\nsleep for 20 seconds to") 
        print("let the cleanup component receive the messages")
        time.sleep(20)
        try:
            JobStateChangeAPI.purgeStates()
            self.ms.purgeMessages()
        except StandardError, ex:
            msg = "Failed testE:\n"
            msg += str(ex)
            self.fail(msg)
       
    def runTest(self):
         self.testA()
         self.testB()
         self.testC()
         self.testD()
         self.testE()
         self.testF()
         self.testG()
         self.testH()

    

if __name__ == '__main__':
    unittest.main()

    
