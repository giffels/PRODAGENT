#!/usr/bin/env python
"""
_ServerTest_


"""
import os
import unittest
import time

from MessageService.MessageService import MessageService
from ProdAgent.Trigger.Trigger import Trigger as TriggerAPI
from ProdAgent.WorkflowEntities import JobState
from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdCommon.Database import Session


class ComponentServerTest(unittest.TestCase):
    """
    TestCase implementation for ServerTest
    """

    _triggerSet = False
    def setUp(self):
        Session.set_database(dbConfig)
        Session.connect()
        Session.start_transaction()

        if not ComponentServerTest._triggerSet:
           print "\n****Start ComponentServerTest (JobCleanup)*******"
           # we use this for event publication.
           self.ms=MessageService()
           self.ms.registerAs("JobCleanupTest")
           self.jobSpecs=1000
           self.location='/tmp/prodagent/components/JobCleanup/cacheDirs'
           self.failureJobSpecs=1000
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
           Session.commit_all()
           Session.close_all()


    def testA(self):
        print("""\npublish events to turn JobCleanup logging on""")
        try:
            Session.set_database(dbConfig)
            Session.connect()
            Session.start_transaction()
            self.ms.publish("JobCleanup:StartDebug", "none")
            self.ms.commit()
            Session.commit_all()
            Session.close_all()
        except StandardError, ex:
            msg = "Failed testA:\n"
            msg += str(ex)
            self.fail(msg)

    def testB(self):
        print("""\nSet the job cache (used for job cleanup)""")
        try:
            Session.set_database(dbConfig)
            Session.connect()
            Session.start_transaction()
            for i in xrange(0,self.jobSpecs):
                JobState.register("jobSpec"+str(i),"Processing",2,2)
                JobState.create("jobSpec"+str(i),self.location+"/jobSpecDir_"+str(i))
            Session.commit_all()
            Session.close_all()
        except StandardError, ex:
            msg = "Failed testB:\n"
            msg += str(ex)
            self.fail(msg)

    def testC(self):
        print("""\nEmit partial cleanup events to test the partialCleanupHandler""")
        Session.set_database(dbConfig)
        Session.connect()
        Session.start_transaction()
        for i in xrange(0,self.jobSpecs):
            payload="jobSpec"+str(i)+",SubmitJob,jobSpec"+str(i)
            self.ms.publish("PartialJobCleanup", payload)
            self.ms.commit()
        Session.commit_all()
        Session.close_all()
        print("""\nSleep for several seconds""")
        time.sleep(3)


    def testD(self):
        print("""\nCreate and set triggers to activate job cleanup""")
        try:
            Session.set_database(dbConfig)
            Session.connect()
            Session.start_transaction()
            for i in xrange(0,self.jobSpecs):
                for k in xrange(0,self.flags):
                    self.trigger.addFlag("jobCleanupTrigger"+str(i),\
                        "jobSpec"+str(i),"flag"+str(k))
                self.trigger.setAction("jobSpec"+str(i),\
                    "jobCleanupTrigger"+str(i),"jobCleanAction")
            Session.commit_all()
            Session.close_all()
        except StandardError, ex:
            msg = "Failed testC:\n"
            msg += str(ex)
            self.fail(msg)

    def testE(self):
        try:
            Session.set_database(dbConfig)
            Session.connect()
            Session.start_transaction()
            for i in xrange(0,self.jobSpecs):
                for k in xrange(0,self.flags):
                    self.trigger.setFlag("jobCleanupTrigger"+str(i),\
                        "jobSpec"+str(i),"flag"+str(k))
            Session.commit_all()
            Session.close_all()
        except StandardError, ex:
            msg = "Failed testD:\n"
            msg += str(ex)
            self.fail(msg)

    def testF(self):
        print("""\nSet the job cache (used for failure job cleanup)""")
        try:
            Session.set_database(dbConfig)
            Session.connect()
            Session.start_transaction()
            for i in xrange(0,self.failureJobSpecs):
                JobState.register("failureJobSpec"+str(i),"Processing",2,2)
                JobState.create("failureJobSpec"+str(i),self.location+"/failureJobSpecDir_"+str(i))
            Session.commit_all()
            Session.close_all()
        except StandardError, ex:
            msg = "Failed testB:\n"
            msg += str(ex)
            self.fail(msg)

    def testG(self):
        print("""\nEmit failure cleanup events to test the failureCleanupHandler""")
        Session.set_database(dbConfig)
        Session.connect()
        Session.start_transaction()
        for i in xrange(0,self.failureJobSpecs):
            payload="failureJobSpec"+str(i)
            #print('publishing FailureCleanup for failureJobSpec'+str(i))
            self.ms.publish("FailureCleanup", payload)
            self.ms.commit()
        Session.commit_all()
        Session.close_all()
        print("""\nSleep for several seconds""")
        time.sleep(3)

    def testH(self):
        return
        print("""\nCleanup the prodagent database""")
        print("\nsleep for 20 seconds to") 
        print("let the cleanup component receive the messages")
        time.sleep(20)
        try:
            Session.set_database(dbConfig)
            Session.connect()
            Session.start_transaction()
            JobState.purgeStates()
            self.ms.purgeMessages()
            Session.commit_all()
            Session.close_all()
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
#         self.testH()

    

if __name__ == '__main__':
    unittest.main()

    
