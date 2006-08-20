#!/usr/bin/env python
"""
_ServerTest_


"""
import os
import unittest
import time

from FwkJobRep.FwkJobReport import FwkJobReport
from JobState.JobStateAPI import JobStateChangeAPI
from JobState.JobStateAPI import JobStateInfoAPI
from MessageService.MessageService import MessageService
from Trigger.TriggerAPI.TriggerAPI import TriggerAPI


class ComponentServerTest(unittest.TestCase):
    """
    TestCase implementation for ServerTest
    """
    def setUp(self):
        # we use this for event publication.
        self.ms=MessageService()
        self.ms.registerAs("TestComponent")
        self.ms.subscribeTo("SubmitJob")
        self.failedJobs=10
        self.successJobs=10
        self.outputPath=os.getenv('PRODAGENT_WORKDIR')

    def testA(self):
        try:
            print("--->sending debug events")
            self.ms.publish("ErrorHandler:StartDebug", "none")
            self.ms.publish("JobCleanup:StartDebug", "none")
            self.ms.commit()
        except StandardError, ex:
            msg = "Failed testA\n"
            msg += str(ex)
            self.fail(msg)

    def testB(self):
        try:
            print("--->setting job states for failure and success")
            print("we allow for 10 retries")
            for i in xrange(0,self.successJobs):
                JobStateChangeAPI.register('JobSpecSuccess_'+str(i),"processing",10,1)
            for i in xrange(0,self.failedJobs):
                JobStateChangeAPI.register('JobSpecFailed_'+str(i),"processing",10,1)
            # check if the entry was registered:
            for i in xrange(0,self.successJobs):
                generalInfo=JobStateInfoAPI.general('JobSpecSuccess_'+str(i))
                self.assertEqual(int(generalInfo['MaxRetries']),10)
                self.assertEqual(int(generalInfo['MaxRacers']),1)
            for i in xrange(0,self.failedJobs):
                generalInfo=JobStateInfoAPI.general('JobSpecFailed_'+str(i))
                self.assertEqual(int(generalInfo['MaxRetries']),10)
                self.assertEqual(int(generalInfo['MaxRacers']),1)
        except StandardError, ex:
            msg = "Failed testB:\n"
            msg += str(ex)
            self.fail(msg)

    def testC(self):
        try:
            print("--->creating fake job caches for testing")
            # create dirs and files for failed jobs
            for i in xrange(0,self.failedJobs):
                os.makedirs(self.outputPath+'/JobCache/JobSpecFailed_'+str(i))
                # create a job report
                fwkJobReport=FwkJobReport("JobSpecFailed_"+str(i))
                fwkJobReport.status="Failed"
                fwkJobReport.jobSpecId="JobSpecFailed_"+str(i)
                fwkJobReport.write(self.outputPath+"/JobCache/JobSpecFailed_"+str(i)+"/JobSpecFailed_"+str(i)+'.xml')

                # create some more dirs and files
                fakeFile=open(self.outputPath+'/JobCache/JobSpecFailed_'+str(i)+\
                    '/file1.xml','w')
                fakeFile.write('test1')
                fakeFile.close()
                fakeFile=open(self.outputPath+'/JobCache/JobSpecFailed_'+str(i)+\
                    '/file2.xml','w')
                fakeFile.write('test2')
                fakeFile.close()
                os.makedirs(self.outputPath+'/JobCache/JobSpecFailed_'+str(i)+\
                    '/aDir1')
                fakeFile=open(self.outputPath+'/JobCache/JobSpecFailed_'+str(i)+\
                    '/aDir1/file3.xml','w')
                fakeFile.write('test3')
                fakeFile.close()
                fakeFile=open(self.outputPath+'/JobCache/JobSpecFailed_'+str(i)+\
                    '/aDir1/file4.xml','w')
                fakeFile.write('test4')
                fakeFile.close()
                os.makedirs(self.outputPath+'/JobCache/JobSpecFailed_'+str(i)+\
                    '/aDir2')
                fakeFile=open(self.outputPath+'/JobCache/JobSpecFailed_'+str(i)+\
                    '/aDir2/file5.xml','w')
                fakeFile.write('test5')
                fakeFile.close()
                fakeFile=open(self.outputPath+'/JobCache/JobSpecFailed_'+str(i)+\
                    '/aDir2/file6.xml','w')
                fakeFile.write('test6')
                fakeFile.close()
            # create dirs and files for success jobs
            for i in xrange(0,self.successJobs):
                os.makedirs(self.outputPath+'/JobCache/JobSpecSuccess_'+str(i))
                # create a job report
                fwkJobReport=FwkJobReport("JobSpecSuccess_"+str(i))
                fwkJobReport.status="Success"
                fwkJobReport.jobSpecId="JobSpecSuccess_"+str(i)
                fwkJobReport.write(self.outputPath+"/JobCache/JobSpecSuccess_"+str(i)+"/JobSpecSuccess_"+str(i)+'.xml')

                #create some more files and dirs.
                fakeFile=open(self.outputPath+'/JobCache/JobSpecSuccess_'+str(i)+\
                    '/file2.xml','w')
                fakeFile.write('test2')
                fakeFile.close()
                os.makedirs(self.outputPath+'/JobCache/JobSpecSuccess_'+str(i)+\
                    '/aDir1')
                fakeFile=open(self.outputPath+'/JobCache/JobSpecSuccess_'+str(i)+\
                    '/aDir1/file3.xml','w')
                fakeFile.write('test3')
                fakeFile.close()
                fakeFile=open(self.outputPath+'/JobCache/JobSpecSuccess_'+str(i)+\
                    '/aDir1/file4.xml','w')
                fakeFile.write('test4')
                fakeFile.close()
                os.makedirs(self.outputPath+'/JobCache/JobSpecSuccess_'+str(i)+\
                    '/aDir2')
                fakeFile=open(self.outputPath+'/JobCache/JobSpecSuccess_'+str(i)+\
                    '/aDir2/file5.xml','w')
                fakeFile.write('test5')
                fakeFile.close()
                fakeFile=open(self.outputPath+'/JobCache/JobSpecSuccess_'+str(i)+\
                    '/aDir2/file6.xml','w')
                fakeFile.write('test6')
                fakeFile.close()
          
                
        except StandardError, ex:
            msg = "Failed testC:\n"
            msg += str(ex)
            self.fail(msg)

    def testD(self):
        try:
            print("--->create jobs for failure and success")
            print("and set jobcache in jobstate db")
            for i in xrange(0,self.successJobs):
                JobStateChangeAPI.create('JobSpecSuccess_'+str(i),self.outputPath+'/JobCache/JobSpecSuccess_'+str(i))
            for i in xrange(0,self.failedJobs):
                JobStateChangeAPI.create('JobSpecFailed_'+str(i),self.outputPath+'/JobCache/JobSpecFailed_'+str(i))
            # check if the entry was registered:
            for i in xrange(0,self.successJobs):
                generalInfo=JobStateInfoAPI.general('JobSpecSuccess_'+str(i))
                self.assertEqual(int(generalInfo['MaxRetries']),10)
                self.assertEqual(int(generalInfo['MaxRacers']),1)
                self.assertEqual(generalInfo['CacheDirLocation'],self.outputPath+'/JobCache/JobSpecSuccess_'+str(i))
                self.assertEqual(generalInfo['State'],'create')
            for i in xrange(0,self.failedJobs):
                generalInfo=JobStateInfoAPI.general('JobSpecFailed_'+str(i))
                self.assertEqual(int(generalInfo['MaxRetries']),10)
                self.assertEqual(int(generalInfo['MaxRacers']),1)
                self.assertEqual(generalInfo['CacheDirLocation'],self.outputPath+'/JobCache/JobSpecFailed_'+str(i))
                self.assertEqual(generalInfo['State'],'create')
        except StandardError, ex:
            msg = "Failed testD:\n"
            msg += str(ex)
            self.fail(msg)

    def testE(self):
        try:
            print("--->change job states to progress")
            for i in xrange(0,self.successJobs):
                JobStateChangeAPI.inProgress('JobSpecSuccess_'+str(i))
            for i in xrange(0,self.failedJobs):
                JobStateChangeAPI.inProgress('JobSpecFailed_'+str(i))
            # check if the entry was registered:
            for i in xrange(0,self.successJobs):
                generalInfo=JobStateInfoAPI.general('JobSpecSuccess_'+str(i))
                self.assertEqual(int(generalInfo['MaxRetries']),10)
                self.assertEqual(int(generalInfo['MaxRacers']),1)
                self.assertEqual(int(generalInfo['Racers']),0)
                self.assertEqual(generalInfo['CacheDirLocation'],self.outputPath+'/JobCache/JobSpecSuccess_'+str(i))
                self.assertEqual(generalInfo['State'],'inProgress')
            for i in xrange(0,self.failedJobs):
                generalInfo=JobStateInfoAPI.general('JobSpecFailed_'+str(i))
                self.assertEqual(int(generalInfo['MaxRetries']),10)
                self.assertEqual(int(generalInfo['MaxRacers']),1)
                self.assertEqual(int(generalInfo['Racers']),0)
                self.assertEqual(generalInfo['CacheDirLocation'],self.outputPath+'/JobCache/JobSpecFailed_'+str(i))
                self.assertEqual(generalInfo['State'],'inProgress')
        except StandardError, ex:
            msg = "Failed testE:\n"
            msg += str(ex)
            self.fail(msg)

    def testF(self):
        try:
            print("--->pretend to submit and emit job failed ")
            # we only emit failed events as error handlers and job cleanup
            # do not subscribe to success events
            
            # do this 10 times (to simulate 10 retries)
            # pretend to submit the jobs.
            for tries in xrange(0,10):
                for i in xrange(0,self.failedJobs):
                    JobStateChangeAPI.submit('JobSpecFailed_'+str(i))

                    # NOTE: IF YOU COMMENT OUT THE LINE BELOW YOU GET A RACER EXCEPTION
                    # NOTE: AS YOU SUBMIT A JOB TWICE (AND ONLY 1 IS ALLOWED)
                    # NOTE: (RE)SUBMISSION CAN TAKE PLACE AFTER THE JOB HAS BEEN HANDLED BY THE 
                    # NOTE: ERROR HANDLER (THE JOBSUBMITTER CHECKS FOR THIS WITHOUT GENERATING
                    # NOTE: THIS ERROR, BUT THE RESULT IS THE SAME IT GENERATES SOMETHING LIKE:
                    # NOTE: Too many submitted jobs for JobSpecID: ....
                    # NOTE: Current Jobs: 1
                    # NOTE: Maximum Jobs: 1
                    #JobStateChangeAPI.submit('JobSpecFailed_'+str(i))
                    
                    # after a job has been submissions racers=MaxRacers=1
                    generalInfo=JobStateInfoAPI.general('JobSpecFailed_'+str(i))
                    self.assertEqual(int(generalInfo['MaxRacers']),1)
                    self.assertEqual(int(generalInfo['Racers']),1)
            
                    # submission successful but job fails and job failed event emitted
                    self.ms.publish("JobFailed",self.outputPath+"/JobCache/JobSpecFailed_"+str(i)+"/JobSpecFailed_"+str(i)+'.xml')
                    self.ms.commit()

                    # NOTE: IF YOU COMMENT OUT THE 2 LINES BELOW THE ERROR HANDLER WILL GENERATE
                    # NOTE: AN EXCEPTION ON THE NUMBER OF RACERS AS ONLY 1 RUNNING JOB IS ALLOWED
                    # NOTE: SENDING 2 JOBFAILED WOULD INDICATE 2 RUNNING JOBS
                    # NOTE: NOTE THAT EVENT WITH THIS THE TESTS FINISHE SUCCESSFULLY AND THE EXTRA JOBFAILED
                    # NOTE: IS DISCARDED.
                    #self.ms.publish("JobFailed",self.outputPath+"/JobCache/JobSpecFailed_"+str(i)+"/JobSpecFailed_"+str(i)+'.xml')
                    #self.ms.commit()

                # the error handler submits an Submit Job event that we will receive.
                # note we should not wait on the last one as after 10 retries the job gets cleaned out
                if tries<9:
                    for i in xrange(0,self.failedJobs):
                        type, payload = self.ms.get()
                        self.ms.commit()
                        print('Getting message of type '+str(type)+' and payload: '+str(payload)+\
                            ' '+str(i)+'/'+str(self.failedJobs)+' re-try: '+str(tries))
                        # check if we get the right event back
                        self.assertEqual("SubmitJob",type)
                        # after the error is handled the number of racers is 0 again
                        generalInfo=JobStateInfoAPI.general(str(payload))
                        self.assertEqual(int(generalInfo['MaxRacers']),1)
                        self.assertEqual(int(generalInfo['Racers']),0)
           
        except StandardError, ex:
            msg = "Failed testF:\n"
            msg += str(ex)
            self.fail(msg)

    def testG(self):
        try:
            print("--->pretend that submission fails ")
            # we only emit failed events as error handlers and job cleanup
            # do not subscribe to success events
            
            # do this 10 times (to simulate 10 retries)
            # pretend to submit the jobs.
            for tries in xrange(0,10):
                for i in xrange(0,self.failedJobs):
                    #NOTE: IF YOU COMMENT OUT THIS LINE AND THUS SUBMITS
                    #NOTE: A JOB THE RACER IS UPDATED HOWEVER THE LAST
                    #NOTE: ASSERT EQUAL FAILS AS AFTER THE submit CALL
                    #NOTE: A SubmissionFailed EVENT WAS GENERATED.
                    #NOTE: WHICH GENERATES A SUBMIT JOB EVENT WHICH
                    #NOTE: WILL GENERATE THIS ERROR IN THE JobSubmitter:
                    #NOTE: Too many submitted jobs for JobSpecID: ....
                    #NOTE: Current Jobs: 1
                    #NOTE: Maximum Jobs: 1

                    #JobStateChangeAPI.submit('JobSpecFailed_'+str(i))

                    #NOTE: due to this the job submitter emits a SubmissionFailed
                    #NOTE: event which is caught by the error handler which emits
                    #NOTE: an SubmitJob event until the maximum is reached after
                    #NOTE: which the cleanout is done.

                    self.ms.publish("SubmissionFailed","JobSpecFailed_"+str(i))
                    self.ms.commit()
                    generalInfo=JobStateInfoAPI.general('JobSpecFailed_'+str(i))
                    self.assertEqual(int(generalInfo['MaxRacers']),1)
                    self.assertEqual(int(generalInfo['Racers']),0)

                # the error handler submits an Submit Job event that we will receive.
                # note we should not wait on the last one as after 10 retries the job gets cleaned out
                if tries<9:
                    for i in xrange(0,self.failedJobs):
                        type, payload = self.ms.get()
                        self.ms.commit()
                        print('Getting message of type '+str(type)+' and payload: '+str(payload)+\
                            ' '+str(i)+'/'+str(self.failedJobs)+' re-try: '+str(tries))
                        # check if we get the right event back
                        self.assertEqual("SubmitJob",type)
                        # after the error is handled the number of racers is 0 again
                        generalInfo=JobStateInfoAPI.general(str(payload))
                        self.assertEqual(int(generalInfo['MaxRacers']),1)
                        self.assertEqual(int(generalInfo['Racers']),0)
           
        except StandardError, ex:
            msg = "Failed testG:\n"
            msg += str(ex)
            self.fail(msg)

    def testH(self):
        try:
            print("--->sleep for 20 seconds to make sure messages")
            print("are delivered before purging them")
            self.ms.purgeMessages()
        except StandardError, ex:
            msg = "Failed testH:\n"
            msg += str(ex)
            self.fail(msg)
       
        self.ms.purgeMessages()

    def runTest(self):
         self.testA()
         self.testB()
         self.testC()
         self.testD()
         self.testE()
         # either comment out testF or testG
         # but not both!!
         self.testF()
         #self.testG()

if __name__ == '__main__':
    unittest.main()

    
