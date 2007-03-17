#!/usr/bin/env python
"""
Unittest JobState.JobStateAPI module
"""


import unittest

from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdAgent.WorkflowEntities import JobState 
from ProdAgent.WorkflowEntities import Job
from ProdCommon.Core.ProdException import ProdException
from ProdCommon.Database import Session


class JobStateUnitTests(unittest.TestCase):
    """
    TestCase for JobStateAPI module 
    """

    def setUp(self):
        print "\n**************Start JobStateUnitTests**********"

    def testA(self):
        """change state test"""
        Session.set_database(dbConfig)
        Session.connect()
        Session.start_transaction()
        try:
         # illegal state transitions:
         try:
            JobState.create("jobClassID1","cacheDir/location/1somewhere")
         except ProdException, ex:
            print('>>>Test succeeded for exception 1/3 in testA of JobState_t.py\n')
         self.assertEqual(JobState.isRegistered("jobClassID1"),False)
         JobState.register("jobClassID1","Processing",3,1,"myWorkflowID")
         self.assertEqual(JobState.isRegistered("jobClassID1"),True)

         # register again (illegal):
         try:
             JobState.register("jobClassID1","Processing",3,1,"myWorkflowID")
             print('>>>Test ERROR \n')
         except ProdException, ex:
             print('>>>Test succeeded for exception 2/3 in testA of JobState_t.py\n')
         try:
         # illegal state transitions:
            JobState.inProgress("jobClassID1")
         except ProdException, ex:
            print('>>>Test succeeded for exception 3/3 in testA of JobState_t.py\n')
         JobState.create("jobClassID1","cacheDir/location/1somewhere")
         JobState.inProgress("jobClassID1")
         # retries=racers=0;
         self.assertEqual(JobState.general("jobClassID1"), {'Retries': 0, 'CacheDirLocation': 'cacheDir/location/1somewhere', 'MaxRacers': 1, 'Racers': 0, 'State': 'inProgress', 'MaxRetries': 3, 'JobType': 'Processing'}
)


         JobState.submit("jobClassID1")

         # retries=0, racers=1;
         self.assertEqual(JobState.general("jobClassID1"), {'Retries': 0L, 'CacheDirLocation': 'cacheDir/location/1somewhere', 'MaxRacers': 1L, 'Racers': 1L, 'State': 'inProgress', 'MaxRetries': 3L, 'JobType': 'Processing'})

         JobState.runFailure("jobClassID1","jobInstanceID1.1",
              "some.location1.1","job/Report/Location1.1.xml")
         JobState.submit("jobClassID1")
        except StandardError, ex:
            msg = "Failed State Change TestA:\n"
            msg += str(ex)
            self.fail(msg)
        Session.commit_all()
        Session.close_all()



    def testB(self):
        """change state test"""
        try:
         JobState.register("jobClassID2","Processing",2,1,"myWorkflowID")
         JobState.create("jobClassID2","cacheDir/location/2somewhere")
         JobState.inProgress("jobClassID2")

         # retries=racers=0
         self.assertEqual(JobState.general("jobClassID2"), {'Retries': 0, 'CacheDirLocation': 'cacheDir/location/2somewhere', 'MaxRacers': 1, 'Racers': 0, 'State': 'inProgress', 'MaxRetries': 2, 'JobType': 'Processing'})

         JobState.submit("jobClassID2")

         # retries0,=racers=1
         self.assertEqual(JobState.general("jobClassID2"),{'Retries': 0, 'CacheDirLocation': 'cacheDir/location/2somewhere', 'MaxRacers': 1, 'Racers': 1, 'State': 'inProgress', 'MaxRetries': 2, 'JobType': 'Processing'})

         JobState.runFailure("jobClassID2","jobInstanceID2.1",
              "some.location2.1","job/Report/Location2.1.xml")

         # retries= 1, racers=0
         self.assertEqual(JobState.general("jobClassID2"),
              {'CacheDirLocation': 'cacheDir/location/2somewhere', 
               'MaxRacers': 1, 'Racers': 0, 'State': 'inProgress', 
               'MaxRetries': 2, 'Retries': 1, 'JobType': 'Processing'})

         JobState.submit("jobClassID2")

         # retries= 1, racers=1
         self.assertEqual(JobState.general("jobClassID2"),{'Retries': 1L, 'CacheDirLocation': 'cacheDir/location/2somewhere', 'MaxRacers': 1L, 'Racers': 1L, 'State': 'inProgress', 'MaxRetries': 2L, 'JobType': 'Processing'})

        except StandardError, ex:
            msg = "Failed State Change TestB:\n"
            msg += str(ex)
            self.fail(msg)

    def testC(self):
        """change state test"""
        try:
         JobState.register("jobClassID3","Merge",5,1,"myWorkflowID")
         JobState.create("jobClassID3","cacheDir/location/3somewhere")
         JobState.inProgress("jobClassID3")
         JobState.submit("jobClassID3")

         # try an illegal state transition:
         try:
              JobState.create("jobClassID3","cacheDir/location3somewhere")
         except ProdException, ex:
              print('>>>Test succeeded for  exception 1/3 in testC of JobState_t.py\n')
        # try to submit another job while the first one has not finished (we only are allowed one racer)
         try:
              JobState.submit("jobClassID3")
         except ProdException, ex:
              print('>>>Test succeeded for  exception 2/3 in testC of JobState_t.py\n')

        # set the maximum number of racers higher and submit again.
         JobState.setRacer("jobClassID3",50)
         JobState.submit("jobClassID3")
         JobState.submit("jobClassID3")
         JobState.submit("jobClassID3")
         JobState.submit("jobClassID3")

        # althought the number of racers has been set higher, we are now
        # bound by the maximum number of retries.
         try:
              JobState.submit("jobClassID3")
         except ProdException, ex:
              print('>>>Test succeeded for exception 3/3 in testC of JobState_t.py\n')

        except StandardError, ex:
            msg = "Failed State Change TestC:\n"
            msg += str(ex)
            self.fail(msg)

    def testD(self):
        """change state test"""
        try:
         JobState.register("jobClassID4","Processing",6,2,"myWorkflowID")
         JobState.create("jobClassID4","cacheDir/location/4somewhere")
         JobState.inProgress("jobClassID4")

         # retries=racers=0
         self.assertEqual(JobState.general("jobClassID4"),{'Retries': 0L, 'CacheDirLocation': 'cacheDir/location/4somewhere', 'MaxRacers': 2L, 'Racers': 0L, 'State': 'inProgress', 'MaxRetries': 6L, 'JobType': 'Processing'})

         JobState.submit("jobClassID4")

         # retries=0, racers=1
         self.assertEqual(JobState.general("jobClassID4"),{'Retries': 0L, 'CacheDirLocation': 'cacheDir/location/4somewhere', 'MaxRacers': 2L, 'Racers': 1L, 'State': 'inProgress', 'MaxRetries': 6L, 'JobType': 'Processing'})


         JobState.runFailure("jobClassID4","jobInstanceID4.0",
              "some.location4.0","job/Report/Location4.0.xml")

         # retries=1, racers=0
         self.assertEqual(JobState.general("jobClassID4"),{'Retries': 1L, 'CacheDirLocation': 'cacheDir/location/4somewhere', 'MaxRacers': 2L, 'Racers': 0L, 'State': 'inProgress', 'MaxRetries': 6L, 'JobType': 'Processing'})

         JobState.submit("jobClassID4")

         # retries=1, racers=1
         self.assertEqual(JobState.general("jobClassID4"),{'Retries': 1L, 'CacheDirLocation': 'cacheDir/location/4somewhere', 'MaxRacers': 2L, 'Racers': 1L, 'State': 'inProgress', 'MaxRetries': 6L, 'JobType': 'Processing'})


         JobState.runFailure("jobClassID4","jobInstanceID4.1",
              "some.location4.1","job/Report/Location4.1.xml")
         # retries=2, racers=0
         JobState.submit("jobClassID4")
         # retries=2, racers=1
         JobState.submit("jobClassID4")
         # retries=2, racers=2
         JobState.runFailure("jobClassID4","jobInstanceID4.2",
              "some.location4.2","job/Report/Location4.2.xml")
         # retries=3, racers=1
         JobState.submit("jobClassID4")
         # retries=3, racers=2
         self.assertEqual(JobState.general("jobClassID4"),{'Retries': 3L, 'CacheDirLocation': 'cacheDir/location/4somewhere', 'MaxRacers': 2L, 'Racers': 2L, 'State': 'inProgress', 'MaxRetries': 6L, 'JobType': 'Processing'})
         JobState.finished("jobClassID4")
         self.assertEqual(JobState.general("jobClassID4"),{'Retries': 3L, 'CacheDirLocation': 'cacheDir/location/4somewhere', 'MaxRacers': 2L, 'Racers': 2L, 'State': 'finished', 'MaxRetries': 6L, 'JobType': 'Processing'})
        except StandardError, ex:
            msg = "Failed State Change TestD:\n"
            msg += str(ex)
            self.fail(msg)

    def testE(self):
        try:
         JobState.register("jobClassID5","Processing",2,2,"myWorkflowID")
         JobState.create("jobClassID5","cacheDir/location/5somewhere")
         JobState.inProgress("jobClassID5")
         JobState.submit("jobClassID5")

        # now introduce some failures until we have more failures
        # then retries (this raises an error)

         JobState.runFailure("jobClassID5","jobInstanceID5.1",
              "some.location5.1","job/Report/Location5.1.xml")
         try:
              JobState.runFailure("jobClassID5","jobInstanceID5.2",
                   "some.location5.1","job/Report/Location5.1.xml")
         except ProdException, ex:
              print('>>>Test succeeded for exception 1/1 in testE of JobState_t.py\n')
         JobState.finished("jobClassID5")

        except StandardError, ex:
            msg = "Failed State Change TestE:\n"
            msg += str(ex)
            self.fail(msg)

    def testF(self):
        try:
         self.assertEqual(JobState.lastLocations("jobClassID4"),\
           ["some.location4.0","some.location4.1","some.location4.2"])    
         self.assertEqual(JobState.lastLocations("jobClassID2"),\
           ["some.location2.1"])
        except StandardError, ex:
            msg = "Failed State Change TestF:\n"
            msg += str(ex)
            self.fail(msg)

    def testG(self):
        try:
         reportList=JobState.jobReports("jobClassID4")
         self.assertEqual(JobState.jobReports("jobClassID4"), \
              ['job/Report/Location4.0.xml','job/Report/Location4.1.xml', 'job/Report/Location4.2.xml'])
        except StandardError, ex:
            msg = "Failed State Change TestG:\n"
            msg += str(ex)
            self.fail(msg)

    def testH(self):
         JobState.register("jobClassID7","Processing",8,2,"myWorkflowID")
         JobState.register("jobClassID8","Processing",8,2,"myWorkflowID")
         JobState.register("jobClassID9","Processing",8,2,"myWorkflowID")

    def testI(self):
         JobState.register("jobClassID10","Processing",8,2,"myWorkflowID")
         #retries=racer=0
         self.assertEqual(JobState.general("jobClassID10"),{'Retries': 0, 'CacheDirLocation': None, 'MaxRacers': 2, 'Racers': 0, 'State': 'register', 'MaxRetries': 8, 'JobType': 'Processing'})
         JobState.createFailure("jobClassID10")
         #retries=1, racer=0
         self.assertEqual(JobState.general("jobClassID10"),{'Retries': 1, 'CacheDirLocation': None, 'MaxRacers': 2, 'Racers': 0, 'State': 'register', 'MaxRetries': 8, 'JobType': 'Processing'})
         JobState.createFailure("jobClassID10")
         #retries=2, racer=0
         self.assertEqual(JobState.general("jobClassID10"),{'Retries': 2, 'CacheDirLocation': None, 'MaxRacers': 2, 'Racers': 0, 'State': 'register', 'MaxRetries': 8, 'JobType': 'Processing'})
         JobState.create("jobClassID10","cacheDir/location/10somewhere")
         #retries=2, racer=0
         self.assertEqual(JobState.general("jobClassID10"),{'Retries': 2, 'CacheDirLocation': 'cacheDir/location/10somewhere', 'MaxRacers': 2, 'Racers': 0, 'State': 'create', 'MaxRetries': 8, 'JobType': 'Processing'})
         JobState.inProgress("jobClassID10")
         #retries=2, racer=0
         self.assertEqual(JobState.general("jobClassID10"),{'Retries': 2, 'CacheDirLocation': 'cacheDir/location/10somewhere', 'MaxRacers': 2, 'Racers': 0, 'State': 'inProgress', 'MaxRetries': 8, 'JobType': 'Processing'})
         JobState.submitFailure("jobClassID10")
         #retries=3, racer=0
         self.assertEqual(JobState.general("jobClassID10"),{'Retries': 3, 'CacheDirLocation': 'cacheDir/location/10somewhere', 'MaxRacers': 2, 'Racers': 0, 'State': 'inProgress', 'MaxRetries': 8, 'JobType': 'Processing'})
         JobState.submit("jobClassID10")
         #retries=3, racer=1
         self.assertEqual(JobState.general("jobClassID10"),{'Retries': 3, 'CacheDirLocation': 'cacheDir/location/10somewhere', 'MaxRacers': 2, 'Racers': 1, 'State': 'inProgress', 'MaxRetries': 8, 'JobType': 'Processing'})
         JobState.submitFailure("jobClassID10")
         #retries=4, racer=1
         self.assertEqual(JobState.general("jobClassID10"),{'Retries': 4, 'CacheDirLocation': 'cacheDir/location/10somewhere', 'MaxRacers': 2, 'Racers': 1, 'State': 'inProgress', 'MaxRetries': 8, 'JobType': 'Processing'})
         JobState.submit("jobClassID10")
         #retries=4, racer=2
         self.assertEqual(JobState.general("jobClassID10"),{'Retries': 4, 'CacheDirLocation': 'cacheDir/location/10somewhere', 'MaxRacers': 2, 'Racers': 2, 'State': 'inProgress', 'MaxRetries': 8, 'JobType': 'Processing'})

         # on purpose we introduce an error:
         try:
             JobState.submit("jobClassID10")
         except ProdException, ex:
             print('>>>Test succeeded for exception 1/1 in testH of JobState_t.py\n')
         JobState.runFailure("jobClassID10","jobInstanceID10.1",
              "some.location10.1","job/Report/Location10.1.xml")
         #retries=5, racer=1
         self.assertEqual(JobState.general("jobClassID10"),{'Retries': 5, 'CacheDirLocation': 'cacheDir/location/10somewhere', 'MaxRacers': 2, 'Racers': 1, 'State': 'inProgress', 'MaxRetries': 8, 'JobType': 'Processing'})
         JobState.runFailure("jobClassID10","jobInstanceID10.2",
              "some.location10.2","job/Report/Location10.2.xml")
         #retries=6, racer=0
         self.assertEqual(JobState.general("jobClassID10"),{'Retries': 6, 'CacheDirLocation': 'cacheDir/location/10somewhere', 'MaxRacers': 2, 'Racers': 0, 'State': 'inProgress', 'MaxRetries': 8, 'JobType': 'Processing'})

    def testJ(self):
        pass
        #self.assertEqual(JobState.jobSpecTotal(),9)

    def testK(self):
        jobIDs=[]
        for i in xrange(0,20):
            JobState.register("jobClassID_0."+str(i),"Processing",30,1)
            JobState.register("jobClassID_1."+str(i),"Processing",30,1,"myWorkflowID1")
            JobState.register("jobClassID_2."+str(i),"Processing",30,1,"myWorkflowID2")
            JobState.register("jobClassID_3."+str(i),"Processing",30,1,"myWorkflowID3")
            jobIDs.append("jobClassID_1."+str(i))
            jobIDs.append("jobClassID_2."+str(i))
            jobIDs.append("jobClassID_3."+str(i))
        JobState.setMaxRetries(jobIDs,2)
        self.assertEqual(JobState.general("jobClassID_1.1")['MaxRetries'],2)
        JobState.setMaxRetries("jobClassID_1.1",3)
        self.assertEqual(JobState.general("jobClassID_1.1")['MaxRetries'],3)
        jobIDs=JobState.retrieveJobIDs("myWorkflowID1")
        self.assertEqual(len(jobIDs),20)
        jobIDs=JobState.retrieveJobIDs(["myWorkflowID1","myWorkflowID2","myWorkflowID3"])
        self.assertEqual(len(jobIDs),60)
        jobs=JobState.rangeGeneral(0,10)
        print(str(jobs))

    def runTest(self):
        # testA-K are also used for the error handler test
        self.testA()
        self.testB()
        self.testC()
        self.testD()
        self.testE()
        self.testF()
        self.testG()
        self.testH()
        self.testI()
        self.testJ()
        self.testK()
        
            
if __name__ == '__main__':
    unittest.main()
