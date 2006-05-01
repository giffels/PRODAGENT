#!/usr/bin/env python
"""
Unittest JobState.JobStateAPI module
"""


import unittest

from JobState.JobStateAPI import JobStateChangeAPI
from JobState.JobStateAPI import JobStateInfoAPI


class JobStateUnitTests(unittest.TestCase):
    """
    TestCase for JobStateAPI module 
    """

    def setUp(self):
        print "**************NOTE JobStateUnitTests***********"
        print "Make sure the test input does not conflict"
        print "with the data in the database!"
        print " "
        print "Make sure the database (and client) are properly"
        print "configured."
        print " "

    def testA(self):
        """change state test"""
        try:
         # illegal state transitions:
         try:
            JobStateChangeAPI.create("jobClassID1","cacheDir/location/1somewhere")
         except Exception, ex:
            print('Testing exception 1/2 in testA of JobState_t.py')
            self.assertEqual(ex[1],'Illegal state transition: Undefined-->create')
         JobStateChangeAPI.register("jobClassID1","processing",3,1)

         try:
         # illegal state transitions:
            JobStateChangeAPI.inProgress("jobClassID1")
         except Exception, ex:
            print('Testing exception 2/2 in testA of JobState_t.py')
            self.assertEqual(ex[1],'Illegal state transition: register-->inProgress')
         JobStateChangeAPI.create("jobClassID1","cacheDir/location/1somewhere")
         JobStateChangeAPI.inProgress("jobClassID1")

         # retries=racers=0;
         self.assertEqual(JobStateInfoAPI.general("jobClassID1"), {'Retries': 0, 'CacheDirLocation': 'cacheDir/location/1somewhere', 'MaxRacers': 1, 'Racers': 0, 'State': 'inProgress', 'MaxRetries': 3, 'JobType': 'processing'}
)


         JobStateChangeAPI.submit("jobClassID1")

         # retries=0, racers=1;
         self.assertEqual(JobStateInfoAPI.general("jobClassID1"), {'Retries': 0L, 'CacheDirLocation': 'cacheDir/location/1somewhere', 'MaxRacers': 1L, 'Racers': 1L, 'State': 'inProgress', 'MaxRetries': 3L, 'JobType': 'processing'})

         JobStateChangeAPI.runFailure("jobClassID1","jobInstanceID1.1",
              "some.location1.1","job/Report/Location1.1.xml")
         JobStateChangeAPI.submit("jobClassID1")
        except StandardError, ex:
            msg = "Failed State Change TestA:\n"
            msg += str(ex)
            self.fail(msg)


    def testB(self):
        """change state test"""
        try:
         JobStateChangeAPI.register("jobClassID2","processing",2,1)
         JobStateChangeAPI.create("jobClassID2","cacheDir/location/2somewhere")
         JobStateChangeAPI.inProgress("jobClassID2")

         # retries=racers=0
         self.assertEqual(JobStateInfoAPI.general("jobClassID2"), {'Retries': 0, 'CacheDirLocation': 'cacheDir/location/2somewhere', 'MaxRacers': 1, 'Racers': 0, 'State': 'inProgress', 'MaxRetries': 2, 'JobType': 'processing'})

         JobStateChangeAPI.submit("jobClassID2")

         # retries0,=racers=1
         self.assertEqual(JobStateInfoAPI.general("jobClassID2"),{'Retries': 0, 'CacheDirLocation': 'cacheDir/location/2somewhere', 'MaxRacers': 1, 'Racers': 1, 'State': 'inProgress', 'MaxRetries': 2, 'JobType': 'processing'})

         JobStateChangeAPI.runFailure("jobClassID2","jobInstanceID2.1",
              "some.location2.1","job/Report/Location2.1.xml")

         # retries= 1, racers=0
         self.assertEqual(JobStateInfoAPI.general("jobClassID2"),
              {'CacheDirLocation': 'cacheDir/location/2somewhere', 
               'MaxRacers': 1, 'Racers': 0, 'State': 'inProgress', 
               'MaxRetries': 2, 'Retries': 1, 'JobType': 'processing'})

         JobStateChangeAPI.submit("jobClassID2")

         # retries= 1, racers=1
         self.assertEqual(JobStateInfoAPI.general("jobClassID2"),{'Retries': 1L, 'CacheDirLocation': 'cacheDir/location/2somewhere', 'MaxRacers': 1L, 'Racers': 1L, 'State': 'inProgress', 'MaxRetries': 2L, 'JobType': 'processing'})

        except StandardError, ex:
            msg = "Failed State Change TestB:\n"
            msg += str(ex)
            self.fail(msg)

    def testC(self):
        """change state test"""
        try:
         JobStateChangeAPI.register("jobClassID3","merging",5,1)
         JobStateChangeAPI.create("jobClassID3","cacheDir/location/3somewhere")
         JobStateChangeAPI.inProgress("jobClassID3")
         JobStateChangeAPI.submit("jobClassID3")

         # try an illegal state transition:
         try:
              JobStateChangeAPI.create("jobClassID3","cacheDir/location3somewhere")
         except Exception, ex:
              print('Testing exception 1/3 in testC of JobState_t.py')
              self.assertEqual(str(ex[1]),'Illegal state transition: inProgress-->create')

        # try to submit another job while the first one has not finished (we only are allowed one racer)
         try:
              JobStateChangeAPI.submit("jobClassID3")
         except Exception, ex:
              print('Testing exception 2/3 in testC of JobState_t.py')
              self.assertEqual(str(ex[1]),'job with id: jobClassID3 is already submitted will not resubmit')

        # set the maximum number of racers higher and submit again.
         JobStateChangeAPI.setRacer("jobClassID3",50)
         JobStateChangeAPI.submit("jobClassID3")
         JobStateChangeAPI.submit("jobClassID3")
         JobStateChangeAPI.submit("jobClassID3")
         JobStateChangeAPI.submit("jobClassID3")

        # althought the number of racers has been set higher, we are now
        # bound by the maximum number of retries.
         try:
              JobStateChangeAPI.submit("jobClassID3")
         except Exception, ex:
              print('Testing exception 3/3 in testC of JobState_t.py')
              self.assertEqual(str(ex[1]),'reached maximum number of retries 5 (this includes running jobs)')

        except StandardError, ex:
            msg = "Failed State Change TestC:\n"
            msg += str(ex)
            self.fail(msg)

    def testD(self):
        """change state test"""
        try:
         JobStateChangeAPI.register("jobClassID4","processing",6,2)
         JobStateChangeAPI.create("jobClassID4","cacheDir/location/4somewhere")
         JobStateChangeAPI.inProgress("jobClassID4")

         # retries=racers=0
         self.assertEqual(JobStateInfoAPI.general("jobClassID4"),{'Retries': 0L, 'CacheDirLocation': 'cacheDir/location/4somewhere', 'MaxRacers': 2L, 'Racers': 0L, 'State': 'inProgress', 'MaxRetries': 6L, 'JobType': 'processing'})

         JobStateChangeAPI.submit("jobClassID4")

         # retries=0, racers=1
         self.assertEqual(JobStateInfoAPI.general("jobClassID4"),{'Retries': 0L, 'CacheDirLocation': 'cacheDir/location/4somewhere', 'MaxRacers': 2L, 'Racers': 1L, 'State': 'inProgress', 'MaxRetries': 6L, 'JobType': 'processing'})


         JobStateChangeAPI.runFailure("jobClassID4","jobInstanceID4.0",
              "some.location4.0","job/Report/Location4.0.xml")

         # retries=1, racers=0
         self.assertEqual(JobStateInfoAPI.general("jobClassID4"),{'Retries': 1L, 'CacheDirLocation': 'cacheDir/location/4somewhere', 'MaxRacers': 2L, 'Racers': 0L, 'State': 'inProgress', 'MaxRetries': 6L, 'JobType': 'processing'})

         JobStateChangeAPI.submit("jobClassID4")

         # retries=1, racers=1
         self.assertEqual(JobStateInfoAPI.general("jobClassID4"),{'Retries': 1L, 'CacheDirLocation': 'cacheDir/location/4somewhere', 'MaxRacers': 2L, 'Racers': 1L, 'State': 'inProgress', 'MaxRetries': 6L, 'JobType': 'processing'})


         JobStateChangeAPI.runFailure("jobClassID4","jobInstanceID4.1",
              "some.location4.1","job/Report/Location4.1.xml")
         # retries=2, racers=0
         JobStateChangeAPI.submit("jobClassID4")
         # retries=2, racers=1
         JobStateChangeAPI.submit("jobClassID4")
         # retries=2, racers=2
         JobStateChangeAPI.runFailure("jobClassID4","jobInstanceID4.2",
              "some.location4.2","job/Report/Location4.2.xml")
         # retries=3, racers=1
         JobStateChangeAPI.submit("jobClassID4")
         # retries=3, racers=2
         self.assertEqual(JobStateInfoAPI.general("jobClassID4"),{'Retries': 3L, 'CacheDirLocation': 'cacheDir/location/4somewhere', 'MaxRacers': 2L, 'Racers': 2L, 'State': 'inProgress', 'MaxRetries': 6L, 'JobType': 'processing'})
         JobStateChangeAPI.finished("jobClassID4")
         self.assertEqual(JobStateInfoAPI.general("jobClassID4"),{'Retries': 3L, 'CacheDirLocation': 'cacheDir/location/4somewhere', 'MaxRacers': 2L, 'Racers': 2L, 'State': 'finished', 'MaxRetries': 6L, 'JobType': 'processing'})
        except StandardError, ex:
            msg = "Failed State Change TestD:\n"
            msg += str(ex)
            self.fail(msg)

    def testE(self):
        try:
         JobStateChangeAPI.register("jobClassID5","processing",2,2)
         JobStateChangeAPI.create("jobClassID5","cacheDir/location/5somewhere")
         JobStateChangeAPI.inProgress("jobClassID5")
         JobStateChangeAPI.submit("jobClassID5")

        # now introduce some failures until we have more failures
        # then retries (this raises an error)

         JobStateChangeAPI.runFailure("jobClassID5","jobInstanceID5.1",
              "some.location5.1","job/Report/Location5.1.xml")
         try:
              JobStateChangeAPI.runFailure("jobClassID5","jobInstanceID5.2",
                   "some.location5.1","job/Report/Location5.1.xml")
         except Exception, ex:
              print('Testing exception 1/1 in testE of JobState_t.py')
              self.assertEqual(ex[1],'Negative number of racers, is not possible, will not update ')
         JobStateChangeAPI.finished("jobClassID5")

        except StandardError, ex:
            msg = "Failed State Change TestE:\n"
            msg += str(ex)
            self.fail(msg)

    def testF(self):
        try:
         self.assertEqual(JobStateInfoAPI.lastLocations("jobClassID4"),\
           ["some.location4.0","some.location4.1","some.location4.2"])    
         self.assertEqual(JobStateInfoAPI.lastLocations("jobClassID2"),\
           ["some.location2.1"])
        except StandardError, ex:
            msg = "Failed State Change TestF:\n"
            msg += str(ex)
            self.fail(msg)

    def testG(self):
        try:
         reportList=JobStateInfoAPI.jobReports("jobClassID4")
         self.assertEqual(JobStateInfoAPI.jobReports("jobClassID4"), \
              ['job/Report/Location4.0.xml','job/Report/Location4.1.xml', 'job/Report/Location4.2.xml'])
        except StandardError, ex:
            msg = "Failed State Change TestG:\n"
            msg += str(ex)
            self.fail(msg)

    def testH(self):
         JobStateChangeAPI.register("jobClassID7","processing",8,2)
         JobStateChangeAPI.register("jobClassID8","processing",8,2)
         JobStateChangeAPI.register("jobClassID9","processing",8,2)

    def testI(self):
         JobStateChangeAPI.register("jobClassID10","processing",8,2)
         #retries=racer=0
         self.assertEqual(JobStateInfoAPI.general("jobClassID10"),{'Retries': 0, 'CacheDirLocation': None, 'MaxRacers': 2, 'Racers': 0, 'State': 'register', 'MaxRetries': 8, 'JobType': 'processing'})
         JobStateChangeAPI.createFailure("jobClassID10")
         #retries=1, racer=0
         self.assertEqual(JobStateInfoAPI.general("jobClassID10"),{'Retries': 1, 'CacheDirLocation': None, 'MaxRacers': 2, 'Racers': 0, 'State': 'register', 'MaxRetries': 8, 'JobType': 'processing'})
         JobStateChangeAPI.createFailure("jobClassID10")
         #retries=2, racer=0
         self.assertEqual(JobStateInfoAPI.general("jobClassID10"),{'Retries': 2, 'CacheDirLocation': None, 'MaxRacers': 2, 'Racers': 0, 'State': 'register', 'MaxRetries': 8, 'JobType': 'processing'})
         JobStateChangeAPI.create("jobClassID10","cacheDir/location/10somewhere")
         #retries=2, racer=0
         self.assertEqual(JobStateInfoAPI.general("jobClassID10"),{'Retries': 2, 'CacheDirLocation': 'cacheDir/location/10somewhere', 'MaxRacers': 2, 'Racers': 0, 'State': 'create', 'MaxRetries': 8, 'JobType': 'processing'})
         JobStateChangeAPI.inProgress("jobClassID10")
         #retries=2, racer=0
         self.assertEqual(JobStateInfoAPI.general("jobClassID10"),{'Retries': 2, 'CacheDirLocation': 'cacheDir/location/10somewhere', 'MaxRacers': 2, 'Racers': 0, 'State': 'inProgress', 'MaxRetries': 8, 'JobType': 'processing'})
         JobStateChangeAPI.submitFailure("jobClassID10")
         #retries=3, racer=0
         self.assertEqual(JobStateInfoAPI.general("jobClassID10"),{'Retries': 3, 'CacheDirLocation': 'cacheDir/location/10somewhere', 'MaxRacers': 2, 'Racers': 0, 'State': 'inProgress', 'MaxRetries': 8, 'JobType': 'processing'})
         JobStateChangeAPI.submit("jobClassID10")
         #retries=3, racer=1
         self.assertEqual(JobStateInfoAPI.general("jobClassID10"),{'Retries': 3, 'CacheDirLocation': 'cacheDir/location/10somewhere', 'MaxRacers': 2, 'Racers': 1, 'State': 'inProgress', 'MaxRetries': 8, 'JobType': 'processing'})
         JobStateChangeAPI.submitFailure("jobClassID10")
         #retries=4, racer=1
         self.assertEqual(JobStateInfoAPI.general("jobClassID10"),{'Retries': 4, 'CacheDirLocation': 'cacheDir/location/10somewhere', 'MaxRacers': 2, 'Racers': 1, 'State': 'inProgress', 'MaxRetries': 8, 'JobType': 'processing'})
         JobStateChangeAPI.submit("jobClassID10")
         #retries=4, racer=2
         self.assertEqual(JobStateInfoAPI.general("jobClassID10"),{'Retries': 4, 'CacheDirLocation': 'cacheDir/location/10somewhere', 'MaxRacers': 2, 'Racers': 2, 'State': 'inProgress', 'MaxRetries': 8, 'JobType': 'processing'})

         # on purpose we introduce an error:
         try:
             JobStateChangeAPI.submit("jobClassID10")
         except Exception, ex:
             print('Testing exception 1/1 in testH of JobState_t.py')
             self.assertEqual(ex[1],'job with id: jobClassID10 is already submitted will not resubmit')
         JobStateChangeAPI.runFailure("jobClassID10","jobInstanceID10.1",
              "some.location10.1","job/Report/Location10.1.xml")
         #retries=5, racer=1
         self.assertEqual(JobStateInfoAPI.general("jobClassID10"),{'Retries': 5, 'CacheDirLocation': 'cacheDir/location/10somewhere', 'MaxRacers': 2, 'Racers': 1, 'State': 'inProgress', 'MaxRetries': 8, 'JobType': 'processing'})
         JobStateChangeAPI.runFailure("jobClassID10","jobInstanceID10.2",
              "some.location10.2","job/Report/Location10.2.xml")
         #retries=6, racer=0
         self.assertEqual(JobStateInfoAPI.general("jobClassID10"),{'Retries': 6, 'CacheDirLocation': 'cacheDir/location/10somewhere', 'MaxRacers': 2, 'Racers': 0, 'State': 'inProgress', 'MaxRetries': 8, 'JobType': 'processing'})

    def testJ(self):

         self.assertEqual(JobStateInfoAPI.jobSpecTotal(),9)
         print(str(JobStateInfoAPI.rangeGeneral()))

    def runTest(self):
        self.testA()
        self.testB()
        self.testC()
        self.testD()
        self.testE()
        self.testF()
        self.testG()
        self.testH()
        self.testI()
            
if __name__ == '__main__':
    unittest.main()
