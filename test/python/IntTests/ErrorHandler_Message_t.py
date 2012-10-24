#!/usr/bin/env python
"""
_ServerTest_


"""
import os
import unittest
import time

from MessageService.MessageService import MessageService


class ComponentServerTest(unittest.TestCase):
    """
    TestCase implementation for ServerTest
    """
    def setUp(self):
        print "******Start ComponentServerTest (ErrorHandler) ***********"
        print "\nThis test depends on data generated by the JobState_t.py"
        print " and FwkJobReport_t.py tests and should NOT be run"
        print " separately, but only in a test suite "
        print " Make sure ONLY the error handler component is running!"
        print " "

        # we use this for event publication.
        self.ms=MessageService()
        self.ms.registerAs("TestComponent")
        # subscribe on the events this test produces
        # so we can verify this in the database
        self.ms.subscribeTo("CreateJob")
        self.ms.subscribeTo("GeneralJobFailure")
        self.ms.subscribeTo("SubmitJob")

        
        self.outputPath=os.getenv('PRODAGENT_WORKDIR')

    def testA(self):
        print("""\nPublish events to turn ErrorHandler logging on""")
        try:
            self.ms.publish("ErrorHandler:StartDebug", "none")
            self.ms.publish("JobCleanup:StartDebug", "none")
            self.ms.commit()
        except StandardError, ex:
            msg = "Failed testA\n"
            msg += str(ex)
            self.fail(msg)

    def testB(self):
        print("""\nPublish job failed, create failed and submission failed events""")

        try:
            #The payload will consist of an  url to a JobReport
            #in this test the url is based on files created in
            #another unit test (FwkJobReport_t.py)
            fileUrl="file://"+self.outputPath+"/jobReportTest1.xml"
            self.ms.publish("JobFailed", fileUrl)

            fileUrl="file://"+self.outputPath+"/jobReportTest2.xml"
            self.ms.publish("JobFailed", fileUrl)

            fileUrl="file://"+self.outputPath+"/jobReportTest3.xml"
            self.ms.publish("JobFailed", fileUrl)

            for i in xrange(0,9):
               self.ms.publish("SubmissionFailed", "jobClassID10")
               self.ms.publish("CreateFailed", "jobClassID7")
               self.ms.publish("CreateFailed", "jobClassID8")
               self.ms.publish("CreateFailed", "jobClassID9")
               self.ms.commit()

        except StandardError, ex:
            msg = "Failed testB:\n"
            msg += str(ex)
            self.fail(msg)

    def testC(self):
        print("\nPublication of events that will lead to either")
        print("a transition exception or a retry exception or other")
        print("errors.  These errors should be handled internally by the")
        print("code and registered in the log file")

        try:
            # ther have been more failures than retries are possible.
            self.ms.publish("SubmissionFailed", \
                                    "jobClassID5")
            # there is no jobClassID6 
            self.ms.publish("SubmissionFailed", \
                                    "jobClassID6")
            #note that we supply the wrong payload.
            self.ms.publish("JobFailed", \
                                    "jobClassID5")
            self.ms.commit()
            print("sleeping for 20 seconds, as there might")
            print("be a delay in getting messages by the component")
            time.sleep(20)

        except StandardError, ex:
            msg = "Failed testC:\n"
            msg += str(ex)
            self.fail(msg)

    def testD(self):
        self.ms.purgeMessages()

    def runTest(self):
         self.testA()
         self.testB()
         self.testC()
         self.testD()

if __name__ == '__main__':
    unittest.main()

    