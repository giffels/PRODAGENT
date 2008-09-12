#!/usr/bin/env python
"""
_ServerTest_

Start an instance of a server in its own process,
and test communicating with it using Message Service


"""
import os
import socket
import unittest
import threading
import time
import xmlrpclib

from MessageService.MessageService import MessageService


class ComponentServerTest(unittest.TestCase):
    """
    TestCase implementation for ServerTest
    """
    def setUp(self):
        print "**************NOTE ComponentServerTest***********"
        print "This test depends on data generated by the JobState_t.py"
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
        """publish events to turn logging on"""
        try:
            self.ms.publish("ErrorHandler:StartDebug", "none")
            self.ms.commit()
        except StandardError, ex:
            msg = "Failed calling ms.publish:\n"
            msg += str(ex)
            self.fail(msg)

    def testB(self):
        """test publish of events"""

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

            self.ms.publish("SubmissionFailed", "jobClassID10")
            self.ms.publish("CreateFailed", "jobClassID7")
            self.ms.publish("CreateFailed", "jobClassID8")
            self.ms.publish("CreateFailed", "jobClassID9")
            self.ms.commit()

        except StandardError, ex:
            msg = "Failed calling ms.publish:\n"
            msg += str(ex)
            self.fail(msg)

    def testC(self):
        """
        publication of events that will lead to either 
        a transition exception or a retry exception or other 
        errors.  These errors should be handled internally by the
        code and registered in the log file.
        """
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
            print("sleeping for 40 seconds, as there might")
            print("be a 5 second polling delay in getting")
            print("messages")
            time.sleep(40)

        except StandardError, ex:
            msg = "Failed calling server.publish:\n"
            msg += str(ex)
            self.fail(msg)
    def runTest(self):
         self.testA()
         self.testB()
         self.testC()

if __name__ == '__main__':
    unittest.main()

    
