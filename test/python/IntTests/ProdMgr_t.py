#!/usr/bin/env python
"""
Unittest ProdMgr.ProdMgr module
"""

import os
import random
import unittest
from xml.dom.minidom import parse

from MessageService.MessageService import MessageService

from FwkJobRep.FwkJobReport import FwkJobReport
from FwkJobRep.ReportParser import readJobReport



class ProdMgrUnitTests(unittest.TestCase):
    """
    TestCase for ProdMgr module which tests the component
    which involves interaction with the ProdMgr
    """

    # keep track of some jobspecs
    __jobSpecId=[]

    def setUp(self):
        # we use this for event publication.
        self.ms=MessageService()
        self.ms.registerAs("TestComponent")
        # subscribe on the events this test produces
        # so we can verify this in the database
        self.ms.subscribeTo("CreateJob")
        self.requests=5
        self.prodMgrUrl='https://localhost:8444/'
        self.jobReportDir='/tmp/prodAgent/ProdMgrInterface/jobReportDir'
        try:
            os.makedirs(self.jobReportDir)
        except:
            pass

    
    def testA(self):
        msg="""
  These tests requires a running prodMgr and the urls 
  used in this test need to be consistent with the urls 
  where the different prodMgr(s) are running. In order 
  to successfully run this test you first need to run test 
  ProdMgrInterfactPrepare_t.py of the prodMgr as this 
  will create the necessary requests.

   >>>Make sure the ProdMgrInterface component is running<<<!
   >>>Make sure the ProdMgr Clarens services are running and
   that the urls used point to the proper clarens server <<<!

   Press any key to continue
         """ 
        raw_input(msg)
        try:
            print("--->sending debug events")
            self.ms.publish("ProdMgrInterface:StartDebug", "none")
            print("start resources available test")
            for i in xrange(0,self.requests):
               priority=random.randint(0,10)
               self.ms.publish("ProdMgrInterface:AddRequest", self.prodMgrUrl+"?RequestId=requestID_"+str(i)+"?Priority="+str(priority))
               self.ms.commit()
            self.ms.publish("ResourcesAvailable", str(15))
            self.ms.commit()
          
            # check if some jobs where created 
            for i in xrange(0,15): 
                type, payload = self.ms.get()
                print("Message type: "+str(type)+", payload: "+str(payload))
                # retrieve the job spec id (jobname)
                dom=parse(payload)
                jobspecs=dom.getElementsByTagName("jobspec")
                ProdMgrUnitTests.__jobSpecId.append(jobspecs[0].getAttribute("JobName"))
                self.ms.commit()
            print("More Resources Available: ######################")
            # again publish resources available.
            self.ms.publish("ResourcesAvailable", str(30))
            self.ms.commit()
            for i in xrange(0,30): 
                type, payload = self.ms.get()
                print("Message type: "+str(type)+", payload: "+str(payload))
                # retrieve the job spec id (jobname)
                dom=parse(payload)
                jobspecs=dom.getElementsByTagName("jobspec")
                ProdMgrUnitTests.__jobSpecId.append(jobspecs[0].getAttribute("JobName"))
                self.ms.commit()
            #self.ms.publish("JobSuccess", "ASuccessJobSpecID")
            #self.ms.publish("GeneralJobFailure", "AFailedJobSpecID")

        except StandardError, ex:
            msg = "Failed testA\n"
            msg += str(ex)
            self.fail(msg)

    def testB(self):
        print("start publications of success and failure. We have "+\
           str(len(ProdMgrUnitTests.__jobSpecId))+" jobspecs to deal with")
        # here we mimic the steps in the prodagent, we generate
        # a job report and publish a success or failure with a pointer
        # to this report. 
        try:
           for jobspecid in ProdMgrUnitTests.__jobSpecId[:20]:
              print("handling jobspecid: "+str(jobspecid))
              reportFile='FrameworkJobReport.xml'
              report=readJobReport(reportFile)
              for fileinfo in report[-1].files:
                  if  fileinfo['TotalEvents'] != None:
                      fileinfo['TotalEvents'] = 20
              report[-1].jobSpecId=jobspecid
              report[-1].status="Success"
              reportLocation=self.jobReportDir+'/'+jobspecid.replace('/','_')+".xml"
              report[-1].write(reportLocation)
              self.ms.publish("JobSuccess", reportLocation)
              self.ms.commit()
           raw_input("Please shutdown the server we are interacting with so to "+\
               "test the messaging queing functionality. Before doing this check the"+\
               " ProdMgrInterface log file for any anomalies (errors) and check the "+\
               " pm_allocation table to see if allocations are set to idle. ")
           for jobspecid in ProdMgrUnitTests.__jobSpecId[20:27]:
              print("handling jobspecid: "+str(jobspecid))
              reportFile='FrameworkJobReport.xml'
              report=readJobReport(reportFile)
              for fileinfo in report[-1].files:
                  if  fileinfo['TotalEvents'] != None:
                      fileinfo['TotalEvents'] = 20
              report[-1].jobSpecId=jobspecid
              report[-1].status="Success"
              reportLocation=self.jobReportDir+'/'+jobspecid.replace('/','_')+".xml"
              report[-1].write(reportLocation)
              self.ms.publish("JobSuccess", reportLocation)
              self.ms.commit()
           raw_input("Please start the server again to resume with job success reports."+\
              " Before doing this you can check the ws_queue table, and the ProdMgrInterface"+\
              " log to view how the lack of a connection was handled. There should be several "+\
              " entries in the ws_queue table.")
           for jobspecid in ProdMgrUnitTests.__jobSpecId[27:30]:
              print("handling jobspecid: "+str(jobspecid))
              reportFile='FrameworkJobReport.xml'
              report=readJobReport(reportFile)
              for fileinfo in report[-1].files:
                  if  fileinfo['TotalEvents'] != None:
                      fileinfo['TotalEvents'] = 20
              report[-1].jobSpecId=jobspecid
              report[-1].status="Success"
              reportLocation=self.jobReportDir+'/'+jobspecid.replace('/','_')+".xml"
              report[-1].write(reportLocation)
              self.ms.publish("JobSuccess", reportLocation)
              self.ms.commit()
           raw_input("The next events will emit job failures. The amount of the job failures"+\
              " is large enough that this prodagent will be set to a cooloff state and hence"+\
              " can not acquire any allocations or jobs for a while")
           for jobspecid in ProdMgrUnitTests.__jobSpecId[30:45]:
              print("handling jobspecid: "+str(jobspecid))
              reportFile='FrameworkJobReport.xml'
              report=readJobReport(reportFile)
              for fileinfo in report[-1].files:
                  if  fileinfo['TotalEvents'] != None:
                      fileinfo['TotalEvents'] = 20
              report[-1].jobSpecId=jobspecid
              report[-1].status="Failure"
              reportLocation=self.jobReportDir+'/'+jobspecid.replace('/','_')+".xml"
              report[-1].write(reportLocation)
              self.ms.publish("GeneralJobFailure", reportLocation)
              self.ms.commit()
            
        except StandardError, ex:
            msg = "Failed testB\n"
            msg += str(ex)
            self.fail(msg)

    def testC(self):
        try:
            print("start resources available test2")
            self.ms.publish("ResourcesAvailable", str(15))
            self.ms.commit()
        except StandardError, ex:
            msg = "Failed testC\n"
            msg += str(ex)
            self.fail(msg)


    def runTest(self):
        self.testA()
            
if __name__ == '__main__':
    unittest.main()
