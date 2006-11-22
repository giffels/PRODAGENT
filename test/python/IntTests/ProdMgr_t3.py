#!/usr/bin/env python
"""
Unittest ProdMgr.ProdMgr module
"""

import os
import random
import sys
import time
import unittest
from xml.dom.minidom import parse

from MessageService.MessageService import MessageService

from FwkJobRep.FwkJobReport import FwkJobReport
from FwkJobRep.ReportParser import readJobReport
from MCPayloads.JobSpec import JobSpec



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
        self.prodMgrUrl='https://localhost:8443/clarens/'
        self.jobReportDir='/tmp/prodAgent/ProdMgrInterface/jobReportDir'
        try:
            os.makedirs(self.jobReportDir)
        except:
            pass

    
    def testA(self):
        try:
            ###shell start####
            self.ms.publish("ProdMgrInterface:StartDebug",'')
            self.ms.commit()
            self.ms.publish("ProdMgrInterface:JobSize",'3')
            self.ms.commit()
            self.ms.publish("ProdMgrInterface:AddRequest",'https://localhost:8443/clarens/?Request_id=requestID0?Priority=3')
            self.ms.commit()
            self.ms.publish("ProdMgrInterface:ResourcesAvailable",'4')
            self.ms.commit()
            print('Waiting for 4 creatjobs')
            ###shell end ####
            for i in xrange(0,4): 
                type, payload = self.ms.get()
                print("Message type: "+str(type)+", payload: "+str(payload))
                # retrieve the job spec id (jobname)
                jobspec=JobSpec()
                jobspec.load(payload)
                ProdMgrUnitTests.__jobSpecId.append(jobspec.parameters['JobName'])
                self.ms.commit()
            for jobspecid in ProdMgrUnitTests.__jobSpecId:
              print("handling jobspecid: "+str(jobspecid))
              reportFile='FrameworkJobReport.xml'
              report=readJobReport(reportFile)
              for fileinfo in report[-1].files:
                  if  fileinfo['TotalEvents'] != None:
                      fileinfo['TotalEvents'] = 3
              report[-1].jobSpecId=jobspecid
              report[-1].status="Success"
              reportLocation=self.jobReportDir+'/'+jobspecid.replace('/','_')+".xml"
              report[-1].write(reportLocation)
              self.ms.publish("JobSuccess", reportLocation)
              self.ms.commit()
            ###shell start####
            self.ms.publish("ProdMgrInterface:ResourcesAvailable",'2')
            self.ms.commit()
            print('Waiting for 2 creatjobs')
            ###shell end ####
            ProdMgrUnitTests.__jobSpecId=[]
            for i in xrange(0,2): 
                type, payload = self.ms.get()
                print("Message type: "+str(type)+", payload: "+str(payload))
                # retrieve the job spec id (jobname)
                jobspec=JobSpec()
                jobspec.load(payload)
                ProdMgrUnitTests.__jobSpecId.append(jobspec.parameters['JobName'])
                self.ms.commit()
            for jobspecid in ProdMgrUnitTests.__jobSpecId:
              print("handling jobspecid: "+str(jobspecid))
              reportFile='FrameworkJobReport.xml'
              report=readJobReport(reportFile)
              for fileinfo in report[-1].files:
                  if  fileinfo['TotalEvents'] != None:
                      fileinfo['TotalEvents'] = 3
              report[-1].jobSpecId=jobspecid
              report[-1].status="Success"
              reportLocation=self.jobReportDir+'/'+jobspecid.replace('/','_')+".xml"
              report[-1].write(reportLocation)
              self.ms.publish("JobSuccess", reportLocation)
              self.ms.commit()
            ###shell start####
            self.ms.publish("ProdMgrInterface:AddRequest",'https://localhost:8443/clarens/?Request_id=requestID1?Priority=4')
            self.ms.commit()
            self.ms.publish("ProdMgrInterface:ResourcesAvailable",'10')
            self.ms.commit()
            print('Waiting for 10 creatjobs')
            ###shell end ####
            ProdMgrUnitTests.__jobSpecId=[]
            for i in xrange(0,10): 
                type, payload = self.ms.get()
                print("Message type: "+str(type)+", payload: "+str(payload))
                # retrieve the job spec id (jobname)
                jobspec=JobSpec()
                jobspec.load(payload)
                ProdMgrUnitTests.__jobSpecId.append(jobspec.parameters['JobName'])
                self.ms.commit()
            for jobspecid in ProdMgrUnitTests.__jobSpecId:
              print("handling jobspecid: "+str(jobspecid))
              reportFile='FrameworkJobReport.xml'
              report=readJobReport(reportFile)
              for fileinfo in report[-1].files:
                  if  fileinfo['TotalEvents'] != None:
                      fileinfo['TotalEvents'] = 3
              report[-1].jobSpecId=jobspecid
              report[-1].status="Success"
              reportLocation=self.jobReportDir+'/'+jobspecid.replace('/','_')+".xml"
              report[-1].write(reportLocation)
              self.ms.publish("JobSuccess", reportLocation)
              self.ms.commit()
            print('ProdMgr is left with 8 allocations as 2 allocations successfully finished')
            ###shell start####
            self.ms.publish("ProdMgrInterface:RemoveIdlingAllocs",'00:00:01')
            print('All idling allocations should have been removed since the used a small time interval')
            self.ms.commit()
            self.ms.publish("ProdMgrInterface:ResourcesAvailable",'20')
            self.ms.commit()
            print('ProdAgent should now have 20 active allocations (inlcuding the ones that where removed)')
            ###shell end ####
            ProdMgrUnitTests.__jobSpecId=[]
            for i in xrange(0,20): 
                type, payload = self.ms.get()
                print("Message type: "+str(type)+", payload: "+str(payload))
                # retrieve the job spec id (jobname)
                jobspec=JobSpec()
                jobspec.load(payload)
                ProdMgrUnitTests.__jobSpecId.append(jobspec.parameters['JobName'])
                self.ms.commit()
            #raw_input("Shut down the server to test queueing capability (check the log to see when no more messages enter)\n")
            for jobspecid in ProdMgrUnitTests.__jobSpecId[0:4]:
              print("handling jobspecid: "+str(jobspecid))
              reportFile='FrameworkJobReport.xml'
              report=readJobReport(reportFile)
              for fileinfo in report[-1].files:
                  if  fileinfo['TotalEvents'] != None:
                      fileinfo['TotalEvents'] = 3
              report[-1].jobSpecId=jobspecid
              report[-1].status="Success"
              reportLocation=self.jobReportDir+'/'+jobspecid.replace('/','_')+".xml"
              report[-1].write(reportLocation)
              self.ms.publish("JobSuccess", reportLocation)
              self.ms.commit()
            #raw_input("Start server again\n")
            for jobspecid in ProdMgrUnitTests.__jobSpecId[4:]:
              print("handling jobspecid: "+str(jobspecid))
              reportFile='FrameworkJobReport.xml'
              report=readJobReport(reportFile)
              for fileinfo in report[-1].files:
                  if  fileinfo['TotalEvents'] != None:
                      fileinfo['TotalEvents'] = 3
              report[-1].jobSpecId=jobspecid
              report[-1].status="Success"
              reportLocation=self.jobReportDir+'/'+jobspecid.replace('/','_')+".xml"
              report[-1].write(reportLocation)
              self.ms.publish("JobSuccess", reportLocation)
              self.ms.commit()
            ###shell start####
            print('Adding a non existing request')
            self.ms.publish("ProdMgrInterface:AddRequest",'https://localhost:8443/clarens/?Request_id=NOTEXISTINGREQUEST?Priority=1')
            self.ms.commit()
            print('Adding a request that alrady finished ')
            self.ms.publish("ProdMgrInterface:AddRequest",'https://localhost:8443/clarens/?Request_id=requestID0?Priority=2')
            self.ms.commit()
            self.ms.publish("ProdMgrInterface:AddRequest",'https://localhost:8443/clarens/?Request_id=requestID2?Priority=5')
            print('There should now be 3 additional requests in the request queue')
            self.ms.commit()
            self.ms.publish("ProdMgrInterface:JobSize",'5')
            self.ms.commit()
            self.ms.publish("ProdMgrInterface:ResourcesAvailable",'10')
            self.ms.commit()
            ###shell end ####
            ProdMgrUnitTests.__jobSpecId=[]
            for i in xrange(0,10): 
                type, payload = self.ms.get()
                print("Message type: "+str(type)+", payload: "+str(payload))
                # retrieve the job spec id (jobname)
                jobspec=JobSpec()
                jobspec.load(payload)
                ProdMgrUnitTests.__jobSpecId.append(jobspec.parameters['JobName'])
                self.ms.commit()
            for jobspecid in ProdMgrUnitTests.__jobSpecId[0:4]:
              print("handling jobspecid: "+str(jobspecid))
              reportFile='FrameworkJobReport.xml'
              report=readJobReport(reportFile)
              for fileinfo in report[-1].files:
                  if  fileinfo['TotalEvents'] != None:
                      fileinfo['TotalEvents'] = 2
              report[-1].jobSpecId=jobspecid
              report[-1].status="Success"
              reportLocation=self.jobReportDir+'/'+jobspecid.replace('/','_')+".xml"
              report[-1].write(reportLocation)
              self.ms.publish("JobSuccess", reportLocation)
              self.ms.commit()
            for jobspecid in ProdMgrUnitTests.__jobSpecId[4:]:
              print("handling jobspecid: "+str(jobspecid))
              reportFile='FrameworkJobReport.xml'
              report=readJobReport(reportFile)
              for fileinfo in report[-1].files:
                  if  fileinfo['TotalEvents'] != None:
                      fileinfo['TotalEvents'] = 5
              report[-1].jobSpecId=jobspecid
              report[-1].status="Success"
              reportLocation=self.jobReportDir+'/'+jobspecid.replace('/','_')+".xml"
              report[-1].write(reportLocation)
              self.ms.publish("JobSuccess", reportLocation)
              self.ms.commit()
            ###shell start####
            print('Emitting resources available which should get allocations of multiple requests')
            self.ms.publish("ProdMgrInterface:ResourcesAvailable",'15')
            self.ms.commit()
            print('There should be now 15 active allocations and the finished request and nonexisting request are removed')
            ###shell end ####
            ProdMgrUnitTests.__jobSpecId=[]
            for i in xrange(0,15): 
                type, payload = self.ms.get()
                print("Message type: "+str(type)+", payload: "+str(payload))
                # retrieve the job spec id (jobname)
                jobspec=JobSpec()
                jobspec.load(payload)
                ProdMgrUnitTests.__jobSpecId.append(jobspec.parameters['JobName'])
                self.ms.commit()
            for jobspecid in ProdMgrUnitTests.__jobSpecId:
              print("handling jobspecid: "+str(jobspecid))
              reportFile='FrameworkJobReport.xml'
              report=readJobReport(reportFile)
              for fileinfo in report[-1].files:
                  if  fileinfo['TotalEvents'] != None:
                      fileinfo['TotalEvents'] = 3
              report[-1].jobSpecId=jobspecid
              report[-1].status="Success"
              reportLocation=self.jobReportDir+'/'+jobspecid.replace('/','_')+".xml"
              report[-1].write(reportLocation)
              self.ms.publish("GeneralJobFailure", reportLocation)
              self.ms.commit()
            ###shell start####
            self.ms.publish("ProdMgrInterface:AddRequest",'https://localhost:8443/clarens/?Request_id=requestID5?Priority=3')
            self.ms.commit()
            self.ms.publish("ProdMgrInterface:AddRequest",'https://localhost:8443/clarens/?Request_id=requestID6?Priority=3')
            self.ms.commit()
            self.ms.publish("ProdMgrInterface:AddRequest",'https://localhost:8443/clarens/?Request_id=requestID7?Priority=3')
            self.ms.commit()
            self.ms.publish("ProdMgrInterface:ResourcesAvailable",'15')
            self.ms.commit()
            ###shell end ####
        except StandardError, ex:
            msg = "Failed testA\n"
            msg += str(ex)
            self.fail(msg)

    def runTest(self):
        self.testA()
            
if __name__ == '__main__':
    unittest.main()
