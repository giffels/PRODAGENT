#!/usr/bin/env python
"""
Unittest FwkJobReport.FwkJobReport module
"""
import os
import unittest

from FwkJobRep.FwkJobReport import FwkJobReport
from FwkJobRep.ReportParser import readJobReport
from FwkJobRep.MergeReports import mergeReports


class FwkJobReportTests(unittest.TestCase):
    """
    TestCase for FwkJobReport module 
    """

    def setUp(self):
        print "\n**************Start FwkJobReportTests**********"
        prodAgentEnv = os.environ.get('PRODAGENT_WORKDIR', None)
        if prodAgentEnv == None:
            proidAgentEnv = "/tmp"
        self.outputPath = prodAgentEnv


    def testAA(self):
        print("""\nJob report instantiation""")
        try:
            jobRep = FwkJobReport()
        except StandardError, ex:
            msg = "Failed to instantiate FwkJobReport:\n"
            msg += str(ex)
            self.fail(msg)


    def testBB(self):
        print("""\nAdding information to job report""")
        jobRep = FwkJobReport()
        jobRep.exitCode = 0
        jobRep.status = "Success"
        self.assertEqual(jobRep.wasSuccess(), True)
        jobRep.exitCode = 127
        self.assertEqual(jobRep.wasSuccess(), False)
        jobRep.status = "Failed"
        self.assertEqual(jobRep.wasSuccess(), False)


        file1 = jobRep.newFile()
        file2 = jobRep.newFile()
        file3 = jobRep.newFile()

        self.assertEqual(len(jobRep.files), 3)
        
        infile1 = jobRep.newInputFile()
        infile2 = jobRep.newInputFile()
        infile3 = jobRep.newInputFile()

        self.assertEqual(len(jobRep.inputFiles), 3)

        jobRep.addSkippedEvent(10001, 1001)
        jobRep.addSkippedEvent(20002, 2002)
        jobRep.addSkippedEvent(30003, 3003)

        self.assertEqual(len(jobRep.skippedEvents), 3)


        try:
            jobRep.save()
        except StandardError, ex:
            msg = "Error invoking FwkJobReport.save method"
            msg += str(ex)
            self.fail(msg)
            

    def testA(self):
        print("""\nCreate job reports""")
        try:
           for i in [1,2,3]:
              fwkJobReport=FwkJobReport("jobClassID"+str(i))
              fwkJobReport.status="Failed"
              fwkJobReport.jobSpecId="jobClassID"+str(i)
              fwkJobReport.write(self.outputPath+"/jobReportTest"+str(i)+".xml")
              mergeReports(self.outputPath+"/jobReportTest"+str(i)+".xml",self.outputPath+"/jobReportTest"+str(i)+".xml")
              print('A file '+self.outputPath+'/jobReportTest'+str(i)+'.xml" has been created')
        except StandardError, ex:
            msg = "Failed Job Report Creation Test:\n"
            msg += str(ex)
            self.fail(msg)

    def testB(self):
         print("""\nRead and duplicate job reports""")
         try:
              for i in [1,2,3]:
                 fwkJobReport=readJobReport(self.outputPath+"/jobReportTest"+str(i)+".xml")
                 print('JobReport has been read')
                 self.assertEqual(fwkJobReport[0].name,"jobClassID"+str(i))
                 self.assertEqual(fwkJobReport[0].jobSpecId,"jobClassID"+str(i))
                 fwkJobReport[0].write(self.outputPath+"/jobReportTest"+str(i)+"."+"1.xml")
                 mergeReports(self.outputPath+"/jobReportTest"+str(i)+".1.xml",self.outputPath+"/jobReportTest"+str(i)+".1.xml")
                 print('A file '+self.outputPath+'/jobReportTest'+str(i)+'1.xml" has been created')
         except StandardError, ex:
              msg = "Failed Job Report Creation Test:\n"
              msg += str(ex)
              self.fail(msg)




    def runTest(self):
         self.testA()
         self.testB()

if __name__ == '__main__':
    unittest.main()
