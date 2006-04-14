#!/usr/bin/env python
"""
Unittest FwkJobReport.FwkJobReport module
"""
import os
import unittest

from FwkJobRep.FwkJobReport import FwkJobReport
from FwkJobRep.ReportParser import readJobReport


class FwkJobReportTests(unittest.TestCase):
    """
    TestCase for FwkJobReport module 
    """

    def setUp(self):
        print "**************NOTE FwkJobReportTests***********"
        print "This test module will generate several xml files."
        print "in the data directory"
        print ""

        self.outputPath=os.getenv('PRODAGENT_WORKDIR')

    def testA(self):
        try:
           for i in [1,2,3]:
              fwkJobReport=FwkJobReport("jobClassID"+str(i))
              fwkJobReport.newFile()
              fwkJobReport.newFile()
              fwkJobReport.status="Failed"
              fwkJobReport.write(self.outputPath+"/jobReportTest"+str(i)+".xml")
              print('A file '+self.outputPath+'/jobReportTest'+str(i)+'.xml" has been created')
        except StandardError, ex:
            msg = "Failed Job Report Creation Test:\n"
            msg += str(ex)
            self.fail(msg)

    def testB(self):
         try:
              for i in [1,2,3]:
                 fwkJobReport=readJobReport(self.outputPath+"jobReportTest"+str(i)+".xml")
                 print('JobReport has been read')
                 self.assertEqual(fwkJobReport[0].name,"jobClassID"+str(i))
                 fwkJobReport[0].write(self.outputPath+"/jobReportTest"+str(i)+"."+"1.xml")
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
