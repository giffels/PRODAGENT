#!/usr/bin/env python
"""
Unittest for DashboardInfo
"""

import unittest
import os
from ShREEK.CMSPlugins.DashboardInfo import DashboardInfo



class DashboardInfoTest(unittest.TestCase):
    """
    TestCase for DashboardInfo class
    """
    def setUp(self):
        """setup"""
        self.testFile = os.path.join(os.getcwd(), "DashboardInfo_t.xml")

    def tearDown(self):
        """cleanup"""
        if os.path.exists(self.testFile):
            os.remove(self.testFile)

    def testA(self):
        """instantiation"""
        try:
            info = DashboardInfo()
        except StandardError, ex:
            msg = "Failed to instantiate DashboardInfo Object:\n"
            msg += str(ex)
            self.fail(msg)

    def testB(self):
        """test serialisation"""
        info = DashboardInfo()
        info.job = "JobName"
        info.task = "TaskName"
        info.addDestination("cms-pamon.cern.ch", 8884)
        info.addDestination("cithep90.ultralight.org", 58884)
        info['FloatParam'] = 1.234
        info['IntParam'] = 1234
        info['StringParam'] = "this is a string"
        info['NoneParam'] = None

        try:
            node = info.save()
        except StandardError, ex:
            msg = "Error calling DashboardInfo.save:\n"
            msg += str(ex)
            self.fail(msg)

        try:
            info2 = DashboardInfo()
            info2.load(node)
        except StandardError, ex:
            msg = "Error calling DashboardInfo.load:\n"
            msg += str(ex)
            self.fail(msg)
            

        self.assertEqual(info2, info)
        self.assertEqual(info2.job, info.job)
        self.assertEqual(info2.task, info.task)

        
        self.assertEqual(type(info['NoneParam']), type(info2['NoneParam']))
        self.assertEqual(type(info['IntParam']), type(info2['IntParam']))
        self.assertEqual(type(info['FloatParam']), type(info2['FloatParam']))
        self.assertEqual(type(info['StringParam']), type(info2['StringParam']))
        self.assertEqual(info.destinations, info2.destinations)
        

    def testC(self):
        """test save load to file"""
        info = DashboardInfo()
        info.job = "JobName"
        info.task = "TaskName"
        info.addDestination("cms-pamon.cern.ch", 8884)
        info.addDestination("cithep90.ultralight.org", 58884)
        try:
            info.write(self.testFile)
        except StandardError, ex:
            msg = "Error calling DashboardInfo.write:\n"
            msg += str(ex)
            self.fail(msg)

        try:
            info2 = DashboardInfo()
            info2.read(self.testFile)
        except StandardError, ex:
            msg = "Error calling DashboardInfo.read:\n"
            msg += str(ex)
            self.fail(msg)

        self.assertEqual(info, info2)
        self.assertEqual(info2.job, info.job)
        self.assertEqual(info2.task, info.task)
        self.assertEqual(info.destinations, info2.destinations)
        
        
    #def testD(self):
    #    """test publish to dashboard"""
    #    info = DashboardInfo()
    #    info.job = "EvansdeTest1"
    #    info.task = "EvansdeProdAgentTest"
    #    info['User'] = 'evansde'
    #    info['GridUser'] = "DavidEvans"
    #    info['NodeName'] = "twoflower.fnal.gov"
    #    info['Application'] = "CMSSW"
    #    info.addDestination("cms-pamon.cern.ch", 8884)
    #    info.publish(100)

if __name__ == '__main__':
    unittest.main()
    
