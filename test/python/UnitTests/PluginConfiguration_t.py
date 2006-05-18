#!/usr/bin/env python
"""
unittest for ProdAgentCore.PluginConfiguration module

"""


import unittest
import os

from ProdAgentCore.PluginConfiguration import PluginConfiguration


class PluginConfigurationTest(unittest.TestCase):
    """
    TestCase for PluginConfiguration class
    """
    def setUp(self):
        """set up test"""
        self.testFile = os.path.join(os.getcwd(), "PluginConfiguration_t.xml")

    def tearDown(self):
        """clean up"""
        if os.path.exists(self.testFile):
            os.remove(self.testFile)
            

    def testA(self):
        """instantiation"""
        try:
            test = PluginConfiguration()

        except StandardError,ex:
            msg = "Failed to instantiate ProdAgentCore.PluginConfiguration:\n"
            msg += str(ex)
            self.fail(msg)
            
    def testB(self):
        """adding blocks"""
        cfg = PluginConfiguration()
        self.assertEqual(cfg.keys(), [])
        block1 = cfg.newBlock("Block1")
        self.failUnless("Block1" in cfg.keys(), "Failed to add new Block1")
        block2 = cfg.newBlock("Block2")
        self.failUnless("Block2" in cfg.keys(), "Failed to add new Block2")
        block3 = cfg.newBlock("Block3")
        self.failUnless("Block3" in cfg.keys(), "Failed to add new Block3")

        block1['Param1'] = 'Value1'
        block1['Param2'] = 'Value2'
        block1['Param3'] = 'Value3'
        
        self.assertEqual(cfg['Block1']['Param1'], "Value1")
        self.assertEqual(cfg['Block1']['Param2'], "Value2")
        self.assertEqual(cfg['Block1']['Param3'], "Value3")
        

    def testC(self):
        """test save/load"""
        
        cfg = PluginConfiguration()
        block1 = cfg.newBlock("Block1")
        block1['Param1'] = 'Value1'
        block1['Param2'] = 'Value2'
        block1['Param3'] = 'Value3'
        block1.comment = "This is Block 1"
        block2 = cfg.newBlock("Block2")
        block2['Param4'] = 'Value4'
        block2['Param5'] = 'Value5'
        block2['Param6'] = 'Value6'
        block2.comment = "This is Block 2"

        try:
            improvNode = cfg.save()
        except StandardError, ex:
            msg = "Error calling PluginConfiguration.save:\n"
            msg += str(ex)
            self.fail(msg)

        cfg2 = PluginConfiguration()

        try:
            cfg2.load(improvNode)
        except StandardError, ex:
            msg = "Error calling PluginConfiguration.load:\n"
            msg += str(ex)
            self.fail(msg)

        self.assertEqual(cfg, cfg2)

    def testD(self):
        """test save/load to file"""
        cfg = PluginConfiguration()
        block1 = cfg.newBlock("Block1")
        block1['Param1'] = 'Value1'
        block1['Param2'] = 'Value2'
        block1['Param3'] = 'Value3'
        block1.comment = "This is Block 1"
        block2 = cfg.newBlock("Block2")
        block2['Param4'] = 'Value4'
        block2['Param5'] = 'Value5'
        block2['Param6'] = 'Value6'
        block2.comment = "This is Block 2"

        try:
            cfg.writeToFile(self.testFile)
        except StandardError, ex:
            msg = "Error calling PluginConfiguration.writeToFile:\n"
            msg += str(ex)
            self.fail(msg)

        cfg2 = PluginConfiguration()

        try:
            cfg2.loadFromFile(self.testFile)
        except StandardError, ex:
            msg = "Error calling PluginConfiguration.loadFromFile:\n"
            msg += str(ex)
            self.fail(msg)

        
        self.assertEqual(cfg, cfg2)


if __name__ == '__main__':
    unittest.main()

    
