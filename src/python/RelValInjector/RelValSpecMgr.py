#!/usr/bin/env python
"""
_RelValSpecMgr_

Object to take a RelVal Spec file, load it up and start creating
workflows & JobSpecs for all the sites

"""

import logging
import os
import pickle
import time
import traceback

from IMProv.IMProvLoader import loadIMProvFile
from IMProv.IMProvQuery import IMProvQuery
from IMProv.IMProvNode import IMProvNode

from ProdCommon.MCPayloads.WorkflowMaker import WorkflowMaker
from ProdCommon.CMSConfigTools.ConfigAPI.CMSSWAPILoader import CMSSWAPILoader
from ProdCommon.CMSConfigTools.ConfigAPI.CMSSWConfig import CMSSWConfig

from RequestInjector.RequestIterator import RequestIterator


class RelValTest(dict):
    """
    _RelValTest_

    Dictionary of parameters used for creating a set of RelVal Test
    jobs to get submitted to a single site

    """
    def __init__(self):
        dict.__init__(self)
        self.setdefault("Name", None)
        self.setdefault("SpeedCategory", None)
        self.setdefault("TotalEvents", None)
        self.setdefault("EventsPerJob", None)
        self.setdefault("Site", None)
        self.setdefault("SelectionEfficiency", None)
        self.setdefault("PickleFile", None)
        self.setdefault("InputDataset", None)
        self.setdefault("WorkflowSpecId", None)
        self.setdefault("CMSSWVersion", None)
        self.setdefault("CMSSWArchitecture", None)
        self.setdefault("CMSPath", None)
        self.setdefault("WorkflowFile", None)
        self.setdefault("JobSpecs", {}) # map jobspecid: jobspecFile
        self.setdefault("BadTest", False)

    def save(self):
        """
        _save_

        convert to improv node

        """
        node = IMProvNode("RelValTest", None, Name = self['Name'])
        for key, val in self.items():
            if key in ["Name", "JobSpecs", "BadTest"]:
                continue

            if val != None:
                node.addNode(IMProvNode(key, None, Value = str(val)))
        return node
        

    def load(self, improvNode):
        """
        _load_

        Read data into this object from improvNode containing a
        RelValTest node

        """
        self['Name'] = improvNode.attrs.get("Name", None)
        for child in improvNode.children:
            if self.has_key(child.name):
                value = child.attrs.get("Value", None)
                if value != None:
                    self[str(child.name)] = str(value)
                
                

class RelValSpecMgr:
    """
    _RelValSpecMgr_

    Object to generate workflows from a RelValSpec and list
    of pickled files

    """
    def __init__(self, relValSpec, siteList,  **settings):
        self.args = settings
        self.sites = siteList
        self.relvalSpecFile = relValSpec
        self.timestamp = int(time.time())
        self.datestamp = time.asctime(time.localtime(time.time()))
        self.datestamp = self.datestamp.replace("  ", " ")
        self.datestamp = self.datestamp.replace(" ", "_")
        self.datestamp = self.datestamp.replace(":", "-")
        self.tests = []
        self.iterators = {}
        self.workflows = {}
        self.workflowFiles = {}
        self.workingDirs = {}
        
        
    def __call__(self):
        """
        _operator()_
x
        Call to invoke creation of tests and return a set of
        test dictionaries containing workflow and JobSpec files
        to be published into the ProdAgent

        """
        result = [] # list of RelValTest instances
        try:
            self.loadRelValSpec()
        except Exception, ex:
            msg = "Unable to load RelValSpec File:\n"
            msg += " %s\n" % self.relvalSpecFile
            msg += "Error:\n %s\n" % str(ex)
            logging.error(msg)
            return result 

        
        
        for test in self.tests:
            try:
                self.makeWorkflow(test)
            except Exception, ex:
                msg = "Error Creating workflow for test: %s\n" % test['Name']
                msg += "Skipping Test...\n"
                test['BadTest'] = True
                logging.error(msg)
                dbg = traceback.format_exc()
                logging.debug("Traceback:\n%s\n" % dbg)
                continue
            
            
      

        for test in self.tests:
            try:
                self.makeJobs(test)
            except Exception, ex:
                msg = "Error Creating jobs for test: %s\n" % test['Name']
                msg += "Skipping Test..."
                test['BadTest'] = True
                logging.error(msg)
                continue
            
        self.tests = [ x for x in self.tests if x['BadTest'] == False ]
        return self.tests
        
        
    def loadRelValSpec(self):
        """
        _loadRelValSpec_

        Load the RelVal Spec file.
        Populate the list of tests
        """
        improv = loadIMProvFile(self.relvalSpecFile)
        testQ = IMProvQuery("/RelValSpec/RelValTest")
        testNodes = testQ(improv)
        
        for test in testNodes:
             for site in self.sites:
                 newTest = RelValTest()
                 newTest.load(test)
                 newTest['Site'] = site
                 speed = newTest.get('SpeedCategory', "Slow")
                 eventsPerJob = self.args[speed]
                 newTest['EventsPerJob'] = eventsPerJob
                 self.tests.append(newTest)
                 

                 
            
        logging.info("Loaded %s tests from file:\n %s\n" % (
            len(self.tests),
            self.relvalSpecFile))

        
        
        return
    
        
    
    def makeWorkflow(self, testInstance):
        """
        _processTest_

        Process a test, create a WorkflowSpec for it, generate job specs
        and add the, to the test instance
        
        """
        logging.info("Processing : %s" % testInstance['Name'])
        testName = testInstance['Name']
        if self.workflows.has_key(testName):
            testInstance['WorkflowSpecId'] = self.workflows[testName]
            testInstance['WorkflowSpecFile'] = self.workflowFiles[testName]
            testInstance['WorkingDir'] = self.workingDirs[testName]
            return
        
        workingDir = os.path.join(self.args['ComponentDir'],
                                  testInstance['CMSSWVersion'],
                                  testInstance['Name'])
        if not os.path.exists(workingDir):
            os.makedirs(workingDir)

            
        loader = CMSSWAPILoader(testInstance['CMSSWArchitecture'],
                                testInstance['CMSSWVersion'],
                                testInstance['CMSPath'])
        loader.load()
        cfgWrapper = CMSSWConfig()
        process = pickle.load(file(testInstance['PickleFile']))
        cfgInt = cfgWrapper.loadConfiguration(process)
        cfgInt.validateForProduction()
        loader.unload()
        
        maker = WorkflowMaker(str(self.timestamp),
                              testInstance['Name'],
                              'RelVal')
        
        maker.setCMSSWVersion(testInstance['CMSSWVersion'])
        maker.setPhysicsGroup("dataOps")
        maker.setConfiguration(cfgWrapper, Type = "instance")

        psetHash = "NO_PSET_HASH"
        if cfgWrapper.configMetadata.has_key('PSetHash'):
            psetHash =  cfgWrapper.configMetadata['PSetHash']
        maker.setPSetHash(psetHash)
        maker.changeCategory("mc")
        if testInstance['SelectionEfficiency']  != None:
            selEff = float(testInstance['SelectionEfficiency'] )
            maker.addSelectionEfficiency(selEff)
            
        spec = maker.makeWorkflow()
        specFile = "/%s/%s-Workflow.xml" % (workingDir, maker.workflowName) 
        spec.save(specFile)

        self.workflows[testName] = str(maker.workflowName)
        self.workflowFiles[testName] = specFile
        self.workingDirs[testName] = workingDir
        
        testInstance['WorkflowSpecId'] = str(maker.workflowName)
        testInstance['WorkflowSpecFile'] = specFile
        testInstance['WorkingDir'] = workingDir

        msg = "Workflow created for test: %s" % testName
        logging.info(msg)

        
        return



    def makeJobs(self, testInstance):
        """
        _makeJobs_

        Create Job Specs for the test instance provided

        """
        logging.info("Creating Jobs for test %s at site %s" % (
            testInstance['Name'],
            testInstance['Site'])
                     )
        testName = testInstance['WorkflowSpecId']
        if not self.iterators.has_key(testName):
            iterator = RequestIterator(testInstance['WorkflowSpecFile'],
                                       testInstance['WorkingDir'])
            self.iterators[testName] = iterator
            iterator.count = 1
            logging.info("Created RequestIterator for %s" % (
                testInstance['WorkflowSpecId'],))
            iterator.save(testInstance['WorkingDir'])
        else:
            iterator = self.iterators[testName]
            iterator.load(testInstance['WorkingDir'])
            logging.info("Retrieved RequestIterator for %s" % (
                testInstance['WorkflowSpecId'],))
            
        iterator.sitePref = testInstance['Site']
        iterator.eventsPerJob = testInstance['EventsPerJob']
        
        totalEvents = float(testInstance['TotalEvents'])
        eventsPerJob = float(testInstance['EventsPerJob'])
        numJobs = int(totalEvents/eventsPerJob) +1
        
        msg = "Created %s jobs:\n" % numJobs
        
        for i in range(0, numJobs):
            jobSpecFile = str(iterator())
            jobSpecId = iterator.currentJob
            msg += "  %s\n" % jobSpecId
            testInstance['JobSpecs'][jobSpecId] = jobSpecFile

        
        
        logging.info(msg)
            
        iterator.save(testInstance['WorkingDir'])
        
        return

    
if __name__ == '__main__':

    logging.getLogger().setLevel(logging.DEBUG)

    args = {
        'ComponentDir' : '/home/evansde/work/PRODAGENT/src/python/RelValInjector/detritus',

        "Fast" : 100,
        "Slow" : 50,
        "Medium" : 75,
        
        'CurrentArch' : "slc4_ia32_gcc345",
        "CurrentCMSPath" :"/uscms/home/cms_admin/SL4",
        "CurrentVersion" : "CMSSW_1_5_0_pre4",
        
        
        }
    sites = ['CERN', 'FNAL']
    specFile = "/home/evansde/work/PRODAGENT/src/python/RelValInjector/Oli.xml"

    mgr = RelValSpecMgr(specFile, sites, **args)
    mgr()

