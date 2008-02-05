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
from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec

from ProdCommon.JobFactory.RequestJobFactory import RequestJobFactory
from ProdCommon.JobFactory.DatasetJobFactory import DatasetJobFactory 

#from RequestInjector.RequestIterator import RequestIterator
from ProdAgentCore.Configuration import loadProdAgentConfiguration

def getGlobalDBSURL():
    try:
        config = loadProdAgentConfiguration()
    except StandardError, ex:
        msg = "Error reading configuration:\n"
        msg += str(ex)
        logging.error(msg)
        raise RuntimeError, msg
    
    try:
        dbsConfig = config.getConfig("GlobalDBSDLS")
    except StandardError, ex:
        msg = "Error reading configuration for GlobalDBSDLS:\n"
        msg += str(ex)
        logging.error(msg)
        raise RuntimeError, msg

    return dbsConfig.get("DBSURL", None)

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
        self.setdefault("PileupDataset", None)
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
        node = IMProvNode("RelValTest")
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
        self.dbsUrl = getGlobalDBSURL()
        self.jobCounts = {}
        
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
            msg += "Traceback: %s\n" % traceback.format_exc()
            logging.error(msg)
            return result 

        
        
        for test in self.tests:
            try:
                self.makeWorkflow(test)
            except Exception, ex:
                msg = "Error Creating workflow for test: %s\n" % test['PickleFile']
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
                msg = "Error Creating jobs for test: %s\n" % test['PickleFile']
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
            
        loader = CMSSWAPILoader(testInstance['CMSSWArchitecture'],
                                testInstance['CMSSWVersion'],
                                testInstance['CMSPath'])
        loader.load()
        cfgWrapper = CMSSWConfig()
        process = pickle.load(file(testInstance['PickleFile']))
        cfgInt = cfgWrapper.loadConfiguration(process)
        cfgInt.validateForProduction()
        cfgAsString = process.dumpConfig()
        #  //
        # // Get release validation PSet from process
        #//
        relValPSet = getattr(process, "ReleaseValidation", None)
        if relValPSet == None:
            msg = "Unable to extract ReleaseValidation PSet from pickled cfg for \n"
            msg += "%s\n" % testInstance['PickleFile']
            logging.error(msg)
            return

        testName = getattr(relValPSet, "primaryDatasetName", None)
        testInstance['Name'] = testName.value()
        if testName == None:
            msg = "No primaryDatasetName parameter in ReleaseValidation PSet\n"
            msg += "%s\n" % testInstance['PickleFile']
            logging.error(msg)
            return

        totalEvents = getattr(relValPSet, "totalNumberOfEvents", None)
        if totalEvents == None:
            msg = "No totalNumberOfEvents  parameter in ReleaseValidation PSet\n"
            msg += "%s\n" % testInstance['PickleFile']
            logging.error(msg)
            return
        testInstance['TotalEvents'] = totalEvents.value()

        eventsPerJob = getattr(relValPSet, "eventsPerJob", None)
        speedCat = getattr(relValPSet, "speedCategory", None)

        if (eventsPerJob == None) and (speedCat == None):
            msg = "ReleaseValidation PSet must contain one of either eventsPerJob or speedCategory\n"
            msg += "%s\n" % testInstance['PickleFile']
            logging.error(msg)
            return
        
        if eventsPerJob != None:
            testInstance['EventsPerJob'] = eventsPerJob.value()
        else:
            testInstance['SpeedCategory'] = speedCat.value()
            if not self.args.has_key(testInstance['SpeedCategory']):
                msg = "Unknown Speed Category: %s\n" % testInstance['SpeedCategory']
                msg += "In file: %s\n" % testInstance['PickleFile']
                logging.error(msg)
                return

            testInstance['EventsPerJob'] = self.args[testInstance['SpeedCategory']]


        inputDataset = getattr(relValPSet, "inputDatasetPath", None)
        pileupDataset = getattr(relValPSet, "pileupDatasetPath", None)

        if pileupDataset != None:
            testInstance['PileupDataset'] = pileupDataset.value()

        if inputDataset != None:
            testInstance['InputDataset'] = inputDataset.value()
        
        msg = "Processing : %s\n" % testInstance['Name']
        msg += "From Pickle: %s\n" % testInstance['PickleFile']
        msg += "TotalEvents: %s\n" % testInstance['TotalEvents']
        msg += "EventsPerJob: %s\n" % testInstance['EventsPerJob']
        msg += "SpeedCategory: %s\n" % testInstance['SpeedCategory']
        logging.info(msg)
        
        if self.workflows.has_key(testInstance['Name']):
            testInstance['WorkflowSpecId'] = self.workflows[testInstance['Name']]
            testInstance['WorkflowSpecFile'] = self.workflowFiles[testInstance['Name']]
            testInstance['WorkingDir'] = self.workingDirs[testInstance['Name']]
            
            loader.unload()
            return

        self.jobCounts[testInstance['Name']] = 1
        workingDir = os.path.join(self.args['ComponentDir'],
                                  testInstance['CMSSWVersion'],
                                  testInstance['Name'])
        if not os.path.exists(workingDir):
            os.makedirs(workingDir)


        loader.unload()
        
        maker = WorkflowMaker(str(self.timestamp),
                              testInstance['Name'],
                              'RelVal')
        
        maker.setCMSSWVersion(testInstance['CMSSWVersion'])
        maker.setPhysicsGroup("RelVal")
        maker.setConfiguration(cfgWrapper, Type = "instance")
        maker.setOriginalCfg(cfgAsString)
        psetHash = "NO_PSET_HASH"
        if cfgWrapper.configMetadata.has_key('PSetHash'):
            psetHash =  cfgWrapper.configMetadata['PSetHash']
        maker.setPSetHash(psetHash)
        maker.changeCategory("relval")
        if testInstance['SelectionEfficiency']  != None:
            selEff = float(testInstance['SelectionEfficiency'] )
            maker.addSelectionEfficiency(selEff)

        if testInstance['PileupDataset'] != None:
            maker.addPileupDataset(testInstance['PileupDataset'], 100)

        if testInstance['InputDataset'] != None:
            maker.addInputDataset(testInstance['InputDataset'])
            maker.inputDataset["SplitType"] = "events"
            maker.inputDataset["SplitSize"] = testInstance['EventsPerJob']              
        spec = maker.makeWorkflow()
        spec.parameters['OnlySites'] = testInstance['Site']
        spec.parameters['DBSURL'] = self.dbsUrl
        specFile = "/%s/%s-Workflow.xml" % (workingDir, maker.workflowName) 
        spec.save(specFile)
        
        self.workflows[testInstance['Name']] = str(maker.workflowName)
        self.workflowFiles[testInstance['Name']] = specFile
        self.workingDirs[testInstance['Name']] = workingDir
        
        testInstance['WorkflowSpecId'] = str(maker.workflowName)
        testInstance['WorkflowSpecFile'] = specFile
        testInstance['WorkingDir'] = workingDir
        msg = "Workflow created for test: %s" % testInstance['Name']
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
        specInstance = WorkflowSpec()
        specInstance.load(testInstance['WorkflowSpecFile'])
        
        if testInstance['InputDataset'] == None:
            initialRun = self.jobCounts.get(testInstance['Name'], 1)
            factory = RequestJobFactory(
                specInstance,
                testInstance['WorkingDir'],
                testInstance['TotalEvents'],
                InitialRun = initialRun,
                EventsPerJob = testInstance['EventsPerJob'],
                Sites = [testInstance['Site']])

            jobsList = factory()
            self.jobCounts[testInstance['Name']] += len(jobsList)
        else:
            
            factory = DatasetJobFactory(
                specInstance,
                testInstance['WorkingDir'],
                specInstance.parameters['DBSURL'],
                )
            
            jobsList = factory()
            self.jobCounts[testInstance['Name']] += len(jobsList)
            
        
        msg = "Created %s jobs:\n" % len(jobsList)
    
        for job in jobsList:
            jobSpecFile = job['JobSpecFile']
            jobSpecId = job['JobSpecId']
            msg += "  %s\n" % jobSpecId
            testInstance['JobSpecs'][jobSpecId] = jobSpecFile
            
        
        
        logging.info(msg)
            

        
        return

    
if __name__ == '__main__':

    logging.getLogger().setLevel(logging.DEBUG)

    args = {
        'ComponentDir' : '/home/evansde/work/PRODAGENT/src/python/RelValInjector/detritus',
        
        "Fast" : 100,
        "Slow" : 50,
        "Medium" : 75,
        "VerySlow" : 25,
        
        }
    sites = ['srm.cern.ch', 'cmssrm.fnal.gov']
    specFile = "/home/evansde/work/CMSSW/CMSSW_1_7_0_pre5/src/Configuration/ReleaseValidation/data/relval_workflows.xml"
    

    mgr = RelValSpecMgr(specFile, sites, **args)
    mgr()
    
