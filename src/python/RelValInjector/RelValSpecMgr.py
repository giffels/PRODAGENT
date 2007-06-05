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

from IMProv.IMProvLoader import loadIMProvFile
from IMProv.IMProvQuery import IMProvQuery

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
        self.setdefault("WorkflowFile", None)
        self.setdefault("JobSpecs", {}) # map jobspecid: jobspecFile
        self.setdefault("BadTest", False)

    def load(self, improvNode):
        """
        _load_

        Read data into this object from improvNode containing a
        RelValTest node

        """
        self['Name'] = improvNode.attrs.get("Name", None)
        for child in improvNode.children:
            if self.has_key(child.name):                
                self[str(child.name)] = child.attrs.get("Value", None)
        
        

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
                msg += "Skipping Test..."
                test['BadTest'] = True
                logging.error(msg)
                continue
            

        #  //
        # // Duplicate each test for every site
        #//
        newTests = []
        for test in self.tests:
            for site in self.sites:
                newTest = RelValTest()
                newTest.update(test)
                newTest['Site'] = site
                newTests.append(newTest)
        self.tests = newTests
        

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
        tests = testQ(improv)

        for test in tests:
            newTest = RelValTest()
            newTest.load(test)
            self.tests.append(newTest)

        for test in self.tests:
            speed = test.get("SpeedCategory", "Slow")
            test['EventsPerJob'] = self.args[speed]
            
            
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
        print "Processing : %s" % testInstance['Name']
        
        workingDir = os.path.join(self.args['ComponentDir'],
                                  self.args['CurrentVersion'],
                                  testInstance['Name'])
        if not os.path.exists(workingDir):
            os.makedirs(workingDir)

        loader = CMSSWAPILoader(self.args['CurrentArch'],
                                self.args['CurrentVersion'],
                                self.args['CurrentCMSPath'])

        cfgWrapper = CMSSWConfig()
        loader.load()
        process = pickle.load(file(testInstance['PickleFile']))
        cfgWrapper.loadConfiguration(process)            
        loader.unload()
        
        for outMod in cfgWrapper.outputModules.values():
            if outMod.get('dataTier', None) == None:
                outMod['dataTier'] = "GEN-SIM-DIGI-RECO"

        cfgWrapper.configMetadata['name'] = testInstance['PickleFile']
        cfgWrapper.configMetadata['version'] = "%s-%s" % (
            self.args['CurrentVersion'],
            self.datestamp)
        annotation = "RelVal test %s. " % testInstance['Name']
        annotation += "Using Version %s. " % self.args['CurrentVersion']
        annotation += "On date %s. " % self.datestamp
        cfgWrapper.configMetadata['annotation'] = annotation
        
        
        maker = WorkflowMaker(
            "%s-%s" % ( self.args['CurrentVersion'], 
                        self.timestamp),
            testInstance['Name'], 'RelVal')
        
        maker.setCMSSWVersion(self.args['CurrentVersion'])
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

        testInstance['WorkflowSpecId'] = maker.workflowName
        testInstance['WorkflowSpecFile'] = specFile
        testInstance['WorkingDir'] = workingDir
        
        return



    def makeJobs(self, testInstance):
        """
        _makeJobs_

        Create Job Specs for the test instance provided

        """
        print "Creating Jobs for test %s at site %s" % (testInstance['Name'],
                                                        testInstance['Site'])
        
        if not self.iterators.has_key(testInstance['WorkflowSpecId']):
            iterator = RequestIterator(testInstance['WorkflowSpecFile'],
                                       testInstance['WorkingDir'])
            self.iterators[testInstance['WorkflowSpecId']] = iterator
            iterator.save(testInstance['WorkingDir'])
        else:
            iterator = self.iterators[testInstance['WorkflowSpecId']]
            iterator.load(testInstance['WorkingDir'])
            
        iterator.sitePref = testInstance['Site']
        iterator.eventsPerJob = testInstance['EventsPerJob']
        
        totalEvents = float(testInstance['TotalEvents'])
        eventsPerJob = float(testInstance['EventsPerJob'])
        numJobs = int(totalEvents/eventsPerJob) +1

        print "Creating total of %s jobs" % numJobs
        
        for i in range(0, numJobs):
            jobSpecFile = str(iterator())
            jobSpecId = iterator.currentJob
            testInstance['JobSpecs'][jobSpecId] = jobSpecFile
            
        iterator.save(testInstance['WorkingDir'])
        
        return

    
        
        
        

if __name__ == '__main__':
    args = {
        'ComponentDir' : '/home/evansde/work/PRODAGENT/src/python/RelValInjector/detritus/',  
        'CurrentArch' : "slc4_ia32_gcc345",
        'CurrentCMSPath' : "/uscms/home/cms_admin/SL4",
        'CurrentVersion' : "CMSSW_1_5_0_pre4",
        'Fast' : 250,
        'Medium' : 100,
        'Slow' : 50  ,
        }
    specFile = "/home/evansde/work/PRODAGENT/cmssw/CMSSW_1_5_0_pre4/PyRelValSpec.xml"
    specMgr = RelValSpecMgr(specFile, ['CERN', 'FNAL'], **args)
    tests = specMgr()

    
    
