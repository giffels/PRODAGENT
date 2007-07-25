#!/usr/bin/env python
"""
_RequestIterator_

Maintain a Workflow specification, and when prompted,
generate a new concrete job from that workflow

The Workflow is stored as an MCPayloads.WorkflowSpec,
The jobs are JobSpec instances created from the WorkflowSpec

"""

import os
import logging

from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec
from ProdCommon.MCPayloads.LFNAlgorithm import DefaultLFNMaker
from ProdCommon.CMSConfigTools.ConfigAPI.CfgGenerator import CfgGenerator
from ProdCommon.CMSConfigTools.SeedService import randomSeed

from PileupTools.PileupDataset import PileupDataset, createPileupDatasets, getPileupSites

from IMProv.IMProvDoc import IMProvDoc
from IMProv.IMProvNode import IMProvNode
from IMProv.IMProvQuery import IMProvQuery
from IMProv.IMProvLoader import loadIMProvFile



class GeneratorMaker(dict):
    """
    _GeneratorMaker_

    Operate on a workflow spec and create a map of node name
    to CfgGenerator instance

    """
    def __init__(self):
        dict.__init__(self)


    def __call__(self, payloadNode):
        if payloadNode.type != "CMSSW":
            return
        
        if payloadNode.cfgInterface != None:
            generator = CfgGenerator(payloadNode.cfgInterface, False,
                                     payloadNode.applicationControls)
            self[payloadNode.name] = generator
            return
            
        if payloadNode.configuration in ("", None):
            #  //
            # // Isnt a config file
            #//
            return
        try:
            generator = CfgGenerator(payloadNode.configuration, True,
                                         payloadNode.applicationControls)
            self[payloadNode.name] = generator
        except StandardError, ex:
            #  //
            # // Cant read config file => not a config file
            #//
            return
    
        


class RequestIterator:
    """
    _RequestIterator_

    Working from a Generic Workflow template, generate
    concrete jobs from it, keeping in-memory history

    """
    def __init__(self, workflowSpecFile, workingDir):
        self.workflow = workflowSpecFile
        self.workingDir = workingDir
        self.count = 0
        self.runIncrement = 1
        self.currentJob = None
        self.sitePref = None
        self.pileupDatasets = {}
        self.ownedJobSpecs = {}
        
        #  //
        # // Initially hard coded, should be extracted from Component Config
        #//
        self.eventsPerJob = 10 
        
        self.workflowSpec = WorkflowSpec()
        self.workflowSpec.load(workflowSpecFile)

        if self.workflowSpec.parameters.get("RunIncrement", None) != None:
            self.runIncrement = int(
                self.workflowSpec.parameters['RunIncrement']
                )

    
        self.generators = GeneratorMaker()
        self.workflowSpec.payload.operate(self.generators)
        
        
        
        #  //
        # // Cache Area for JobSpecs
        #//
        self.specCache = os.path.join(
            self.workingDir,
            "%s-Cache" %self.workflowSpec.workflowName())
        if not os.path.exists(self.specCache):
            os.makedirs(self.specCache)
        


    def loadPileupDatasets(self):
        """
        _loadPileupDatasets_

        Are we dealing with pileup? If so pull in the file list
        
        """
        puDatasets = self.workflowSpec.pileupDatasets()
        if len(puDatasets) > 0:
            logging.info("Found %s Pileup Datasets for Workflow: %s" % (
                len(puDatasets), self.workflowSpec.workflowName(),
                ))
            self.pileupDatasets = createPileupDatasets(self.workflowSpec)
        return

    def loadPileupSites(self):
        """
        _loadPileupSites_
                                                                                                              
        Are we dealing with pileup? If so pull in the site list
                                                                                                              
        """
        sites = []
        puDatasets = self.workflowSpec.pileupDatasets()
        if len(puDatasets) > 0:
            logging.info("Found %s Pileup Datasets for Workflow: %s" % (
                len(puDatasets), self.workflowSpec.workflowName(),
                ))
            sites = getPileupSites(self.workflowSpec)
        return sites
                                                                                                              


            
    def __call__(self):
        """
        _operator()_

        When called generate a new concrete job payload from the
        generic workflow and return it.

        """
        newJobSpec = self.createJobSpec()
        self.count += self.runIncrement
        return newJobSpec


    def createJobSpec(self):
        """
        _createJobSpec_

        Load the WorkflowSpec object and generate a JobSpec from it

        """
        
        jobSpec = self.workflowSpec.createJobSpec()
        jobName = "%s-%s" % (
            self.workflowSpec.workflowName(),
            self.count,
            )
        self.currentJob = jobName
        jobSpec.setJobName(jobName)
        jobSpec.setJobType("Processing")
        jobSpec.parameters['RunNumber'] = self.count

        
        jobSpec.payload.operate(DefaultLFNMaker(jobSpec))
        jobSpec.payload.operate(self.generateJobConfig)
        jobSpec.payload.operate(self.generateCmsGenConfig)
        specCacheDir =  os.path.join(
            self.specCache, str(self.count // 1000).zfill(4))
        if not os.path.exists(specCacheDir):
            os.makedirs(specCacheDir)
        jobSpecFile = os.path.join(specCacheDir,
                                   "%s-JobSpec.xml" % jobName)
        self.ownedJobSpecs[jobName] = jobSpecFile

        
        #  //
        # // Add site pref if set
        #//
        if self.sitePref != None:
#
#AF:  Allow site pref to be a comma separated list of sites, each one
#     added in the Whitelist:
#            jobSpec.addWhitelistSite(self.sitePref)
          for siteWhite in self.sitePref.split(","): 
            jobSpec.addWhitelistSite(siteWhite)
            
        jobSpec.save(jobSpecFile)        
        return jobSpecFile
        
        
    def generateJobConfig(self, jobSpecNode):
        """
        _generateJobConfig_
        
        Operator to act on a JobSpecNode tree to convert the template
        config file into a JobSpecific Config File
                
        """
        if jobSpecNode.name not in self.generators.keys():
            return
        generator = self.generators[jobSpecNode.name]

        useOutputMaxEv = False
        if jobSpecNode.cfgInterface != None:
            outMaxEv = jobSpecNode.cfgInterface.maxEvents['output']
            if outMaxEv != None:
                useOutputMaxEv = True

        if useOutputMaxEv:
            jobCfg = generator(self.currentJob,
                               maxEventsWritten = self.eventsPerJob,
                               firstRun = self.count)
        else:
            jobCfg = generator(self.currentJob,
                               maxEvents = self.eventsPerJob,
                               firstRun = self.count)
        
        #  //
        # // Is there pileup for this node?
        #//
        if self.pileupDatasets.has_key(jobSpecNode.name):
            puDataset = self.pileupDatasets[jobSpecNode.name]
            logging.debug("Node: %s has a pileup dataset: %s" % (
                jobSpecNode.name,  puDataset.dataset,
                ))
            fileList = puDataset.getPileupFiles()
            jobCfg.pileupFiles = fileList

            
            
        
        jobSpecNode.cfgInterface = jobCfg
        return


    def generateCmsGenConfig(self, jobSpecNode):
        """
        _generateCmsGenConfig_

        Process CmsGen type nodes to insert maxEvents and run numbers
        for cmsGen jobs

        """
        if jobSpecNode.type != "CmsGen":
            return

        jobSpecNode.applicationControls['firstRun'] = self.count
        jobSpecNode.applicationControls['maxEvents'] = self.eventsPerJob
        jobSpecNode.applicationControls['randomSeed'] = randomSeed()
        jobSpecNode.applicationControls['fileName'] = "%s-%s.root" % (
            self.currentJob, jobSpecNode.name)
        jobSpecNode.applicationControls['logicalFileName'] = "%s-%s.root" % (
            self.currentJob, jobSpecNode.name)
        return
        

    def removeSpec(self, jobSpecId):
        """
        _removeSpec_

        Remove a Spec file when it has been successfully injected

        """
        if jobSpecId not in self.ownedJobSpecs.keys():
            return

        logging.info("Removing JobSpec For: %s" % jobSpecId)
        filename = self.ownedJobSpecs[jobSpecId]
        if os.path.exists(filename):
            os.remove(filename)
            del self.ownedJobSpecs[jobSpecId]
        return

        


    def save(self, directory):
        """
        _save_

        Persist this objects state into an XML file and save
        it in the directory provided

        """
        doc = IMProvDoc("RequestIterator")
        node = IMProvNode(self.workflowSpec.workflowName())
        doc.addNode(node)

        node.addNode(IMProvNode("Run", None, Value = str(self.count)))
        node.addNode(
            IMProvNode("EventsPerJob", None, Value = str(self.eventsPerJob))
            )
        node.addNode(IMProvNode("SitePref", None, Value = str(self.sitePref)))

        pu = IMProvNode("Pileup")
        node.addNode(pu)
        for key, value in self.pileupDatasets.items():
            puNode = value.save()
            puNode.attrs['PayloadNode'] = key
            pu.addNode(puNode)

        specs = IMProvNode("JobSpecs")
        node.addNode(specs)
        for key, val in self.ownedJobSpecs.items():
            specs.addNode(IMProvNode("JobSpec", val, ID = key))
            
        fname = os.path.join(
            directory,
            "%s-Persist.xml" % self.workflowSpec.workflowName()
            )
        handle = open(fname, 'w')
        handle.write(doc.makeDOMDocument().toprettyxml())
        handle.close()
        return


    def load(self, directory):
        """
        _load_

        Load this instance given the workflow and directory containing
        the persistency file

        """
        fname = os.path.join(
            directory,
            "%s-Persist.xml" % self.workflowSpec.workflowName()
            )

        try:
            node = loadIMProvFile(fname)
        except Exception, ex:
            msg = "ERROR: Corrupted Persistency File:\n"
            msg += "  => %s\n" % fname
            msg += "Cannot be read:\n  => %s\n" % str(ex)
            logging.error(msg)
            return
        

        qbase = "/RequestIterator/%s" % self.workflowSpec.workflowName()

        runQ = IMProvQuery("%s/Run[attribute(\"Value\")]" % qbase)
        eventQ = IMProvQuery("%s/EventsPerJob[attribute(\"Value\")]" % qbase)
        siteQ = IMProvQuery("%s/SitePref[attribute(\"Value\")]" % qbase)

        runVal = int(runQ(node)[-1])
        eventVal = int(eventQ(node)[-1])
        siteVal = str(siteQ(node)[-1])
        if siteVal.lower() == "none":
            siteVal = None

        self.count = runVal
        self.eventsPerJob = eventVal
        self.sitePref = siteVal
        
        puQ = IMProvQuery("%s/Pileup/*" % qbase)
        puNodes = puQ(node)
        for puNode in puNodes:
            payloadNode = str(puNode.attrs.get("PayloadNode"))
            puDataset = PileupDataset("dummy", 1)
            puDataset.load(puNode)
            self.pileupDatasets[payloadNode] = puDataset

        specQ = IMProvQuery("%s/JobSpecs/*" % qbase)
        specNodes = specQ(node)
        for specNode in specNodes:
            specId = str(specNode.attrs['ID'])
            specFile = str(specNode.chardata).strip()
            self.ownedJobSpecs[specId] = specFile

        return
    
            
        
        
        
