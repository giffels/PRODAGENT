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
from ProdCommon.MCPayloads.LFNAlgorithm import createUnmergedLFNs
from ProdCommon.CMSConfigTools.CfgGenerator import CfgGenerator
from PileupTools.PileupDataset import PileupDataset, createPileupDatasets, getPileupSites

from IMProv.IMProvDoc import IMProvDoc
from IMProv.IMProvNode import IMProvNode
from IMProv.IMProvQuery import IMProvQuery
from IMProv.IMProvLoader import loadIMProvFile


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
        self.currentJob = None
        self.sitePref = None
        self.pileupDatasets = {}

        #  //
        # // Initially hard coded, should be extracted from Component Config
        #//
        self.eventsPerJob = 10 
        
        self.workflowSpec = WorkflowSpec()
        self.workflowSpec.load(workflowSpecFile)
        



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
        self.count += 1
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

    
        jobSpec.payload.operate(self.generateJobConfig)            
        jobSpecFile = os.path.join(self.workingDir,
                                   "%s-JobSpec.xml" % jobName)

        #  //
        # // generate LFNs for output modules
        #//
        createUnmergedLFNs(jobSpec)

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
        
        return "file://%s" % jobSpecFile
        
        
    def generateJobConfig(self, jobSpecNode):
        """
        _generateJobConfig_
        
        Operator to act on a JobSpecNode tree to convert the template
        config file into a JobSpecific Config File
                
        """
        if jobSpecNode.configuration in ("", None):
            #  //
            # // Isnt a config file
            #//
            return
        try:
            generator = CfgGenerator(jobSpecNode.configuration, True)
        except StandardError, ex:
            #  //
            # // Cant read config file => not a config file
            #//
            return
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
            mixingModules = jobCfg.mixingModules()
            fileList = puDataset.getPileupFiles()
            quotedFiles = [ "\"%s\"" % i for i in fileList ]
            for mixMod in mixingModules:
                inpPSet = mixMod['input'][2]
                inpPSet['fileNames'] = ('vstring', 'untracked', quotedFiles)
            
            
        
        jobSpecNode.configuration = jobCfg.cmsConfig.asPythonString()
        jobSpecNode.loadConfiguration()
        
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
        
        node = loadIMProvFile(fname)

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

        return
    
            
        
        
        
