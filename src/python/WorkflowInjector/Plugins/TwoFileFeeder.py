#!/usr/bin/env python
"""
_TwoFileFeeder_

Plugin to generate a fixed amount of production jobs from a workflow that
processes a dataset

The input to this plugin is a workflow that contains the following
parameters:

- InputDataset  List of InputDataset
- DBSURL        URL of DBS Instance containing the datasets

Note that split type is one file per job at present
Note the OnlyBlocks parameter can't be used, this could be implemented
somewhere else.

TODO: Provide plugin/hook system to allow for checks on file staging.
Initial thought is that this may be better done as a plugin for the JobQueue
and or ResourceMonitor
to wait for files to stage for a job and then release the job from the
queue.
Would need the job queue to have some way to list the input files needed
for each job.

"""

import logging
import os
import pickle


from WorkflowInjector.PluginInterface import PluginInterface
from WorkflowInjector.Registry import registerPlugin

from ProdCommon.JobFactory.ReRecoJobFactory import ReRecoJobFactory

from ProdCommon.DataMgmt.DBS.DBSReader import DBSReader


class PersistencyFile:
    """
    Store last run used and list of blocks in an pickle
    file

    """
    def __init__(self):
        self.blocks = []
        self.run = 1


class TwoFileFeeder(PluginInterface):
    """
    _TwoFileFeeder_

    Generate a pile of processing style jobs based on the workflow
    and dataset provided

    """
    def handleInput(self, payload):
        logging.info("TwoFileFeeder: Handling %s" % payload)
        self.workflow = None
        self.dbsUrl = None
        self.blocks = []
        self.workflowFile = payload
        self.onlyClosedBlocks = False
        self.loadPayloads(self.workflowFile)

        self.publishNewDataset(self.workflowFile)

        logging.debug("Looking for new blocks:")
        self.makeBlockList(self.onlyClosedBlocks)

        factory = ReRecoJobFactory(self.workflow,
                                   self.workingDir,
                                   self.dbsUrl,
                                   InitialRun = self.persistData.run)

        jobs = factory()

        for job in jobs:
            self.queueJob(job['JobSpecId'], job['JobSpecFile'],
                          job['JobType'],
                          job['WorkflowSpecId'],
                          job['WorkflowPriority'],
                          *job['Sites'])

        self.persistData.run += len(jobs)
        handle = open(self.persistFile, 'w')
        pickle.dump(self.persistData, handle)
        handle.close()
        
        return


    def loadPayloads(self, workflowFile):
        """
        _loadPayloads_


        """
        self.workflow = self.loadWorkflow(workflowFile)
        cacheDir = os.path.join(
            self.workingDir,
            "%s-Cache" % self.workflow.workflowName())
        if not os.path.exists(cacheDir):
            os.makedirs(cacheDir)
        self.persistFile = os.path.join(
            cacheDir, "State.pkl")

        self.persistData = PersistencyFile()

        if os.path.exists(self.persistFile):
            handle = open(self.persistFile, 'r')
            self.persistData = pickle.load(handle)
            handle.close()

        #  //
        # // New workflow?  If so, publish it
        #//
        if self.persistData.run == 1:
            logging.debug("Hey, I haven't seen this workflow before.")
            self.publishWorkflow(workflowFile, self.workflow.workflowName())

        #  //
        # // This pluggin accepts not OnlyBlocks parameter
        #//
        onlyBlocks = self.workflow.parameters.get("OnlyBlocks", None)
        if onlyBlocks != None:
            msg = "OnlyBlocks setting conflicts with TwoFileFeeder\n"
            msg += "Logic. You cannot use OnlyBlocks with this plugin"
            raise RuntimeError, msg

        #  //
        # // Only closed blocks are long to be processed?
        #//
        onlyClosedBlocks = self.workflow.parameters.get("OnlyClosedBlocks", False)
        if onlyClosedBlocks and onlyClosedBlocks.lower() == "true":
            self.onlyClosedBlocks = True

        value = self.workflow.parameters.get("DBSURL", None)
        if value != None:
            self.dbsUrl = value

        if self.dbsUrl == None:
            msg = "Error: No DBSURL available for dataset:\n"
            msg += "Cant get local DBSURL and one not provided with workflow"
            logging.error(msg)
            raise RuntimeError, msg



        return


    def makeBlockList(self, onlyClosedBlocks = False, sites=None):
        """
        _makeBlockList_


        Generate the list of blocks for the workflow.

        1. Get the list of all blocks from the DBS
        2. Compare to list of blocks in persistency file
        3. Set OnlyBlocks parameter to new blocks
        
        """
        reader = DBSReader(self.dbsUrl)
        dbsBlocks = reader.listFileBlocks(self.inputDataset(), onlyClosedBlocks)
        
        if self.persistData.blocks != []:
            remover = lambda x : x not in self.persistData.blocks
            newBlocks = filter(remover, dbsBlocks)
        else:
            newBlocks = dbsBlocks
        
        #  //
        # // Sites restriction is not curently used in this plugin.
        #//
        if sites is not None:    
            blocksAtSites = []
            for block in newBlocks:
                for location in reader.listFileBlockLocation(block):
                    if location in sites:
                        blocksAtSites.append(block)
                        break
            newBlocks = blocksAtSites

        if len(newBlocks) == 0:
            msg = "No New Blocks found for dataset\n"
            raise RuntimeError, msg
        
        blockList = str(newBlocks)
        blockList = blockList.replace("[", "")
        blockList = blockList.replace("]", "")
        blockList = blockList.replace("\'", "")
        blockList = blockList.replace("\"", "")
        self.workflow.parameters['OnlyBlocks'] = blockList
        self.persistData.blocks.extend(newBlocks)
        return


    def inputDataset(self):
        """
        util to get input dataset name

        """
        topNode = self.workflow.payload
        try:
            inputDataset = topNode._InputDatasets[-1]
        except StandardError, ex:
            msg = "Error extracting input dataset from Workflow:\n"
            msg += str(ex)
            logging.error(msg)
            raise RuntimeError, msg 

        return inputDataset.name()



registerPlugin(TwoFileFeeder, TwoFileFeeder.__name__)



