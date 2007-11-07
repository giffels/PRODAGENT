#!/usr/bin/env python
"""
_CleanUpSchedulerComponent_

ProdAgent Component to schedule cleanup jobs to remove
unmerged files

"""

import logging
import os
import ProdAgentCore.LoggingUtils as LoggingUtils
from MessageService.MessageService import MessageService
from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdCommon.Database import Session


from MergeSensor.MergeCrossCheck import MergeSensorCrossCheck
from MergeSensor.MergeCrossCheck import listAllMergeDatasets 
import ProdCommon.MCPayloads.CleanUpTools as CleanUpTools
from ProdCommon.DataMgmt.DBS.DBSReader import DBSReader








class CleanUpTasks(dict):
    """
    _CleanUpTasks_

    Helper class to organise cleanup tasks by site.
    Essentially a map of block/Site: list of LFNs 

    """
    def __init__(self):
        dict.__init__(self)

    def addFiles(self, blockOrSite, *lfns):
        """
        _addFiles_

        Add a list of files to be cleaned up at a site

        """
        if not self.has_key(blockOrSite):
            self[blockOrSite] = []
        self[blockOsSite].extend(lfns)
        return
    
    
    


class CleanUpSchedulerComponent:
    """
    _CleanUpSchedulerComponent_


    """
    def __init__(self, **args):
        self.args = {}
        self.args['Logfile'] = None
        
        self.args.update(args)
        if self.args['Logfile'] == None:
            self.args['Logfile'] = os.path.join(self.args['ComponentDir'],
                                                "ComponentLog")
        LoggingUtils.installLogHandler(self)
        msg = "CleanUpScheduler Started:\n"
        logging.info(msg)

        

    def __call__(self, event, payload):
        """
        _operator()_

        Define response to an Event and payload

        """
        msg = "Recieved Event: %s" % event
        msg += "Payload: %s" % payload
        logging.debug(msg)

        if event == "CleanUp:Cycle":
            self.cycle()
            return
        elif event == "CleanUp:CycleOnce":
            self.cycleOnce()
            return
        
        elif event == "CleanUpScheduler:StartDebug":
            logging.getLogger().setLevel(logging.DEBUG)
            return
        elif event == "CleanUpScheduler:EndDebug":
            logging.getLogger().setLevel(logging.INFO)
            return

    def cycle(self):
        """
        _cycle_

        Run a polling cycle and publish the next cycle triggering event

        """
        self.cycleOnce()
        self.ms.publish("CleanUp:Cycle")
        self.ms.commit()
        return

    def cycleOnce(self):
        """
        _cycleOnce_

        Do a single update

        """
        localDBS = self.args['DBSURL']
        dbsReader = DBSReader(localDBS)
        blockTasks = CleanUpTask()
        
        for unmergedDataset in listAllMergeDatasets():
            mergedDataset = unmergedDataset.replace("-unmerged", "")
            
            mergeXCheck = MergeSensorCrossCheck(unmergedDataset)
            unmergedToMergeFiles = mergeXCheck.getFileMap()

            #  //
            # // List of all merged LFNs known to merge sensor
            #//
            mergedLFNs = [ v for v in unmergedToMergedFiles.values()
                           if v != None ]
    
            #  //
            # // List of all merged LFNS that are known to DBS
            #//
            goodMerges = dbsReader.crossCheck(mergedDataset, *mergedLFNs)


            doneUnmergedFiles = []

            for unmerged, merged in unmergedToMergeFiles.items():
                if merged in goodMerges:
                    doneUnmergedFiles.append(unmerged)

            #  //
            # // Get a list of block names for each LFN
            #//
            blockMap = mergeXCheck.getBlocksMap()
            for lfn in doneUnmergedFiles:
                blockTasks.addFile(blockMap[lfn], lfn)

        #  //
        # // For each block name, find the site from DBS
        #//  Assume: Since we are dealing with unmerged files
        #  //Should be only one site per block.
        # // May want to add logic to check that....
        #//
        siteTasks = CleanUpTask()
        for block in blockTasks.keys():
            sites = dbsReader.listFileBlockLocation(block)
            if len(sites) == 0:
                # unknown location
                continue
            site = sites[-1]
            siteTasks.addFiles(site, *blockTasks[block])
            
        #  //
        # // TODO:
        #//  
        #  //For each site in siteTasks, generate a cleanup job spec with 
        # // the list of files to be cleaned, then publish the
        #//  cleanup workflow, job spec etc
        
        return


    def startComponent(self):
        """
        _startComponent_

        Start up the component

        """

 
        # create message service
        self.ms  = MessageService()
        # register
        self.ms.registerAs("CleanUpScheduler")

        # subscribe to messages
        self.ms.subscribeTo("CleanUp:Cycle")
        self.ms.subscribeTo("CleanUp:CycleOnce")
        self.ms.subscribeTo("CleanUpScheduler:StartDebug")
        self.ms.subscribeTo("CleanUpScheduler:EndDebug")
 
        # wait for messages
        while True:
            Session.set_database(dbConfig)
            Session.connect()
            Session.start_transaction()
            type, payload = self.ms.get()
            self.ms.commit()
            logging.debug("CleanUpScheduler: %s, %s" % (type, payload))
            self.__call__(type, payload)
            Session.commit_all()
            Session.close_all()

        
