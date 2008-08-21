#!/usr/bin/env python
"""
_RelValStatus_

Utils to check the state of a set of RelVal jobs using the WorkflowEntities
table

"""
import logging

import ProdAgent.WorkflowEntities.Utilities as WEUtils
import ProdAgent.WorkflowEntities.Workflow as WEWorkflow
from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec
from ProdCommon.MCPayloads.MergeTools import createMergeDatasetWorkflow
from MergeSensor.MergeSensorDB.Interface.MergeSensorDB import MergeSensorDB
from ProdCommon.Database import Session



class ExtractDatasets:
    def __init__(self):
        self.datasets = []

    def __call__(self, node):
        for dataset in node._OutputDatasets:
            if dataset.has_key("NoMerge"):
                #  //
                # // If we need to avoid merging some datasets we
                #//  can add a NoMerge key and this will ignore it
                continue
            self.datasets.append(dataset)



def countOutstandingUnmergedFiles(dataset):
    """
    _countOutstandingUnmergedFiles_

    Get the number of files awaiting merging for a dataset from
    the MergeSensor DB

    """
    mergeDB = MergeSensorDB()
    try:
        filecount = len(mergeDB.getUnmergedFileListFromDataset(dataset))
    except Exception, ex:
        filecount = 0
    return filecount

class RelValStatus:
    """
    _RelValStatus_

    Object to retrieve and compute the overall state of a RelVal
    Workflow

    """
    def __init__(self, config, msgSvcRef,  **workflowDetails):
        self.configuration = config
        self.msgSvcRef = msgSvcRef
        self.workflowDetails = workflowDetails
        self.workflow = workflowDetails['id']
        self.workflowFile = workflowDetails['workflow_spec_file']
        self.workflowSpec = WorkflowSpec()
        self.workflowSpec.load(self.workflowFile)
        
        self.doMigration = self.configuration.get("MigrateToGlobal", False)
        self.doInjection = self.configuration.get("InjectToPhEDEx", False)

        
    def __call__(self):
        """
        _operator()_

        Evaluate the status of this workflow from the WorkflowEntities
        data and publish any events that are triggered

        """
        processed = False
        merged = False
        
        if self.processingComplete():
            logging.info("Processing Complete for %s" % self.workflow)
            processed = True
            #  //
            # //  publish ForceMerge for datasets
            #//
            for dataset in self.unmergedDatasets():
                if countOutstandingUnmergedFiles(dataset) > 0:
                    logging.debug("Publishing ForceMerge for %s" % dataset)
                    self.msgSvcRef.publish("ForceMerge", dataset)
            self.msgSvcRef.commit()
            Session.commit_all()

        if self.mergingComplete():
            logging.info("Merging Complete for %s" % self.workflow)
            merged = True
            #  //
            # // Close Blocks and migrate to global
            #//  Inject them into PhEDEx
            for dataset in self.mergedDatasets():
                if self.doMigration:
                    logging.debug(
                        "Publishing MigrateToGlobal for %s" % dataset)
                    self.msgSvcRef.publish(
                        "DBSInterface:MigrateDatasetToGlobal",
                        dataset)
                    self.msgSvcRef.commit()
                if self.doInjection:
                    logging.debug(
                        "Publishing PhEDExInjectDataset for %s" % dataset)
                    self.msgSvcRef.publish("PhEDExInjectDataset",
                                           dataset)
                    self.msgSvcRef.commit()
                
            Session.commit_all()
                

        if processed and merged:
            #  //
            # // All done: close the workflow out 
            #//
            logging.info("Workflow %s complete" % self.workflow)
            WEWorkflow.setFinished(self.workflow)
            WEWorkflow.remove(self.workflow)
            Session.commit_all()

            #  //
            # // Generate summary
            #//
            self.summariseWorkflow()
            
        return
    

    def summariseWorkflow(self):
        """
        _summariseWorkflow_

        Workflow has been finished, do whatever is required
        to generate a summary for the jobs and dispatch it
        to wherever it is needed
        
        """
        logging.info("Summarising Workflow %s" % self.workflow)
        #  //
        # // Need input from Data Ops here:
        #//
        #  // Can gather information from:
        # //    - Job Reports in Cache
        #//     - ProdMon tables
        #  //   - WE Tables
        # // Generate summary HTML?
        #//  Publish to web somewhere?
        pass
 

    def processingComplete(self):
        """
        _processingComplete_

        look at the processing jobs for the workflow, and return True
        if all processing jobs are complete

        """
        allJobs = WEUtils.jobsForWorkflow(self.workflow, "Processing")
        finishedJobs = WEUtils.jobsForWorkflow(self.workflow, "Processing", "finished")

        totalProcessing = len(allJobs)
        totalProcComplete = len(finishedJobs)

        if totalProcComplete < totalProcessing:
            return False

        return True
        

    def unmergedDatasets(self):
        """
        _unmergedDatasets_

        Retrieve a list of datasets tht can be ForceMerge'd
        """
        extractor = ExtractDatasets()
        self.workflowSpec.payload.operate(extractor)
        result = [ x.name() for x in extractor.datasets ]
        return result

    
    def mergingComplete(self):
        """
        _mergingComplete_

        look at the jobs for the merge jobs for the workflow and
        return True if all merge jobs are complete
        
        """
        allMerges = WEUtils.jobsForWorkflow(self.workflow, "Merge")
        finishedMerges = WEUtils.jobsForWorkflow(self.workflow, "Merge", "finished")
        totalMerging = len(allMerges)
        totalMergeComplete = len(finishedMerges)
        
        if totalMerging == 0:
            # no merges in the system => no merge complete
            return False

        if totalMergeComplete < totalMerging:
            return False
        return True

    def mergedDatasets(self):
        """
        _mergedDatasets_

        Get a list of merged datasets from the workflow that can
        be used to close blocks, migrate and inject

        """
        mergeWorkflow = createMergeDatasetWorkflow(self.workflowSpec)
        extractor = ExtractDatasets()
        mergeWorkflow.payload.operate(extractor)
        result = [ x.name() for x in extractor.datasets ]
        return result
        


   
    
