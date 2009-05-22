#!/usr/bin/env python
"""
_ResultsStatus_

Utils to check the state of a set of StoreResults jobs using the WorkflowEntities
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
#         logging.info("Node is %s" % node)
        for dataset in node._OutputDatasets:
#             logging.info("Checking dataset %s" % dataset)
#             if dataset.has_key("NoMerge"):
#                 #  //
#                 # // If we need to avoid merging some datasets we
#                 #//  can add a NoMerge key and this will ignore it
#                 continue
            self.datasets.append(dataset)

        logging.info("Monitoring datasets %s" % self.datasets)


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

class ResultsStatus:
    """
    _ResultsStatus_

    Object to retrieve and compute the overall state of a Results
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

#         logging.info("Unmerged datasets: %s" % self.unmergedDatasets())
#         logging.info("Merged datasets: %s" % self.mergedDatasets())


        if self.processingComplete():
            logging.info("Processing Complete for %s" % self.workflow)
            for dataset in self.unmergedDatasets():
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
        pass


    def processingComplete(self):
        """
        _processingComplete_

        look at the processing jobs for the workflow, and return True
        if all processing jobs are complete

        """
        allJobs = WEUtils.jobsForWorkflow(self.workflow, "Merge")
        finishedJobs = WEUtils.jobsForWorkflow(self.workflow, "Merge", "finished")
        logging.info("All jobs: %s" % allJobs)
        logging.info("Finished jobs: %s" % finishedJobs)
        totalProcessing = len(allJobs)
        totalProcComplete = len(finishedJobs)

        if totalProcessing == 0:
            return False

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
