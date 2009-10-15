#!/usr/bin/env python
"""
_ResultsStatus_

Utils to check the state of a set of StoreResults jobs using the WorkflowEntities
table

"""

__revision__ = "$Id: ResultsStatus.py,v 1.8 2009/10/09 22:00:57 ewv Exp $"
__version__  = "$Revision: 1.8 $"
__author__   = "ewv@fnal.gov"

import logging

import ProdAgent.WorkflowEntities.Utilities as WEUtils
import ProdAgent.WorkflowEntities.Workflow as WEWorkflow
from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec
from ProdCommon.MCPayloads.MergeTools import createMergeDatasetWorkflow
from MergeSensor.MergeSensorDB.Interface.MergeSensorDB import MergeSensorDB
from ProdCommon.Database import Session
from ProdCommon.DataMgmt.DBS.DBSReader import DBSReader



class ExtractDatasets:
    def __init__(self):
        self.datasets = []


    def __call__(self, node):
        for dataset in node._OutputDatasets:
            self.datasets.append(dataset)



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

        self.doMigration = self.configuration.get("MigrateToGlobal", True)
        self.doInjection = self.configuration.get("InjectToPhEDEx", True)

    def __call__(self):
        """
        _operator()_

        Evaluate the status of this workflow from the WorkflowEntities
        data and publish any events that are triggered

        """

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
                        "Publishing PhEDExDataServiceInject for %s" % dataset)
                    self.msgSvcRef.publish("PhEDExDataServiceInject",
                                           self.workflowFile,"00:03:00")
                    self.msgSvcRef.commit()

            Session.commit_all()


            WEWorkflow.setFinished(self.workflow)
            WEWorkflow.remove(self.workflow)
            Session.commit_all()

        return


    def processingComplete(self):
        """
        _processingComplete_

        look at the processing jobs for the workflow, and return True
        if all processing jobs are complete

        """
        intermediateDBS = self.workflowSpec.parameters['DBSURL']
        outputDataset   = self.workflowSpec.outputDatasets()[0].name()

        allJobs      = WEUtils.jobsForWorkflow(self.workflow, "Merge")
        finishedJobs = WEUtils.jobsForWorkflow(self.workflow, "Merge", "finished")
        totalProcessing = len(allJobs)
        totalComplete   = len(finishedJobs)

        logging.info("%s: %s/%s jobs complete" %
                      (self.workflow,totalComplete,totalProcessing))

        if totalProcessing == 0: # Protection for non-sensical situation
            return False

        if totalComplete < totalProcessing:
            return False

        # Check to make sure local DBS knows about all output files
        try:
            reader = DBSReader(intermediateDBS)
            blockList = reader.getFiles(dataset = outputDataset)
        except:
            logging.info("Dataset not in DBS yet")
            return False

        totalRegistered = 0
        for block in blockList:
            totalRegistered += len(blockList[block]['Files'])

        logging.info("%s: %s/%s jobs registered" %
                      (self.workflow,totalRegistered,totalProcessing))
        if totalRegistered < totalProcessing:
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
