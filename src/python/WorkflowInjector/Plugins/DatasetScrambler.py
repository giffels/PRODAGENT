#!/usr/bin/env python
"""
_DatasetScrambler_

Plugin to generate a fixed amount of production jobs from a workflow that
processes a set of datasets and mixes the injection of the
jobs in a random manner


Input should be an XML file of the form:

<DatasetScrambler>
  <DBSURL>http://dbs.cern.ch</DBSURL>
  <Site>CERN</Site>
  <WorkflowSpec>/path/to/Workflow.xml</WorkflowSpec>
  <Dataset>/Primary1/Processed1/TIER1</Dataset>
  <Dataset>/Primary2/Processed2/TIER2</Dataset>
  ...
  <Dataset>/PrimaryN/ProcessedN/TIERN</Dataset>
</DatasetScrambler>

SplitSize and SplitType will be taken from the workflow spec
to generate jobs
  
"""
import os
import logging
import random

from WorkflowInjector.PluginInterface import PluginInterface
from WorkflowInjector.Registry import registerPlugin

from ProdCommon.JobFactory.DatasetJobFactory import DatasetJobFactory
from ProdCommon.MCPayloads.DatasetConventions import parseDatasetPath

from JobQueue.JobQueueAPI import bulkQueueJobs

from IMProv.IMProvLoader import loadIMProvFile
from IMProv.IMProvQuery import IMProvQuery

class DatasetScrambler(PluginInterface):
    """
    _DatasetScrambler_

    Generate a pile of processing style jobs based on the workflow
    provided using a list of input datasets and mixing the datasets
    together.

    Input is an XML file containing a pointer to a workflow spec file
    and a list of datasets to be mixed together.

    
    """
    def handleInput(self, payload):
        logging.info("DatasetScrambler: Handling %s" % payload)

        self.workflow = None
        self.datasets = []
        self.dbsUrl = None  ###TODO: Get Global DBS by default
        self.siteName = None
        self.loadPayload(payload)

        
        jobsList = []
        
        for dataset in self.datasets:
            self.makeWorkflowTemplate(dataset)
            factory = DatasetJobFactory(self.workflow,
                                        self.workingDir,
                                        self.dbsUrl,
                                        InitialRun = len(jobsList) )
            factory.allowedSites = [self.siteName]
            jobsList.extend(factory())
            
            
        logging.info("Generated %s JobSpecs" % len(jobsList))
        logging.debug(str(jobsList[0]))
        logging.debug(str(jobsList[-1]))
        #  //
        # // Randomise all jobs
        #//
        random.shuffle(jobsList)
        
        #  //
        # // Directly add them to the JobQueue in the random order
        #//  (In this case we bypass the plugin queueJob method since
        #  // we have a pile of jobs and want to try and maintain the
        # //  randomised order)
        #//
        bulkQueueJobs([self.siteName], *jobsList)
        return
        

    def makeWorkflowTemplate(self, dataset):
        """
        _makeWorkflowTemplate_

        Add the dataset provided as the input dataset to the
        workflow template

        """
        datasetPath = parseDatasetPath(dataset)
        self.workflow.payload._InputDatasets = []
        newDataset = self.workflow.payload.addInputDataset(
            datasetPath['Primary'],
            datasetPath['Processed']
            )
        newDataset['DataTier'] = datasetPath['DataTier']
        return
        
            

    def loadPayload(self, payloadFile):
        """
        _loadPayload_

        Load the XML input file

        """
        if not os.path.exists(payloadFile):
            raise RuntimeError, "Payload not found: %s" % payload
        

        improv = loadIMProvFile(payloadFile)
        workflowQ = IMProvQuery("DatasetScrambler/WorkflowSpec[text()]")
        workflowFile = workflowQ(improv)[-1]
        logging.info("DatasetScrambler: Loading Workflow: %s\n" % workflowFile)
        self.workflow = self.loadWorkflow(workflowFile)

        siteQ = IMProvQuery("DatasetScrambler/Site[text()]")
        self.siteName = str(siteQ(improv)[-1])

        dbsUrlQ = IMProvQuery("DatasetScrambler/DBSURL[text()]")
        self.dbsUrl = str(dbsUrlQ(improv)[-1])
                              
        
        datasetsQ = IMProvQuery("DatasetScrambler/Dataset[text()]")
        datasets = datasetsQ(improv)
        self.datasets = [ str(x) for x in datasets ]
        msg = "Datasets to be scrambled:\n"
        for d in self.datasets:
            msg += "  %s\n" % d
        logging.info(msg)
        return
        

registerPlugin(DatasetScrambler, DatasetScrambler.__name__)



