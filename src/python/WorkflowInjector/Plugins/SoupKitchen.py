#!/usr/bin/env python
"""
_SoupKitchen_

Plugin to generate a fixed amount of production jobs from a workflow that
processes a set of datasets and mixes the injection of the
jobs in a random manner


Input should be an XML file of the form:

<SoupKitchen>
  <DBSURL>http://dbs.cern.ch</DBSURL>
  <Site>CERN</Site>
  <WorkflowSpec>/path/to/Workflow.xml</WorkflowSpec>
  <Dataset>/Primary1/Processed1/TIER1</Dataset>
  <Dataset>/Primary2/Processed2/TIER2</Dataset>
  ...
  <Dataset>/PrimaryN/ProcessedN/TIERN</Dataset>
</SoupKitchen>

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

class SoupKitchen(PluginInterface):
    """
    _SoupKitchen_

    Generate a pile of processing style jobs based on the workflow
    provided using a list of input datasets and mixing the datasets
    together.

    Input is an XML file containing a pointer to a workflow spec file
    and a list of datasets to be mixed together.

    
    """
    def handleInput(self, payload):
        logging.info("SoupKitchen: Handling %s" % payload)

        self.workflow = None
        self.datasets = []
        self.dbsUrl = None  ###TODO: Get Global DBS by default
        self.siteName = None
        self.loadPayload(payload)


        jobsList = []
        jobsCount = 0
        for dataset in self.datasets:
            factory = DatasetJobFactory(self.workflow,
                                        self.workingDir,
                                        self.dbsUrl,
                                        InitialRun = jobsCount)
            factory.allowedSites = [self.siteName]
            jobsList.extend(factory())
            jobsCount += len(jobsList)
            logging.info("Generated JobSpecs for dataset %s" %  dataset)        
        #  //
        # // Mix up the jobs List a few times
        #//
        random.shuffle(jobsList)
        random.shuffle(jobsList)
        random.shuffle(jobsList)

        #  //
        # //  Insert jobs into queue
        #//
        logging.info("Generated Total %s JobSpecs " % len(jobsList))        
        bulkQueueJobs([self.siteName], *jobsList)
        return
        

        
            

    def loadPayload(self, payloadFile):
        """
        _loadPayload_

        Load the XML input file

        """
        if not os.path.exists(payloadFile):
            raise RuntimeError, "Payload not found: %s" % payload
        

        improv = loadIMProvFile(payloadFile)
        workflowQ = IMProvQuery("SoupKitchen/WorkflowSpec[text()]")
        workflowFile = workflowQ(improv)[-1]
        logging.info("SoupKitchen: Loading Workflow: %s\n" % workflowFile)
        self.workflow = self.loadWorkflow(workflowFile)

        siteQ = IMProvQuery("SoupKitchen/Site[text()]")
        self.siteName = str(siteQ(improv)[-1])

        dbsUrlQ = IMProvQuery("SoupKitchen/DBSURL[text()]")
        self.dbsUrl = str(dbsUrlQ(improv)[-1])
                              
        
        datasetsQ = IMProvQuery("SoupKitchen/Dataset[text()]")
        datasets = datasetsQ(improv)
        self.datasets = [ str(x) for x in datasets ]
        msg = "Datasets to be scrambled:\n"
        for d in self.datasets:
            msg += "  %s\n" % d
        logging.info(msg)
        return
        

registerPlugin(SoupKitchen, SoupKitchen.__name__)



