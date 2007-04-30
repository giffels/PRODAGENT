#!/usr/bin/env python
"""
_DatasetInjector_

Generate a set of JobSpecs to consume a dataset and inject them into
the ProdAgent.

Note: This component is potentially capable of creating LOTS of jobs
if the dataset is large.

"""


__revision__ = "$Id: DatasetInjectorComponent.py,v 1.14 2007/04/26 13:58:26 afanfani Exp $"
__version__ = "$Revision: 1.14 $"
__author__ = "evansde@fnal.gov"


import os
import logging

from MessageService.MessageService import MessageService

import ProdAgentCore.LoggingUtils as LoggingUtils

from DatasetInjector.DatasetIterator import DatasetIterator

from ProdCommon.MCPayloads.JobSpec import JobSpec


class DatasetInjectorComponent:
    """
    _DatasetInjectorComponent_

    Component to generate JobSpecs based on DBS/DLS information for a
    dataset

    """
    def __init__(self, **args):
        self.args = {}
        self.args['ComponentDir'] = None
        self.args['Logfile'] = None
        self.args['WorkflowCache'] = None
        self.args['QueueJobMode'] = False
        self.args['BulkTestMode'] = False        
        self.args.update(args)

        if self.args['Logfile'] == None:
            self.args['Logfile'] = os.path.join(self.args['ComponentDir'],
                                                "ComponentLog")
        LoggingUtils.installLogHandler(self)
        self.queueMode = False
        if str(self.args['QueueJobMode']).lower() == "true":
            self.queueMode = True
        self.bulkTestMode = False
        if str(self.args['BulkTestMode']).lower() == "true":
            self.bulkTestMode = True
        self.ms = None
        
        if self.args['WorkflowCache'] == None:
            self.args['WorkflowCache'] = os.path.join(
                self.args['ComponentDir'], "WorkflowCache")
        if not os.path.exists(self.args['WorkflowCache']):
            os.makedirs(self.args['WorkflowCache'])

        self.iterators = {}
        self.iterator = None
        
        msg = "DatasetInjector Component Started\n"
        msg += " => QueueMode: %s\n" % self.queueMode
        msg += " => BulkTestMode: %s\n" % self.bulkTestMode
        logging.info(msg)

    def __call__(self, event, payload):
        """
        _operator()_

        Define call for this object to allow it to handle events that
        it is subscribed to
        """
        logging.debug("Event: %s Payload: %s" % (event, payload))
        if event == "DatasetInjector:StartDebug":
            logging.getLogger().setLevel(logging.DEBUG)
            return
        if event == "DatasetInjector:EndDebug":
            logging.getLogger().setLevel(logging.INFO)
            return

        if event == "DatasetInjector:SetWorkflow":
            self.setWorkflow(payload)
            return
        if event == "AcceptedJob":
            self.removeJobSpec(payload)
            return
        
        if event == "DatasetInjector:LoadWorkflows":
            self.loadWorkflows()
            return
        if event == "DatasetInjector:SelectWorkflow":
            self.selectWorkflow(payload)
            return
        if event == "DatasetInjector:UpdateWorkflow":
            self.updateWorkflow(payload)
            return
        if event == "DatasetInjector:RemoveWorkflow":
            self.removeWorkflow(payload)
            return
        
        if event == "DatasetInjector:ReleaseJobs":
            self.releaseJobs(payload)
            return
        
        return

    def removeJobSpec(self, jobSpecId):
        """
        _removeJobSpec_

        Remove the spec file for the job spec ID provided

        """
        for iterName, iterInstance in self.iterators.items():
            if jobSpecId.startswith(jobSpecId):
                iterInstance.removeSpec(jobSpecId)
                iterInstance.save(self.args['WorkflowCache'])
        return
    
    def setWorkflow(self, workflowFile):
        """
        _setWorkflow_

        """
        if not os.path.exists(workflowFile):
            msg = "Workflow File Not Found: %s\n" % workflowFile
            msg += "Cannot create jobs for this workflow\n"
            msg += "Payload for DatasetInjector:SetWorkflow event\n"
            msg += "Must be a valid file, readable by this component\n"
            logging.warning(msg)
            self.iterator = None
            return
        

        logging.debug("Importing workflow file:\n%s\n" % workflowFile)
        #  //
        # // Import the workflow and instantiate an iterator for it
        #//
        migrateCommand = "/bin/cp %s %s" % (
            workflowFile, self.args['WorkflowCache'],
            )
        os.system(migrateCommand)
        workflowName = os.path.basename(workflowFile)
        workflowPath = os.path.join( self.args['WorkflowCache'], workflowName)
        logging.debug("Instantiating DatasetIterator for %s" % workflowPath)

        try:
            newIterator = DatasetIterator(workflowPath,
                                          self.args['ComponentDir'] )
        except Exception, ex:
            msg = "ERROR: Unable to Instantiate a DatasetIterator\n"
            msg += "For file:\n%s\n" % workflowPath
            msg += "Error: %s\n" % str(ex)
            logging.error(msg)
            return
            
        logging.debug("Importing Dataset: %s" % newIterator.inputDataset())
        importResult = newIterator.importDataset()
        if importResult:
            msg = "Unable to Import Dataset for workflow:\n"
            msg += "%s\n" % workflowFile
            logging.error(msg)
            return
        logging.debug("Import successful")

        newIterator.loadPileupDatasets()
        #SEnames=self.iterator.loadPileupSites()
        #if (isinstance(SEnames,list)) and len(SEnames) > 0:
        #   site=",".join(SEnames)
        #   self.setSitePref(site) 
        
        #  //
        # // Keep ref to iterator
        #//
        self.iterators[workflowName] = newIterator
        self.iterator = newIterator
        self.iterator.save(self.args['WorkflowCache'])

        self.ms.publish("NewWorkflow", workflowPath)
        self.ms.commit()
        self.ms.publish("NewDataset", workflowPath)
        self.ms.commit()

        
        return

    
        

    def loadWorkflows(self):
        """
        _loadWorkflows_

        """
        logging.debug("Loading Workflows")
        self.iterator = None
        self.iterators = {}
        fileList = os.listdir(self.args['WorkflowCache'])
        for item in fileList:
            if not item.endswith(".xml"):
                continue
            if item.endswith("Persist.xml"):
                continue
            pathname = os.path.join(self.args['WorkflowCache'], item)
            if not os.path.exists(pathname):
                continue
            try:
                newIterator = DatasetIterator(pathname,
                                              self.args['ComponentDir'] )
                newIterator.load(self.args['WorkflowCache'])
            except Exception, ex:
                msg = "ERROR: Unable to Instantiate a DatasetIterator\n"
                msg += "For file:\n%s\n" % pathname
                msg += "Error: %s\n" % str(ex)
                logging.error(msg)
                continue
            self.iterators[item] = newIterator

        currWorkflow = os.path.join(self.args['WorkflowCache'],
                                    "current.workflow")
        if os.path.exists(currWorkflow):
            content = file(currWorkflow).read()
            content = content.strip()
            logging.debug(
                "Current Workflow File exists: Contents: %s" % content)
            self.selectWorkflow(content)
            
        return
    
            
    def selectWorkflow(self, payload):
        """
        _selectWorkflow_

        """
        logging.debug("SelectWorkflow:%s" % payload)
        if not self.iterators.has_key(payload):
            msg = "No Iterator matching name: %s\n" % payload
            msg += "Found in list of available iterators:\n"
            for iterName in self.iterators.keys():
                msg += "   %s\n" % iterName
            logging.error(msg)
            self.iterator = None
            return
        self.iterator = self.iterators[payload]
        currWorkflow = os.path.join(self.args['WorkflowCache'],
                                    "current.workflow")
        handle = open(currWorkflow, 'w')
        handle.write(payload)
        handle.close()
        logging.debug("Iterator set to: %s" % payload)
        return
        

    def updateWorkflow(self, payload):
        """
        _updateWorkflow_

        """
        iterator = self.iterators.get(payload, None)
        if iterator == None:
            msg = "No DatasetIterator found for workflow name: %s\n" % payload
            msg += "Unable to update Workflow...\n"
            logging.error(msg)
            return

        iterator.updateDataset()
        return
        

    def removeWorkflow(self, payload):
        """
        _removeWorkflow_

        Remove the workflow entries from the DB and workflow cache

        """
        workflowFile = os.path.join(self.args['WorkflowCache'], payload)
        iterator = self.iterators.get(payload, None)
        if os.path.exists(workflowFile):
            os.remove(workflowFile)

        if iterator != None:
            iterator.cleanup()
            del self.iterators[payload]
        return
    
        
    
        
    def releaseJobs(self, payload):
        """
        _releaseJobs_

        payload should be an int value, which is number of jobs to be
        released.

        """
        if self.iterator == None:
            msg = "No Iterator is set, cannot release jobs\n"
            msg += "You must select an iterator with the "
            msg += "DatasetInjector:SelectWorkflow event first"
            logging.error(msg)
            return
        try:
            numJobs = int(payload)
        except ValueError:
            msg = "Payload for DatasetInjector:ReleaseJobs "
            msg += "should be an integer\n"
            msg += "value passed: %s\n" % payload
            logging.error(msg)
            return
        #  //
        # // Check to see if dataset is complete. 
        #//  If not, then we cant create more jobs for it
        #  //unless it gets updated.
        # //
        #//
        if self.iterator.isComplete():
            msg = "Unable to release jobs for workflow: %s\n" % (
                
                self.iterator.workflowSpec.workflowName(),
                )
            msg += "There are no jobs available to release"
            logging.warning(msg)
            return
        logging.debug("Releasing %s jobs for %s" % (
            numJobs, self.iterator.workflowSpec.workflowName(),
            )
                      )


        self.iterator.load(self.args['WorkflowCache'])
        jobDefs = self.iterator.releaseJobs(numJobs)
        logging.debug("Released %s jobs" % len(jobDefs))
        

        bulkSpecs = []
        for jdef in jobDefs:
          jobSpec = self.iterator(jdef)
          if jobSpec:
            if not self.bulkTestMode:
               if self.queueMode:
                 logging.debug("Publishing QueueJob: %s" % jobSpec)
                 self.ms.publish("QueueJob", jobSpec)
               else:
                 logging.debug("Publishing CreateJob: %s" % jobSpec)
                 self.ms.publish("CreateJob", jobSpec)
                 
               self.ms.commit()
            else:
               if numJobs == 1:
                    msg = "Cannot Bulk Submit a single job\n"
                    msg += "When in BulkTestMode, you must provide an"
                    msg += " int payload > 1\n"
                    msg += "For the RequestInjector:ResourcesAvailable event"
                    logging.warning(msg)
                    return
               bulkSpecs.append(jobSpec)

        if self.bulkTestMode:
            firstSpec = bulkSpecs[0]
            bulkSpecName = "%s.BULK" % firstSpec
            bulkSpecName = bulkSpecName.replace("file:///", "/")
            logging.info("Bulk Spec: %s" % bulkSpecName)
            logging.info("JobSpec()")
            bulkSpec = JobSpec()
            logging.debug("bulkSpec.load...")
            firstSpecName = firstSpec.replace("file:///", "/")
            if not os.path.exists(firstSpecName):
                msg = "Primary Spec for Bulk Spec creation not found:\n"
                msg += "%s\n" % firstSpecName
                msg += "Cannot construct Bulk Spec"
                logging.error(msg)
                return
            bulkSpec.load(firstSpec)
            logging.debug("for item...")
            for item in bulkSpecs:
                logging.debug ("item %s " % item)
                specID = os.path.basename(item).replace("-JobSpec.xml", "")
                bulkSpec.bulkSpecs.addJobSpec(specID, item)

            bulkSpec.save(bulkSpecName)
            logging.info("Publishing Bulk Spec")
            if self.queueMode:
                self.ms.publish("QueueJob", bulkSpecName)
            else:
                self.ms.publish("CreateJob", bulkSpecName)
            self.ms.commit()
            
        self.iterator.save(self.args['WorkflowCache'])
     
        return


        



    def startComponent(self):
        """
        _startComponent_
        
        Start the servers required for this component

        """                                   
        # create message service
        self.ms = MessageService()

        # register
        self.ms.registerAs("DatasetInjector")
        
        # subscribe to messages
        
        self.ms.subscribeTo("DatasetInjector:StartDebug")
        self.ms.subscribeTo("DatasetInjector:EndDebug")
        self.ms.subscribeTo("DatasetInjector:SetWorkflow")
        self.ms.subscribeTo("DatasetInjector:SelectWorkflow")
        self.ms.subscribeTo("DatasetInjector:LoadWorkflows")
        self.ms.subscribeTo("DatasetInjector:ReleaseJobs")
        self.ms.subscribeTo("DatasetInjector:UpdateWorkflow")
        self.ms.subscribeTo("DatasetInjector:RemoveWorkflow")
        self.ms.subscribeTo("AcceptedJob")
        
        # wait for messages
        while True:
            msgtype, payload = self.ms.get()
            self.ms.commit()
            self.__call__(msgtype, payload)

        
