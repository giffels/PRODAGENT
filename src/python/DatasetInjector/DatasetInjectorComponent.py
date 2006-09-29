#!/usr/bin/env python
"""
_DatasetInjector_

Generate a set of JobSpecs to consume a dataset and inject them into
the ProdAgent.

Note: This component is potentially capable of creating LOTS of jobs
if the dataset is large.

"""


__revision__ = "$Id: DatasetInjectorComponent.py,v 1.3 2006/08/31 16:37:19 evansde Exp $"
__version__ = "$Revision: 1.3 $"
__author__ = "evansde@fnal.gov"


import os
import logging
from logging.handlers import RotatingFileHandler


from MessageService.MessageService import MessageService


from DatasetInjector.DatasetIterator import DatasetIterator


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
        self.args.update(args)
        
        if self.args['Logfile'] == None:
            self.args['Logfile'] = os.path.join(self.args['ComponentDir'],
                                                "ComponentLog")
        logHandler = RotatingFileHandler(self.args['Logfile'],
                                         "a", 1000000, 3)
        logFormatter = logging.Formatter("%(asctime)s:%(message)s")
        logHandler.setFormatter(logFormatter)
        logging.getLogger().addHandler(logHandler)
        logging.getLogger().setLevel(logging.DEBUG)
        self.ms = None

        if self.args['WorkflowCache'] == None:
            self.args['WorkflowCache'] = os.path.join(
                self.args['ComponentDir'], "WorkflowCache")
        if not os.path.exists(self.args['WorkflowCache']):
            os.makedirs(self.args['WorkflowCache'])

        self.iterators = {}
        self.iterator = None
        
        logging.info("DatasetInjector Component Started")



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
        newIterator = DatasetIterator(workflowPath,
                                      self.args['ComponentDir'] )
        logging.debug("Importing Dataset: %s" % newIterator.inputDataset())
        importResult = newIterator.importDataset()
        if importResult:
            msg = "Unable to Import Dataset for workflow:\n"
            msg += "%s\n" % workflowFile
            logging.error(msg)
            return
        logging.debug("Import successful")
        #  //
        # // Keep ref to iterator
        #//
        self.iterators[workflowName] = newIterator
        self.iterator = newIterator
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
            pathname = os.path.join(self.args['WorkflowCache'], item)
            if not os.path.exists(pathname):
                continue

            newIterator = DatasetIterator(pathname,
                                          self.args['ComponentDir'] )
            self.iterators[item] = newIterator
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
        jobDefs = self.iterator.releaseJobs(numJobs)
        logging.debug("Released %s jobs" % len(jobDefs))

        for jdef in jobDefs:
            jobSpec = self.iterator(jdef)
            logging.debug("Publishing CreateJob: %s" % jobSpec)
            self.ms.publish("CreateJob", jobSpec)
            self.ms.commit()
            
        
        
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
        
        
        # wait for messages
        while True:
            msgtype, payload = self.ms.get()
            self.ms.commit()
            self.__call__(msgtype, payload)

        
