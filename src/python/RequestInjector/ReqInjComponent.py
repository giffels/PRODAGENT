#!/usr/bin/env python
"""
_ReqInjComponent_

ProdAgent Component implementation to fake a call out to the ProdMgr to
get the next available request allocation.

"""
__version__ = "$Revision: 1.6 $"
__revision__ = "$Id: ReqInjComponent.py,v 1.6 2006/05/01 22:12:53 fvlingen Exp $"
__author__ = "evansde@fnal.gov"


import os
import logging
from logging.handlers import RotatingFileHandler

from RequestInjector.RequestIterator import RequestIterator
from MessageService.MessageService import MessageService
from JobState.JobStateAPI import JobStateChangeAPI


class ReqInjComponent:
    """
    _ReqInjComponent_

    Object to encapsulate generation of a concrete job specification

    """
    def __init__(self, **args):
        self.args = {}
        self.args['ComponentDir'] = None
        self.args['Logfile'] = None
        self.args['JobState'] = True
        self.args['WorkflowCache'] = None
        self.args.update(args)
        self.job_state = self.args['JobState']
        
        if self.args['Logfile'] == None:
            self.args['Logfile'] = os.path.join(self.args['ComponentDir'],
                                                "ComponentLog")
        logHandler = RotatingFileHandler(self.args['Logfile'],
                                         "a", 1000000, 3)
        logFormatter = logging.Formatter("%(asctime)s:%(message)s")
        logHandler.setFormatter(logFormatter)
        logging.getLogger().addHandler(logHandler)
        logging.getLogger().setLevel(logging.INFO)

        if self.args['WorkflowCache'] == None:
            self.args['WorkflowCache'] = os.path.join(
                self.args['ComponentDir'], "WorkflowCache")
        if not os.path.exists(self.args['WorkflowCache']):
            os.makedirs(self.args['WorkflowCache'])

        self.iterators = {}
        self.iterator = None
        self.ms = None
        logging.info("RequestInjector Component Started")
        
    def __call__(self, event, payload):
        """
        _operator()_

        Define call for this object to allow it to handle events that
        it is subscribed to
        """
        if event == "ResourcesAvailable":
            self.newJob()
            return
        if event == "RequestInjector:SetWorkflow":
            self.newWorkflow(payload)
            return
        if event == "RequestInjector:SelectWorkflow":
            self.selectWorkflow(payload)
            return
            
        if event == "RequestInjector:NewDataset":
            self.newDataset()
            return
        if event == "RequestInjector:LoadWorkflows":
            self.loadWorkflows()
            return
        if event == "RequestInjector:SetEventsPerJob":
            self.setEventsPerJob(payload)
            return
        if event == "RequestInjector:SetInitialRun":
            self.setInitialRun(payload)
            return
        if event == "RequestInjector:StartDebug":
            logging.getLogger().setLevel(logging.DEBUG)
            return
        if event == "RequestInjector:EndDebug":
            logging.getLogger().setLevel(logging.INFO)
            return
        
        return

    def newDataset(self):
        """
        _newDataset_

        Publish a NewDataset event with the payload being the
        URL of the current WorkflowSpec XML file

        """
        if self.iterator == None:
            msg = "Unable to publish NewDataset Event:\n"
            msg += "No Workflow has been specified\n"
            msg += "Please specify a workflow with the\n"
            msg += "RequestInjector:SelectWorkflow Event\n"
            logging.error(msg)
            return
        payload = "file://%s" % self.iterator.workflow
        self.ms.publish("NewDataset", payload)
        self.ms.commit()       
        return
    

    def newWorkflow(self, workflowFile):
        """
        _newWorkflow_

        Set the current workflow spec file that jobs will be created from,
        create a RequestIterator instance to generate jobs from it.

        """
        if not os.path.exists(workflowFile):
            msg = "Workflow File Not Found: %s\n" % workflowFile
            msg += "Cannot create jobs for this workflow\n"
            msg += "Payload for RequestInjector:SetWorkflow event\n"
            msg += "Must be a valid file, readable by this component\n"
            logging.warning(msg)
            self.iterator = None
            return

        migrateCommand = "/bin/cp %s %s" % (
            workflowFile, self.args['WorkflowCache'],
            )
        os.system(migrateCommand)
        workflowName = os.path.basename(workflowFile)
        workflowPath = os.path.join( self.args['WorkflowCache'], workflowName)
        newIterator = RequestIterator(workflowPath,
                                      self.args['ComponentDir'] )
        self.iterators[workflowName] = newIterator
        self.iterator = newIterator
        return

    def selectWorkflow(self, workflowName):
        """
        _selectWorkflow_

        Switch which request iterator is being used at present based
        on the name of the workflow

        """
        if workflowName not in self.iterators.keys():
            msg = "Error: Cannot select Workflow: %s\n" % workflowName
            msg += "Nothing known about that workflow\n"
            msg += "You may need to import the workflow using the\n"
            msg += "RequestInjector:SetWorkflow event\n"
            msg += "Known Workflows Are:\n"
            msg += "%s" % self.iterators.keys()
            logging.error(msg)
            return
        self.iterator = self.iterators[workflowName]
        return
    
        
        
    def newJob(self):
        """
        _newJob_

        Create a new job from the current workflow and send
        a CreateJob event with the JobSpec for it

        """
        
        
        if self.iterator == None:
            msg = "RequestInjector: No Workflow Set, cannot create job"
            msg += "You need to send a RequestInjector:SetWorkflow event"
            msg += "With the file containing the workflow as the payload"
            logging.warning(msg)
            return
        jobSpec = self.iterator()
        #  //
        # // Save last known counter in WorkflowCache area for workflow
        #//
        workflowCount = os.path.join(
            self.args['WorkflowCache'],
            "%s%s"  % (os.path.basename(self.iterator.workflow), ".counter") )
        handle = open(workflowCount, 'w')
        handle.write("%s" % self.iterator.count)
        handle.close()
        
        if self.job_state:
            try: 
                jobSpecID = self.iterator.currentJob
                # NOTE: temporal fix for dealing with duplicate job spec:
                JobStateChangeAPI.cleanout(jobSpecID)
            except StandardError, ex:
                logging.error('ERROR: '+str(ex))
        
        self.ms.publish("CreateJob", jobSpec)
        self.ms.commit()
        return

    def setEventsPerJob(self, numEvents):
        """
        _setEventsPerJob_

        Set the number of events per job for the current workflow

        """
        try:
            eventsPerJob = int(numEvents)
        except StandardError:
            msg = "RequestInjector: Attempted to set number of events per job"
            msg += "To non integer value: %s" % numEvents
            logging.warning(msg)
            return
        if self.iterator == None:
            msg = "RequestInjector: Attempted to set number of events per job"
            msg += "without specifying a workflow first"
            logging.warning(msg)
            return
        self.iterator.eventsPerJob = eventsPerJob
        #  //
        # // Save last known events per job in WorkflowCache area for workflow
        #//
        workflowEvents = os.path.join(
            self.args['WorkflowCache'],
            "%s%s"  % (os.path.basename(self.iterator.workflow), ".events") )
        handle = open(workflowEvents, 'w')
        handle.write("%s" % self.iterator.eventsPerJob)
        handle.close()
        return

    def setInitialRun(self, initialRun):
        """
        _setInitialRun_

        Set the counter value of the current workflow iterator

        """
        try:
            run = int(initialRun)
        except StandardError:
            msg = "RequestInjector: Attempted to set initial run"
            msg += "To non integer value: %s" % initialRun
            logging.warning(msg)
            return
        if self.iterator == None:
            msg = "RequestInjector: Attempted to set initial run number"
            msg += "without specifying a workflow first"
            logging.warning(msg)
            return
        self.iterator.count = run
        #  //
        # // Save last known counter in WorkflowCache area for workflow
        #//
        workflowCount = os.path.join(
            self.args['WorkflowCache'],
            "%s%s"  % (os.path.basename(self.iterator.workflow), ".counter") )
        handle = open(workflowCount, 'w')
        handle.write("%s" % self.iterator.count)
        handle.close()
        return

    def loadWorkflows(self):
        """
        _loadWorkflows_

        For all workflow files in the WorkflowCache load them and
        their run and event counts into memory

        This erases everything in memory so far.

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
            #  //
            # // Load Workflow Spec into iterator
            #//
            logging.debug("Loading Workflow: %s" % pathname)
            try:
                iterator = RequestIterator(pathname,
                                           self.args['ComponentDir'] )
                self.iterators[item] = iterator
            except StandardError, ex:
                logging.error("ERROR Loading Workflow: %s : %s" % (item, ex))
                continue
            #  //
            # // try and load event and run count.
            #//
            eventsFile = os.path.join(self.args['WorkflowCache'],
                                      "%s.events" % item)
            counterFile = os.path.join(self.args['WorkflowCache'],
                                       "%s.counter" % item)
            eventsValue = readIntFromFile(eventsFile)
            counterValue = readIntFromFile(counterFile)
            logging.debug("EventCounter for workflow %s = %s" % (
                item, eventsValue)
                          )
            logging.debug("RunCounter for workflow %s = %s" % (
                item, counterValue)
                          )
            if eventsValue != None:
                iterator.eventsPerJob = eventsValue

            if counterValue != None:
                iterator.count = counterValue
                
        return
    
            

    def startComponent(self):
        """
        _startComponent_
        
        Start the servers required for this component

        """                                   
        # create message service
        self.ms = MessageService()
                                                                                
        # register
        self.ms.registerAs("RequestInjector")
                                                                                
        # subscribe to messages
        self.ms.subscribeTo("ResourcesAvailable")
        self.ms.subscribeTo("RequestInjector:SetWorkflow")
        self.ms.subscribeTo("RequestInjector:LoadWorkflows")
        self.ms.subscribeTo("RequestInjector:SelectWorkflow")
        self.ms.subscribeTo("RequestInjector:NewDataset")
        self.ms.subscribeTo("RequestInjector:SetEventsPerJob")
        self.ms.subscribeTo("RequestInjector:SetInitialRun")
        self.ms.subscribeTo("RequestInjector:StartDebug")
        self.ms.subscribeTo("RequestInjector:EndDebug")
        
        # wait for messages
        while True:
            msgtype, payload = self.ms.get()
            self.ms.commit()
            logging.debug("ReqInjector: %s, %s" % (msgtype, payload))
            self.__call__(msgtype, payload)



def readIntFromFile(filename):
    """
    _readIntFromFile_

    util to extract an int from a file

    """
    if not os.path.exists(filename):
        return None
    content = file(filename).read()
    content = content.strip()
    try:
        return int(content)
    except ValueError:
        return None
    
