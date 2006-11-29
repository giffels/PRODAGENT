#!/usr/bin/env python
"""
_CondorTrackingComponent_

Component that watches Condor and Job Caches to determine wether jobs have
completed sucessfully, or failed

"""
import os
import time
import logging
from logging.handlers import RotatingFileHandler


from MessageService.MessageService import MessageService
from ProdAgentCore.Configuration import loadProdAgentConfiguration
from ProdAgentCore.LoggingUtils import installLogHandler
from ProdAgentCore.ProdAgentException import ProdAgentException

from FwkJobRep.ReportState import checkSuccess
from FwkJobRep.FwkJobReport import FwkJobReport

import JobState.JobStateAPI.JobStateInfoAPI as JobStateInfoAPI
import JobState.JobStateAPI.JobStateChangeAPI as JobStateChangeAPI

from CondorTracker.Registry import retrieveTracker
import  CondorTracker.CondorTrackerDB as TrackerDB
import CondorTracker.Trackers

class CondorTrackerComponent:
    """
    _CondorTrackerComponent_

    ProdAgent component that polls looking for completed condor jobs

    """
    def __init__(self, **args):
        self.args = {}
        self.args['Logfile'] = None
        self.args['TrackerPlugin'] = None
        self.args['PollInterval'] = "00:01:00"
        self.args.update(args)
        


        
        if self.args['Logfile'] == None:
            self.args['Logfile'] = os.path.join(self.args['ComponentDir'],
                                                "ComponentLog")

        installLogHandler(self)
        msg = "CondorTracker Started\n"
        msg += " ==> Tracker Plugin: %s\n" % self.args['TrackerPlugin']
        msg += " ==> Poll Interval: %s\n" % self.args['PollInterval']
        logging.info(msg)
        
        
    def __call__(self, event, payload):
        """
        _operator()_

        Define response to an Event and payload

        """
        logging.debug("Recieved Event: %s" % event)
        logging.debug("Payload: %s" % payload)
        
        if event == "SubmitJob":
            self.jobSubmitted(payload)
            return

        if event == "SubmissionFailed":
            self.submitFailed(payload)
            return

        if event == "CondorTracker:Update":
            self.update()
            return
        
        


        #  //
        # // Control Stuff
        #//
        if event == "CondorTracker:SetTracker":
            self.args['TrackerPlugin'] = payload
            return
        if event == "CondorTracker:StartDebug":
            logging.getLogger().setLevel(logging.DEBUG)
            return
        if event == "CondorTracker:EndDebug":
            logging.getLogger().setLevel(logging.INFO)
            return

        
    def jobSubmitted(self, jobSpecId):
        """
        _jobSubmitted_

        Start watching for submitted job

        """
        logging.info("--> Start Watching: %s" % jobSpecId)
        TrackerDB.submitJob(jobSpecId)
        return
    

    def submitFailed(self, jobSpecId):
        """
        _submitFailed_

        Stop tracking job

        """
        logging.info("--> Stop Watching: %s" % jobSpecId)
        TrackerDB.removeJob(jobSpecId)

    def update(self):
        """
        _update_

        Update the state of all jobs

        """
        try:
            tracker = retrieveTracker(self.args['TrackerPlugin'])
        except Exception, ex:
            msg = "Unable to Retrieve Tracker Plugin named: %s\n" % (
                self.args['TrackerPlugin'])
            msg += "Cant perform update..."
            logging.error(msg)
            return
        
        try:
            tracker()
        except Exception, ex:
            msg = "Error invoking Tracker Plugin: %s\n" % (
                self.args['TrackerPlugin'],
                )
            msg += str(ex)
            logging.error(msg)
            return

        #  //
        # // Trawl list of completed jobs and publish events
        #//  for them
        completeJobs = TrackerDB.getJobsByState("complete")
        for jobspec, jobindex in completeJobs.items():
            self.jobCompletion(jobspec)
            TrackerDB.removeJob(jobspec)
            logging.info("--> Stop Watching: %s" % jobspec)

        self.ms.publish("CondorTracker:Update", "", self.args['PollInterval'])
        self.ms.commit()
        return

    

    def jobCompletion(self, jobSpecId):
        """
        _jobCompletion_

        Handle a completion for the jobSpecId provided

        """
        try:
            jobState = JobStateInfoAPI.general(jobSpecId)
            jobCache = jobState['CacheDirLocation']
        except ProdAgentException, ex:
            msg = "Unable to Publish Report for %s\n" % jobSpecId
            msg += "Since It is not known to the JobState System:\n"
            msg += str(ex)
            logging.error(msg)
            return
        jobReport = "%s/FrameworkJobReport.xml" % jobCache
        if not os.path.exists(jobReport):
            logging.info("Missing Report for %s" % jobSpecId)
            badReport = FwkJobReport(jobSpecId)
            badReport.status = "Failed"
            badReport.exitCode = 999
            err = badReport.addError(999, "MissingJobReport")
            errDesc = "Framework Job Report was not returned "
            errDesc += "on completion of job"
            err['Description'] = errDesc
            badReport.write(jobReport)
            self.ms.publish("JobFailed" ,jobReport)
            self.ms.commit()
            logging.info("JobFailed Published For %s" % jobSpecId)
            return
        if checkSuccess(jobReport):
            self.ms.publish("JobSuccess", jobReport)
            self.ms.commit()
            logging.info("JobSuccess Published For %s" % jobSpecId)
            return
        else:
            self.ms.publish("JobFailed" ,jobReport)
            self.ms.commit()
            logging.info("JobFailed Published For %s" % jobSpecId)
            return
        
        

    def startComponent(self):
        """
        _startComponent_

        Start component, subscribe to messages and start polling thread

        """
       
        # create message server
        self.ms = MessageService()
                                                             
        # register
        self.ms.registerAs("CondorTracker")
        self.ms.subscribeTo("CondorTracker:StartDebug")
        self.ms.subscribeTo("CondorTracker:EndDebug")
        self.ms.subscribeTo("CondorTracker:Update")
        self.ms.subscribeTo("CondorTracker:SetTracker")
        self.ms.subscribeTo("SubmitJob")
        self.ms.subscribeTo("SubmissionFailed")
        
        
        # wait for messages
        while True:
            type, payload = self.ms.get()
            self.ms.commit()
            logging.debug("CondorTracker: %s, %s" % (type, payload))
            self.__call__(type, payload)
            
    
        
