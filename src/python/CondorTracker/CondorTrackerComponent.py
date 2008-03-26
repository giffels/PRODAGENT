#!/usr/bin/env python
"""
_CondorTrackingComponent_

Component that watches Condor and Job Caches to determine wether jobs have
completed sucessfully, or failed

"""
import os
import time
import logging
import traceback

from logging.handlers import RotatingFileHandler

from ProdCommon.Database import Session
from ProdCommon.MCPayloads.JobSpec import JobSpec

from ProdCommon.FwkJobRep.ReportState import checkSuccess
from ProdCommon.FwkJobRep.FwkJobReport import FwkJobReport
from ProdCommon.FwkJobRep.ReportParser import readJobReport
from MessageService.MessageService import MessageService
from ProdAgentCore.Configuration import loadProdAgentConfiguration
from ProdAgentCore.LoggingUtils import installLogHandler
from ProdAgentCore.ProdAgentException import ProdAgentException
from ProdAgent.WorkflowEntities import JobState
from ProdAgent.WorkflowEntities import Job as WEJob
from ProdAgentDB.Config import defaultConfig as dbConfig

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
        self.args['TrackOnEvent'] = "SubmitJob"
        self.args.update(args)
        

     
        if self.args['Logfile'] == None:
            self.args['Logfile'] = os.path.join(self.args['ComponentDir'],
                                                "ComponentLog")

        installLogHandler(self)
        msg = "CondorTracker Started\n"
        msg += " ==> Tracker Plugin: %s\n" % self.args['TrackerPlugin']
        msg += " ==> Poll Interval: %s\n" % self.args['PollInterval']
        msg += " ==> Tracking starts on %s Event\n" % self.args['TrackOnEvent']
        logging.info(msg)

        if self.args['TrackOnEvent'] not in ("SubmitJob", "TrackJob"):
            msg = "Error: TrackOnEvent argument must be one of:\n"
            msg += "\'SubmitJob\' to track jobs when a SubmitJob "
            msg += "event is published\n"
            msg += "\'TrackJob\' to track jobs when a TrackJob "
            msg += "event is published\n"
            logging.error(msg)
            raise RuntimeError(msg)
        
        
    def __call__(self, event, payload):
        """
        _operator()_

        Define response to an Event and payload

        """
        logging.debug("Recieved Event: %s" % event)
        logging.debug("Payload: %s" % payload)
        
        if event == self.args['TrackOnEvent']:
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

        
    def jobSubmitted(self, payload):
        """
        _jobSubmitted_

        Start watching for submitted job

        """
        if self.args['TrackOnEvent'] == "SubmitJob":
            try:
                spec = JobSpec()
                spec.load(payload)
            except Exception, ex:
                msg = "Unable to read JobSpec File: %s" % payload
                logging.error(msg)
                return
            
            specList = []
            if spec.isBulkSpec():
                specList.extend(spec.bulkSpecs.keys())
            else:
                specList.append(spec.parameters['JobName'])

        else:
            specList = [payload]

            
        for item in specList:
            logging.info("--> Start Watching: %s" % item)
            TrackerDB.submitJob(item)
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
            msg += "\n"
            msg += traceback.format_exc()
            logging.error(msg)
            return

        #  //
        # // Trawl list of completed jobs and publish events
        #//  for them. Note that completed means they succeeded as
        #  //jobs, the report must still be checked for success
        # //
        #//
        completeJobs = TrackerDB.getJobsByState("complete")
        for jobspec, jobindex in completeJobs.items():
            self.jobCompletion(jobspec)
            TrackerDB.removeJob(jobspec)
            logging.info("--> Stop Watching: %s" % jobspec)

        #  //
        # // Trawl list of failed jobs and publish events for them.
        #//  Failed here means a failure in middleware/batch system
        failedJobs = TrackerDB.getJobsByState("failed")
        for jobspec, jobindex in failedJobs.items():
            self.jobFailure(jobspec)
            TrackerDB.removeJob(jobspec)
            logging.info("--> Stop Watching: %s" % jobspec)
            
        self.ms.publishUnique("CondorTracker:Update", "", self.args['PollInterval'])
        self.ms.commit()
        return

    

    def jobCompletion(self, jobSpecId):
        """
        _jobCompletion_

        Handle a completion for the jobSpecId provided

        """
        try:
            jobState = WEJob.get(jobSpecId)
            jobCache = jobState['cache_dir']
        except ProdAgentException, ex:
            msg = "Unable to Publish Report for %s\n" % jobSpecId
            msg += "Since It is not known to the JobState System:\n"
            msg += str(ex)
            logging.error(msg)
            return
        except Exception, ex:
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

            # The write will fail if the jobCache directory is missing.  In
            # case just bail out.  Publishing a JobFailed message is useless
            # because the JobReport doesn't exist.
            try:
                badReport.write(jobReport)
            except IOError:
                logging.error("jobCache directory is missing: %s" % jobCache)
                return
                
            self.ms.publish("JobFailed" ,jobReport)
            self.ms.commit()
            #WEJob.registerFailure(jobSpecId, "run")
            #Session.commit_all()
            logging.info("JobFailed Published For %s" % jobSpecId)
            return
        if checkSuccess(jobReport):
            WEJob.setState(jobSpecId, "finished")
            self.ms.publish("JobSuccess", jobReport)
            self.ms.commit()
            Session.commit_all()
            logging.info("JobSuccess Published For %s" % jobSpecId)
            return
        else:
            self.ms.publish("JobFailed" ,jobReport)
            self.ms.commit()
            #WEJob.registerFailure(jobSpecId, "run")
            #Session.commit_all()
            logging.info("JobFailed Published For %s" % jobSpecId)
            return

    def jobFailure(self, jobSpecId):
        """
        _jobFailure_

        Handle a job that failed in middleware/batch
        for the jobSpecId provided

        """
        try:
            jobState = WEJob.get(jobSpecId)
            jobCache = jobState['cache_dir']
        except ProdAgentException, ex:
            msg = "Unable to Publish Report for %s\n" % jobSpecId
            msg += "Since It is not known to the JobState System:\n"
            msg += str(ex)
            logging.error(msg)
            return
        except Exception, ex:
            msg = "Unable to Publish Report for %s\n" % jobSpecId
            msg += "Since It is not known to the JobState System:\n"
            msg += str(ex)
            logging.error(msg)
            return

        jobReport = "%s/FrameworkJobReport.xml" % jobCache
        logging.info("Creating Failure Report for %s" % jobSpecId)
        badReport = FwkJobReport(jobSpecId)
        badReport.jobSpecId = jobSpecId
        badReport.jobType = "Processing"
        badReport.status = "Failed"
        badReport.exitCode = 998
        err = badReport.addError(998, "BatchMiddlewareFailure")
        errDesc = "Failure in Batch or Middleware Layer \n"
        errDesc  += "No job report produced/retrieved"
        err['Description'] = errDesc
        badReport.write(jobReport)
        self.ms.publish("JobFailed" ,jobReport)
        self.ms.commit()
        #WEJob.registerFailure(jobSpecId, "run")
        #Session.commit_all()
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
        self.ms.subscribeTo(self.args['TrackOnEvent'])
        self.ms.subscribeTo("SubmissionFailed")

        self.ms.remove("CondorTracker:Update")
        self.ms.publishUnique("CondorTracker:Update", "", self.args['PollInterval'])
        self.ms.commit()
        # wait for messages
        while True:
            Session.set_database(dbConfig)
            Session.connect()
            Session.start_transaction()
            type, payload = self.ms.get()
            self.ms.commit()
            logging.debug("CondorTracker: %s, %s" % (type, payload))
            self.__call__(type, payload)
            Session.commit_all()
            Session.close_all()

            
    
        
