#!/usr/bin/env python
"""
_JobSubmitterComponent_

JobSubmitter component

Events Subscribed To:

- *SubmitJob* Submit a job, payload should be the JobCache area where the job
is stored.

Events Published:

- *JobSubmitted* : 


"""
__version__ = "$Revision: 1.11 $"
__revision__ = "$Id: JobSubmitterComponent.py,v 1.11 2007/04/24 15:23:48 evansde Exp $"

import os
import logging

import ProdAgentCore.LoggingUtils  as LoggingUtils

from MessageService.MessageService import MessageService

from JobSubmitter.Registry import retrieveSubmitter
from ProdCommon.MCPayloads.JobSpec import JobSpec
from JobState.JobStateAPI import JobStateChangeAPI
from JobState.JobStateAPI import JobStateInfoAPI
from ProdAgentCore.ProdAgentException import ProdAgentException
from JobSubmitter.JSException import JSException

class JobSubmitterComponent:
    """
    _JobSubmitterComponent_

    ProdAgent Component that responds to SubmitJob Events

    """
    def __init__(self, **args):
        self.args = {}
        self.args['SubmitterName'] = "noSubmit"
        self.args['Logfile'] = None
        self.args['JobState'] = True
        self.args.update(args)
        self.job_state = self.args['JobState']
        self.ms = None
        if self.args['Logfile'] == None:
            self.args['Logfile'] = os.path.join(self.args['ComponentDir'],
                                                "ComponentLog")

        
            
        LoggingUtils.installLogHandler(self)
        msg = "JobSubmitter Component Started...\n"
        msg += " => SubmitterName: %s\n" % self.args['SubmitterName']
        logging.info(msg)
        
        

    def __call__(self, event, payload):
        """
        _operator()_

        Define response to an Event and payload

        """
        logging.debug("Recieved Event: %s" % event)
        logging.debug("Payload: %s" % payload)
        logging.debug("Current Submitter: %s" % self.args['SubmitterName'])
        
        if event == "SubmitJob":
            logging.info("Submitting Job %s" % payload)
            try:
                self.submitJob(payload)
                return
            except StandardError, ex:
                logging.error("Failed to Submit Job: %s" % payload)
                logging.error("Details: %s" % str(ex))
                return
            
     
        elif event == "JobSubmitter:SetSubmitter":
            #  //
            # // Payload should be name of registered submitter
            #//
            self.setSubmitter(payload)
            logging.debug("Set Submitter: %s" % payload)
            return
        elif event == "JobSubmitter:StartDebug":
            logging.getLogger().setLevel(logging.DEBUG)
            return
        elif event == "JobSubmitter:EndDebug":
            logging.getLogger().setLevel(logging.INFO)
            return
        
        


    def setSubmitter(self, submitterName):
        """
        _setSubmitter_

        Allow dynamic changing of Submitter plugin. Sets the SubmitterName
        to the value provided.

        """
        self.args['SubmitterName'] = submitterName
        return

    def submitJob(self, payload ):
        """
        _createJob_

        Create a processing job based on the jobSpec instance provided

        Payload is expected to be "jobSpecID:jobCache" string initially, this
        should simplify to just the jobSpecID with the JobStates 

        """
        logging.debug("submitJob: %s" % payload)
        jobSpecFile = payload
        try:
            jobSpecInstance = JobSpec()
            jobSpecInstance.load(jobSpecFile)
            #TEST ErrorHandler Comment Above, Uncomment below:
            #jobSpecInstance.load(jobSpecFile+"generate_error")
        except StandardError, ex:
            msg = "Failed to read JobSpec File for Job\n"
            msg += "From: %s\n" % jobSpecFile
            msg += str(ex)
            logging.error(msg)
            self.ms.publish("SubmissionFailed", jobSpecFile)
            self.ms.commit()
            return
        



        if not jobSpecInstance.isBulkSpec():
            logging.debug("Non Bulk Submission")
            jobSpecId = jobSpecInstance.parameters['JobName']
            jobCache = self.checkJobState(jobSpecId)
            msg = "jobSpecId=%s\n" % jobSpecId
            msg += "jobCache=%s\n" % jobCache
            logging.debug(msg)
            if jobCache == None:
                #  //
                # // JobState check failed and published a SubmissionFailed event
                #//  nothing more to do
                return
            jobToSubmit = os.path.join(jobCache, jobSpecId)
            result = self.invokeSubmitter(jobCache, jobToSubmit,
                                          jobSpecId, jobSpecInstance,
                                          { jobSpecId : jobCache }
                                          )
            #  //
            # // Publish Successful submission 
            #//
            if result:
                try:
                    JobStateChangeAPI.submit(jobSpecId)
                except ProdAgentException, ex:
                    # NOTE: this should be stored in the logger
                    # NOTE: we can have different errors here
                    # NOTE: transition, submission, other...
                    # NOTE: and need to take different action for it.
                    msg = "Accessing Job State Failed for job %s\n" % jobSpecId
                    msg += str(ex)
                    logging.error(msg) 
            return
        
        #  //
        # // Still here => Bulk style job spec, need to check all job specs
        #//  with JobStates then invoke submitter on bulk spec.
        usedSpecs = {}
        for specId, specFile in jobSpecInstance.bulkSpecs.items():
            specCache = self.checkJobState(specId)
            if specCache == None:
                msg = "Bulk Spec Problem with JobState for %s\n" % specId
                msg += "Skipping job"
                continue
            usedSpecs[specId] = specCache

        result = self.invokeSubmitter(
            "JobCacheNotUsed", "JobToSubmitNotUsed", "JobSpecIDNotUsed",
            jobSpecInstance, usedSpecs)
        
        if result:
            for specId in usedSpecs.keys():
                try:
                    JobStateChangeAPI.submit(specId)
                except ProdAgentException, ex:
                    # NOTE: this should be stored in the logger
                    # NOTE: we can have different errors here
                    # NOTE: transition, submission, other...
                    # NOTE: and need to take different action for it.
                    msg = "Accessing Job State Failed for job %s\n" % specId
                    msg += str(ex)
                    logging.error(msg) 
        return
 
    def invokeSubmitter(self, jobCache, jobToSubmit, jobSpecId,
                        jobSpecInstance, specToCacheMap = {}):
        """
        _invokeSubmitter_

        Invoke the submission plugin for the spec provided for normal 1-submit jobs
        
        """
        #  //
        # // Retrieve the submitter plugin and invoke it
        #//
        submitter = retrieveSubmitter(self.args['SubmitterName'])
        try:
            submitter(
                jobCache,
                jobToSubmit, jobSpecId,
                JobSpecInstance = jobSpecInstance,
                CacheMap = specToCacheMap
                )
        except JSException, ex:
            if ex.data.has_key("FailureList"):
                for failedId in ex.data['FailureList']:
                    msg = "Submission Failed for job %s\n" % failedId
                    msg += str(ex)
                    logging.error(msg)
                    self.ms.publish("SubmissionFailed", failedId)
                    self.ms.commit()
                return False
            elif ex.data.has_key("mainJobSpecName"):
                failedId = ex.data['mainJobSpecName']
                msg = "Bulk Submission Failed for job %s\n" % failedId
                msg += str(ex)
                logging.error(msg)
                self.ms.publish("SubmissionFailed", failedId)
                self.ms.commit()
                return False
            else:
                msg = "Submission Failed for job %s\n" % jobSpecId
                msg += str(ex)
                logging.error(msg)
                self.ms.publish("SubmissionFailed", jobSpecId)
                self.ms.commit()
                return False
        except ProdAgentException, ex:
            msg = "Submission Failed for job %s\n" % jobSpecId
            msg += str(ex)
            logging.error(msg)
            self.ms.publish("SubmissionFailed", jobSpecId)
            self.ms.commit()
            return False
        self.ms.publish("JobSubmitted", jobSpecId)
        self.ms.commit()
        return True


    def checkJobState(self, jobSpecId):
        """
        _checkJobState_

        Check JobStates DB for jobSpecId prior to submission.

        Check job is resubmittable.

        Return Cache dir, or None, if job shouldnt be submitted

        """
        #  //
        # // Should we actually submit the job?
        #//  The Racers settings in the JobStates DB define how many
        #  //times the same identical job can be submitted in parallel
        # // So we check to see how many jobs have been submitted
        #//  for this JobSpecID, and if there are too many, it doesnt
        #  // get submitted, we send a SubmissionFailed Event
        # //
        #//
        try:
            stateInfo = JobStateInfoAPI.general(jobSpecId)
        except StandardError, ex:
            #  //
            # // Error here means JobSpecID is unknown to 
            #//  JobStates DB.
            msg = "Error retrieving JobState Information for %s\n" % jobSpecId
            msg += "Aborting submitting job...\n"
            msg += str(ex)
            logging.error(msg)
            self.ms.publish("SubmissionFailed", jobSpecId)
            self.ms.commit()
            return
        except ProdAgentException, ex:
            #  //
            # // Error here means JobSpecID is unknown to 
            #//  JobStates DB.
            msg = "Error retrieving JobState Information for %s\n" % jobSpecId
            msg += "Aborting submitting job...\n"
            msg += str(ex)
            logging.error(msg)
            self.ms.publish("SubmissionFailed", jobSpecId)
            self.ms.commit()
            return

        cacheDir = stateInfo.get('CacheDirLocation', 'UnknownCache')
        if not os.path.exists(cacheDir):
            msg = "Cache Dir does not exist for job spec id: %s\n" % jobSpecId
            msg += "JobState reports Cache as:\n   %s\n" % cacheDir
            logging.error(msg)
            self.ms.publish("SubmissionFailed", jobSpecId)
            self.ms.commit()            
            return
            
        numRacers = stateInfo['Racers'] # number of currently submitted
        maxRacers = stateInfo['MaxRacers'] # limit on parallel jobs

        if numRacers >= maxRacers:
            #  //
            # // To many submitted jobs for this JobSpecID already
            #//  Abort submission
            msg = "Too many submitted jobs for JobSpecID: %s\n" % jobSpecId
            msg += "Current Jobs: %s\n" % numRacers
            msg += "Maximum Jobs: %s\n" % maxRacers
            logging.warning(msg)
            self.ms.publish("SubmissionFailed", jobSpecId)
            self.ms.commit()
            return

        return cacheDir
        
        

        

    def startComponent(self):
        """
        _startComponent_

        Start up the component

        """
        
        # create message service
        self.ms = MessageService()
                                                                                
        # register
        self.ms.registerAs("JobSubmitter")
 
        # subscribe to messages
        self.ms.subscribeTo("SubmitJob")
        self.ms.subscribeTo("JobSubmitter:SetSubmitter")
        self.ms.subscribeTo("JobSubmitter:StartDebug")
        self.ms.subscribeTo("JobSubmitter:EndDebug")
 
        # wait for messages
        while True:
            msgtype, payload = self.ms.get()
            self.ms.commit()
            logging.debug("JobSubmitter: %s, %s" % (msgtype, payload))
            self.__call__(msgtype, payload)

