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
__version__ = "$Revision: 1.3 $"
__revision__ = "$Id: JobSubmitterComponent.py,v 1.3 2006/05/02 14:25:25 evansde Exp $"

import os
import logging
from logging.handlers import RotatingFileHandler


from MessageService.MessageService import MessageService

from JobSubmitter.Registry import retrieveSubmitter
from MCPayloads.JobSpec import JobSpec
from JobState.JobStateAPI import JobStateChangeAPI
from JobState.JobStateAPI import JobStateInfoAPI
from ProdAgentCore.ProdAgentException import ProdAgentException

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

        
            
        logHandler = RotatingFileHandler(self.args['Logfile'],
                                         "a", 1000000, 3)
        logFormatter = logging.Formatter("%(asctime)s:%(message)s")
        logHandler.setFormatter(logFormatter)
        logging.getLogger().addHandler(logHandler)
        logging.getLogger().setLevel(logging.INFO)
        logging.info("JobSubmitter Component Started...")
        
        

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
        jobSpecId = payload
        jobCache = JobStateInfoAPI.general(jobSpecId)['CacheDirLocation']
        
        jobToSubmit = os.path.join(jobCache, jobSpecId)
        
        
        jobSpecFile = os.path.join(jobCache, "%s-JobSpec.xml" % jobSpecId)

        
        logging.debug("JobSpecID=%s" % jobSpecId)
        logging.debug("JobCache=%s" % jobCache)
        logging.debug("JobToSubmit=%s" % jobToSubmit)
        logging.debug("JobSpecFile=%s" % jobSpecFile)

        try:
            jobSpecInstance = JobSpec()
            jobSpecInstance.load(jobSpecFile)
            #TEST ErrorHandler Comment Above, Uncomment below:
            #jobSpecInstance.load(jobSpecFile+"generate_error")
        except StandardError, ex:
            msg = "Failed to read JobSpec File for Job %s\n" % jobSpecId
            msg += "From: %s\n" % jobSpecFile
            msg += str(ex)
            logging.error(msg)
            self.ms.publish("SubmissionFailed", jobSpecId)
            self.ms.commit()
            return

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

        #  //
        # // Retrieve the submitter plugin and invoke it
        #//
        submitter = retrieveSubmitter(self.args['SubmitterName'])
        try:
            submitter(
                jobCache,
                jobToSubmit, jobSpecId,
                JobSpecInstance = jobSpecInstance
                )
        except ProdAgentException, ex:
            msg = "Submission Failed for job %s\n" % jobSpecId
            msg += str(ex)
            logging.error(msg)
            self.ms.publish("SubmissionFailed", jobSpecId)
            self.ms.commit()
            return
        #  //
        # // Publish Successful submission 
        #//
        if self.job_state:
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
        
        self.ms.publish("JobSubmitted", jobSpecId)
        self.ms.commit()
        return
 
        

        

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

