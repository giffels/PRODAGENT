#!/usr/bin/env python
"""
_MergeAccountant_

Component that deals with success and failures of merge jobs, keeping
input and output file accounting. 

"""

__revision__ = "$Id$"
__version__ = "$Revision$"
__author__ = "Carlos.Kavka@ts.infn.it"

import os

# Merge sensor import
#from MergeSensor.MergeSensorError import MergeSensorError, \
#                                         InvalidDataTier, \
#                                         InvalidDataset, \
#                                         DatasetNotInDatabase
from MergeSensor.MergeSensorDB import MergeSensorDB

# Message service import
from MessageService.MessageService import MessageService

# Job report
from FwkJobRep.ReportParser import readJobReport

# logging
import logging
from logging.handlers import RotatingFileHandler

# trigger api
from Trigger.TriggerAPI.TriggerAPI import TriggerAPI 

# ProdAgent exception
from ProdAgentCore.ProdAgentException import ProdAgentException

##############################################################################
# MergeAccountantComponent class
##############################################################################
                                                                                
class MergeAccountantComponent:
    """
    _MergeAccountantComponent_

    Component that deals with success and failures of merge jobs, keeping
    input and output file accounting. 

    """

    ##########################################################################
    # MergeAccountant component initialization
    ##########################################################################

    def __init__(self, **args):
        """
        
        Arguments:
        
          args -- all arguments from StartComponent.
          
        Return:
            
          none

        """
        
        # initialize the server
        self.args = {}
        self.args.setdefault("ComponentDir", None)
        self.args.setdefault("Logfile", None)
        self.args.setdefault("Enabled", "yes")
        self.args.setdefault("MaxInputAccessFailures", 1)        
        
        # update parameters
        self.args.update(args)

        # update Enabled parameter
        self.args['Enabled'] = str(self.args['Enabled']).lower()
        
        if self.args['Enabled'] == "none" or \
           self.args['Enabled'] == "y" or \
           self.args['Enabled'] == "yes":
            self.enabled = True
        else:
            self.enabled = False
            

        # define log file
        if self.args['Logfile'] == None:
            self.args['Logfile'] = os.path.join(self.args['ComponentDir'], 
                                                "ComponentLog")
        # create log handler
        logHandler = RotatingFileHandler(self.args['Logfile'],
                                         "a", 1000000, 3)

        # define log format
        logFormatter = logging.Formatter("%(asctime)s:%(message)s")
        logHandler.setFormatter(logFormatter)
        logging.getLogger().addHandler(logHandler)
        logging.getLogger().setLevel(logging.INFO)

        # inital log information
        logging.info("MergeAccountant starting...")
        if self.enabled:
            status = "enabled"
        else:
            status = "disabled"
        logging.info("File accounting is " + status + ".")
                
        # database connection not initialized
        self.database = None
        
        # message service instance
        self.ms = None 
        
        # trigger
        self.trigger = None
        
    ##########################################################################
    # handle events
    ##########################################################################

    def __call__(self, event, payload):
        """
        _operator()_

        Used as callback to handle events that have been subscribed to

        Arguments:
            
          event -- the event name
          payload -- its arguments
          
        Return:
            
          none
          
        """
        
        logging.debug("Received Event: %s" % event)
        logging.debug("Payload: %s" % payload)

        # start debug event
        if event == "MergeAccountant:StartDebug":
            logging.getLogger().setLevel(logging.DEBUG)
            return

        # stop debug event
        if event == "MergeAccountant:EndDebug":
            logging.getLogger().setLevel(logging.INFO)
            return

        # enable event
        if event == "MergeAccountant:Enable":
            self.enable = True
            return

        # disable event
        if event == "MergeAccountant:Disable":
            self.enable = False
            return

        # a job has finished
        if event == "JobSuccess":
            try:
                self.jobSuccess(payload)
            except Exception, msg:
                logging.error("Unexpected error when handling a " + \
                              "JobSuccess event: " + msg)
            return

        # a job has failed
        if event == "GeneralJobFailure":
            try:
                self.jobFailed(payload)
            except Exception, msg:
                logging.error("Unexpected error when handling a " + \
                              "GeneralFailure event: " + str(msg))
            return

        # wrong event
        logging.debug("Unexpected event %s, ignored" % event)

    ##########################################################################
    # handle a JobSuccess event
    ##########################################################################

    def jobSuccess(self, jobReport):
        """
        _jobSuccess_

        A job has finished successfully. Non merge jobs are ignored.

        If it is complete success, all input files are marked as 'merged'
        and the output file as 'merged'.

        JobSuccess for partial merge are not implemented yet!. When it
        will be implemented, its behavior will be as follows:

        It it is a partial success, merged input files are marked as
        'merged', output file as 'partiallymerged', input files with
        problems as 'unmerged' (which increments automatically their
        failures counter). If limit of failures is reached, wrong
        input files are tagged as 'invalid'

        Arguments:

          jobReport -- the job report file name

        Return:

          none

        """

        # remove file:// from file name (if any)
        jobReport = jobReport.replace('file://','')

        # verify the file exists
        if not os.path.exists(jobReport):
            logging.error("Cannot process JobSuccess event: " \
                         + "job report %s does not exist." % jobReport)
            return

        # read the report
        try:
            reports = readJobReport(jobReport)

        # check errors
        except Exception, msg:
            logging.error("Cannot process JobSuccess event for %s: %s" \
                           % (jobReport, msg))
            return

        # get job name from first report
        try:
            jobName = reports[0].jobSpecId

        # if cannot be done, signal error
        except Exception, msg:

            logging.error("Cannot process JobSuccess event for %s: %s" \
                          % (jobReport, msg))
            return

        # files can be cleaned up now
        logging.info("trigger cleanup for: %s" % jobName)
        
        try:
            self.trigger.setFlag("cleanup", jobName, "MergeAccountant")
        except ProdAgentException, ex:
            logging.error("trying to continue processing success event")
            
        # ignore non merge jobs
        if jobName.find('mergejob') == -1:
            logging.debug("Ignoring job %s, since it is not a merge job" \
                          % jobName)
            return

        # verify enable condition
        if not self.enabled:
            return
        
        # open a DB connection 
        database = MergeSensorDB()

        # start a transaction
        database.startTransaction()

        # get job information
        try:
            jobInfo = database.getJobInfo(jobName)

        # cannot get it!
        except Exception, msg:
            logging.error("Cannot process JobSuccess event for job %s: %s" \
                  % (jobName, msg))
            database.closeDatabaseConnection()
            return

        # check that job exists
        if jobInfo is None:
            logging.error("Job %s does not exists." % jobName)
            database.closeDatabaseConnection()
            return

        # check status
        if jobInfo['status'] != 'undermerge':
            logging.error("Cannot process JobSuccess event for job %s: %s" \
                  % (jobName, "the job is not currently running"))
            database.closeDatabaseConnection()
            return

        # get dataset id
        datasetId = database.getDatasetId(jobInfo['datasetName'])

        # mark all input files as 'merged'
        for fileName in jobInfo['inputFiles']:
            database.updateInputFile(datasetId, fileName, status="merged")

        # mark output file as 'merged'
        database.updateOutputFile(datasetId, jobName=jobName, status='merged')

        # commit changes
        database.commit()

        # log message
        logging.info("Job %s finished succesfully, file information updated." \
                     % jobName)

        # close connection
        database.closeDatabaseConnection()

    ##########################################################################
    # handle a general failure job event
    ##########################################################################

    def jobFailed(self, jobName):
        """
        _jobFailed_

        A job has failed. Non merge jobs are ignored.

        Since it is a general failure, the error handler has tried to submit
        the job a number of times and it always failed. Mark then all input
        files as 'unmerged' (which will increment their failures counter)
        and the output file as 'failed'. If limit of failures in input files
        is reached, they are tagged as 'invalid'

        Arguments:

          the job name

        Return:

          none

        """

        # files can be cleaned up now
        logging.info("trigger cleanup for: %s" % jobName)

        try:
            self.trigger.setFlag("cleanup", jobName, "MergeAccountant")
        except ProdAgentException, ex:
            logging.error("trying to continue processing failure event")

        # ignore non merge jobs
        if jobName.find('mergejob') == -1:
            logging.debug("Ignoring job %s, since it is not a merge job" \
                          % jobName)
            return

        # verify enable condition
        if not self.enabled:
            return

        # open a DB connection 
        database = MergeSensorDB()

        # start a transaction
        database.startTransaction()

        # get job information
        try:
            jobInfo = database.getJobInfo(jobName)

        # cannot get it!
        except Exception, msg:
            logging.error("Cannot process Failure event for job %s: %s" \
                  % (jobName, msg))
            database.closeDatabaseConnection()
            return

        # check that job exists
        if jobInfo is None:
            logging.error("Job %s does not exist." % jobName)
            database.closeDatabaseConnection()
            return

        # check status
        if jobInfo['status'] != 'undermerge':
            logging.error("Cannot process Failure event for job %s: %s" \
                  % (jobName, "the job is not currently running"))
            database.closeDatabaseConnection()

            return

        # get dataset id
        datasetId = database.getDatasetId(jobInfo['datasetName'])

        # mark all input files as 'unmerged'
        for fileName in jobInfo['inputFiles']:
            database.updateInputFile(datasetId, fileName, status="unmerged", \
                         maxAttempts=int(self.args['MaxInputAccessFailures']))

        # mark output file as 'merged'
        database.updateOutputFile(datasetId, jobName=jobName, status='failed')

        # commit changes
        database.commit()

        # log message
        logging.info("Job %s failed, file information updated." % jobName)

        # close connection
        database.closeDatabaseConnection()


    ##########################################################################
    # start component execution
    ##########################################################################

    def startComponent(self):
        """
        _startComponent_

        Fire up the two main threads
        
        Arguments:
        
          none
          
        Return:
        
          none

        """
       
        # create message service instance
        self.ms = MessageService()
        
        # register
        self.ms.registerAs("MergeAccountant")
        
        # subscribe to messages
        self.ms.subscribeTo("MergeAccountant:StartDebug")
        self.ms.subscribeTo("MergeAccountant:EndDebug")
        self.ms.subscribeTo("MergeAccountant:Enable")
        self.ms.subscribeTo("MergeAccountant:Disable")
        self.ms.subscribeTo("JobSuccess")
        self.ms.subscribeTo("GeneralJobFailure")
       
        # set trigger access for cleanup
        self.trigger = TriggerAPI(self.ms) 
        
        # wait for messages
        while True:
            messageType, payload = self.ms.get()
            self.ms.commit()
            self.__call__(messageType, payload)
        
    ##########################################################################
    # get version information
    ##########################################################################

    @classmethod
    def getVersionInfo(cls):
        """
        _getVersionInfo_
        
        return version information
        """
        
        return __version__  + "\n"
    
