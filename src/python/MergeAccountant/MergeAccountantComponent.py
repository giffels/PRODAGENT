#!/usr/bin/env python
"""
_MergeAccountant_

Component that deals with success and failures of merge jobs, keeping
input and output file accounting.

"""

__revision__ = "$Id: MergeAccountantComponent.py,v 1.13 2008/08/21 16:04:52 ahmadh Exp $"
__version__ = "$Revision: 1.13 $"
__author__ = "Carlos.Kavka@ts.infn.it"

import os
import traceback

from MergeSensor.MergeSensorDB.Interface.MergeSensorDB import MergeSensorDB
from MergeSensor.InsertReport import ReportHandler

# Message service import
from MessageService.MessageService import MessageService

# Job report
from ProdCommon.FwkJobRep.ReportParser import readJobReport

# logging
import logging
import ProdAgentCore.LoggingUtils as LoggingUtils

# trigger api
from ProdAgent.Trigger.Trigger import Trigger

# ProdAgent exception
from ProdAgentCore.ProdAgentException import ProdAgentException
from ProdCommon.Core.ProdException import ProdException

# ProdAgent database
from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdCommon.Database import Session

# PM interaction
from ProdAgent.WorkflowEntities import File

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
        LoggingUtils.installLogHandler(self)

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
            self.enabled = True
            return

        # disable event
        if event == "MergeAccountant:Disable":
            self.enabled = False
            return

        # a job has finished
        if event == "JobSuccess":
            try:
                self.jobSuccess(payload)
            except Exception, ex:
                msg = "Unexpected error when handling a "
                msg += "JobSuccess event: " + str(ex)
                msg += traceback.format_exc()
                logging.error(msg)

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



        jobName = None
        try:

        #// Invoke job report handler with jobReport location and flag to enable/disable merge job report handling

            handler = ReportHandler(jobReport, int(self.args['MaxInputAccessFailures']), enableMergeHandling=self.enabled)
            jobName = handler()
        logging.info('this is jobname'+ str(jobName))
        except Exception, ex:
            msg = "Failed to handle job report from job:\n"
            msg += "%s\n" % jobReport
            msg += str(ex)
            msg += "\n"
            msg += traceback.format_exc()
            logging.error(msg)

        #// Failed to read job report
        if jobName is None:
           return

        # files can be cleaned up now
        logging.info("trigger cleanup for: %s" % jobName)

        try:
            self.trigger.setFlag("cleanup", jobName, "MergeAccountant")
        except (ProdAgentException, ProdException):
            logging.error("trying to continue processing success event")




        return #// END jobSuccess



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

        # ignore non merge jobs
        if jobName.find('mergejob') == -1:
            logging.info("Ignoring job %s, since it is not a merge job" \
                          % jobName)
            # Add cleanup flag for non merge jobs too
            logging.info("trigger cleanup for: %s" % jobName)
            try:
                self.trigger.setFlag("cleanup", jobName, "MergeAccountant")
            except (ProdAgentException, ProdException):
                logging.error("trying to continue processing failure event")
            return

        # files can be cleaned up now
        logging.info("trigger cleanup for: %s" % jobName)

        try:
            self.trigger.setFlag("cleanup", jobName, "MergeAccountant")
        except (ProdAgentException, ProdException):
            logging.error("trying to continue processing failure event")

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

        # mark all input files as 'unmerged' (or 'invalid')
        unFinishedFiles = []
        for fileName in jobInfo['inputFiles']:

            # update status
            newStatus = database.updateInputFile(\
                   datasetId, fileName, \
                   status = "unmerged", \
                   maxAttempts = int(self.args['MaxInputAccessFailures']))

            # add invalid files to list of non finished files
            if newStatus == 'invalid':
                unFinishedFiles.append(fileName)

        # mark output file as 'failed'
        database.updateOutputFile(datasetId, jobName=jobName, status='failed')

        # commit changes
        database.commit()

        # notify the PM about the unrecoverable files
        if len(unFinishedFiles) > 0:
            File.merged(unFinishedFiles, True)

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
        self.ms.subscribeTo("MergeAccountant:SetJobCleanupFlag")

        # set trigger access for cleanup
        self.trigger = Trigger(self.ms)

        # set message service instance for PM interaction
        File.ms = self.ms

        # wait for messages
        while True:


            # get message
            messageType, payload = self.ms.get()
            self.ms.commit()
            # create session object
            Session.set_database(dbConfig)
            Session.connect()

            # start transaction
            Session.start_transaction()

            # process it
            self.__call__(messageType, payload)
            self.ms.commit()

            # commit and close session
            Session.commit_all()
            Session.close_all()

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

