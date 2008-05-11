#!/usr/bin/env python
"""
_StatTrackerComponent_

ProdAgent Component that records job information from the job report in a DB
so that stats can be compiled.

Subscribes to JobSuccess/JobFailure messages and parses the job report,
and inserts the data into tables in the ProdAgentDB.

"""
__version__ = "$Revision: 1.4 $"
__revision__ = "$Id: StatTrackerComponent.py,v 1.4 2007/06/22 19:36:46 fvlingen Exp $"
__author__ = "evansde@fnal.gov"


from logging.handlers import RotatingFileHandler
import os
import logging
import time

from ProdCommon.Database import Session

from FwkJobRep.ReportParser import readJobReport
from MessageService.MessageService import MessageService
from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdAgent.Trigger.Trigger import Trigger
from StatTracker.JobStatistics import jobReportToJobStats

class StatTrackerComponent:
    """
    _StatTrackerComponent_

    Component that stores job report info in DB tables for easy searching

    """
    def __init__(self, **args):
        self.args = {}
        self.args['ComponentDir'] = None
        self.args['Logfile'] = None
        self.args.update(args)
        
        if self.args['Logfile'] == None:
            self.args['Logfile'] = os.path.join(self.args['ComponentDir'],
                                                "ComponentLog")
        logHandler = RotatingFileHandler(self.args['Logfile'],
                                         "a", 1000000, 3)
        logFormatter = logging.Formatter("%(asctime)s:%(message)s")
        logHandler.setFormatter(logFormatter)
        logging.getLogger().addHandler(logHandler)
        logging.getLogger().setLevel(logging.INFO)
        self.ms = None
        logging.info("StatTracker Component Started")
        
    def __call__(self, event, payload):
        """
        _operator()_

        Define call for this object to allow it to handle events that
        it is subscribed to
        """
        logging.debug("Event: %s Payload: %s" % (event, payload))
        if event == "StatTracker:StartDebug":
            logging.getLogger().setLevel(logging.DEBUG)
            return
        if event == "StatTracker:EndDebug":
            logging.getLogger().setLevel(logging.INFO)
            return
        if event == "JobSuccess":
            self.extractStats(payload)
            return

        if event == "JobFailed":
            self.extractStats(payload)
            return
        
        
        return


    def extractStats(self, jobReportFile):
        """
        _extractStats_

        parse the report, convert it into JobStatistics objects and
        insert them into the DB

        """
        try:
            reports = readJobReport(jobReportFile)
        except StandardError, ex:
            msg = "Error loading Job Report: %s\n" % jobReportFile
            msg += "Stats for this job not recorded"
            logging.error(msg)
            return

        logging.debug("ReportFile: %s" % jobReportFile)
        logging.debug("Contains: %s reports" % len(reports))
        for report in reports:
            logging.debug("Job Report was Success: %s" % report.wasSuccess())
            try:
                stats = jobReportToJobStats(report)
                logging.debug(str(stats))
                stats.insertIntoDB()
                logging.debug("Insert complete")
                #  //
                # // if report is a success, we also set the trigger
                #//  to say we have finished with it.
                if report.wasSuccess():
                    try:
                        self.trigger.setFlag("cleanup",
                                             report.jobSpecId,
                                             "StatTracker")
                    except Exception, ex:
                        msg = "Error setting cleanup flag for job: %s" % report.jobSpecId
                        msg += str(ex)
                        logging.error(msg)
                        
            except StandardError, ex:
                msg = "Error inserting Stats into DB for report:\n"
                msg += str(ex)
                logging.error(msg)
        return
        
            

    def startComponent(self):
        """
        _startComponent_
        
        Start the servers required for this component

        """                                   
        # create message service
        self.ms = MessageService()
        self.trigger=Trigger(self.ms)                                                             
        # register
        self.ms.registerAs("StatTracker")
        
        # subscribe to messages
        
        self.ms.subscribeTo("StatTracker:StartDebug")
        self.ms.subscribeTo("StatTracker:EndDebug")
        self.ms.subscribeTo("JobSuccess")
        self.ms.subscribeTo("JobFailed")
        self.ms.subscribeTo("SetJobCleanupFlag")
        
        # wait for messages
        while True:
            msgtype, payload = self.ms.get()
            self.ms.commit()
            Session.set_database(dbConfig)
            Session.connect()
            Session.start_transaction()
            self.__call__(msgtype, payload)
            Session.commit_all()
            Session.close_all()



