#!/usr/bin/env python
"""
_LogArchiver

ProdAgent Component to schedule cleanup jobs to remove
unmerged files

"""

#TODO: Look at replacing cp calls at runtime to links

import logging
import os
import ProdAgentCore.LoggingUtils as LoggingUtils
from MessageService.MessageService import MessageService
from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdCommon.Database import Session
from logging.handlers import RotatingFileHandler

import time
from ProdAgent.Trigger.Trigger import Trigger
from ProdCommon.FwkJobRep.ReportParser import readJobReport

from LogCollector.LogCollectorDB import recordLog, getLogsToArchive, logCollectFailed
import ProdCommon.MCPayloads.LogCollectorTools as LogCollectorTools
from ProdAgentCore.Configuration import prodAgentName
    
class LogCollectorComponent:
    """
    LogCollectorComponent
    """

    def __init__(self, **args):
        self.args = {}
        self.args.setdefault("ComponentDir", None)
        self.args.setdefault("Logfile", None)
        self.args.setdefault("logURL", "srm://srm-cms.cern.ch:8443/srm/managerv2?SFN=/castor/cern.ch/cms")
        #self.args.setdefault("logURL", "srm://gfe02.hep.ph.ic.ac.uk:8443/srm/managerv2?SFN=/pnfs/hep.ph.ic.ac.uk/data/cms")
        self.args.setdefault('logSE', 'gfe02.hep.ph.ic.ac.uk')
        self.args.setdefault("maxLogs", 200)
        self.args.setdefault("pollInterval", "96:00:00")
        self.args.setdefault("logLifetime", "24:00:00")
        self.args.setdefault("maxErrors", 3)
        self.args.setdefault("FailedDir", None)
        self.args.setdefault("Enabled", False)
        self.args.setdefault("LogArchiveSpecs", None)
        self.args.setdefault("QueueJobMode", False)
        self.args.setdefault('logLfnBase', '/store/logs/prod/%s' % prodAgentName())
        
        # override default with provided values and convert to correct types
        try:
            self.args.update(args)
            self.args["maxLogs"] = int(self.args["maxLogs"])
            self.args["maxErrors"] = int(self.args["maxErrors"])
            for arg in ("QueueJobMode", "Enabled"):
                if str(self.args[arg]).lower() in ("true", "yes"):
                    self.args[arg] = True
                else:
                    self.args[arg] = False
            if self.args["FailedDir"] == None:
                self.args["FailedDir"] = os.path.join(self.args['ComponentDir'],
                                                "FailedJobReports")
            if self.args["LogArchiveSpecs"] == None:
                self.args["LogArchiveSpecs"] = os.path.join(self.args['ComponentDir'],
                                                "specs")
            
            
            self.stageOutOverride = {}
            self.stageOutOverride['command'] = 'srmv2'
            self.stageOutOverride['option'] = '-streams_num=1' #seems to be needed by some site firewalls
            self.stageOutOverride['se-name'] = self.args['logSE']
            self.stageOutOverride['lfnPrefix'] = self.args['logURL']
            
            
        except StandardError, ex:
            msg = "Error handling configuration"
            msg += str(ex)
            raise RuntimeError, msg


        # init log file
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
        
        logging.info("LogCollector Component Started")
        logging.info("with config: %s" % self.args)


    def __call__(self, event, payload):
        """
        _operator()_

        Define call for this object to allow it to handle events that
        it is subscribed to
        """
        logging.debug("Event: %s Payload: %s" % (event, payload))
        if event == "LogCollector:StartDebug":
            logging.getLogger().setLevel(logging.DEBUG)
            return
        elif event == "LogCollector:EndDebug":
            logging.getLogger().setLevel(logging.INFO)
            return
        elif event in ("JobSuccess", "JobFailed"):
            self.handleJobReport(payload)
            return
        elif event == "LogCollector:Poll":
            self.poll()
            return
        elif event == "LogCollector:StopPoll":
            self.args['Enabled'] = False
            return
        return
    
    
    def handleJobReport(self, jobReportFile):
        """
        read job report and save log location details to db
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
            try:
                try:
                    if report.jobType == 'LogCollect':
                        # handle log archive - somehow need to do error handling
                        self.parseLogArchiveJobReport(report)
                    # ignore non processing jobs
                    elif not report.jobType == 'CleanUp':
                        self.parseJobReport(report)
                        
                except StandardError, ex:
                    # If error on insert save for later retry
                    try:
                        if not os.path.isdir(self.args["FailedDir"]):
                            os.mkdir(self.args["FailedDir"])
                        report.write(os.path.join(self.args["FailedDir"], report.jobSpecId))
                        msg = "Error inserting log into DB for report: %s\n" % report.jobSpecId
                        msg += "report saved to %s\n" % str(self.args["FailedDir"])
                        msg += str(ex)
                        logging.error(msg)
                    except Exception, ex:
                        logging.error("Unable to save job report to failure area: %s" % str(ex))

            finally:
                #   //
                #  // if report is a success, we also set the trigger
                # //  to allow the cleanup to proceed.
                if report.wasSuccess():
                    try:
                        self.trigger.setFlag("cleanup",
                                             report.jobSpecId,
                                             "LogCollector")
                    except Exception, ex:
                        msg = "Error setting cleanup flag for job: %s" % report.jobSpecId
                        msg += str(ex)
                        logging.error(msg)
                        
    
    def parseLogArchiveJobReport(self, report):
        """
        read a fjr from a logarchive job
        """
        # ignore overall job failures - indicate no files available or error staging out
        if not report.wasSuccess():
            return
            # maybe get staged out log and add to db?
        
        # loop over logs not collected and put back in list for next job
        lfns = [ x['Lfn'] for x in report.skippedFiles ]
        if lfns:
            logCollectFailed(self.args['maxErrors'], lfns)
     
     
    def parseJobReport(self, report):
        """
        read a fjr for a job
        """

        for log, se in report.logFiles.items():
            recordLog(report.workflowSpecId, se, log)  
        return
    
    
    def poll(self):
        """
        look for logs to archive
        """
        logging.debug("look for logs to archive")
        
        toArchive = getLogsToArchive(self.args['logLifetime'])
        
        if not toArchive:
            logging.debug("no un-archived logs found")
            return
        
        for wf, details in toArchive.items():
            
            logCollectorWorkflow = self.createArchiveWF(wf)
            
            for se, logs in details.items():
                logging.info("Found logs to archive for %s at %s" % (wf, se))
                
                # form job specs for this wf/site for max number of input log files
                njobs = len(logs)/self.args['maxLogs']
                lfnBase = "%s/%s/%s/%s" % (self.args['logLfnBase'], time.gmtime()[0], time.gmtime()[1], wf)
                
                if (len(logs) % self.args['maxLogs']) > 0 :               
                    njobs = njobs + 1
               
                ref = 0
               
                for i in range (0, njobs):         
                    #create jobs
                    spec = LogCollectorTools.createLogCollectorJobSpec(\
                                                    logCollectorWorkflow, 
                                                    wf, 
                                                    se,
                                                    lfnBase, 
                                                    self.stageOutOverride,
                                                    *logs[ref:ref+self.args['maxLogs']])
                    jobspec = os.path.join(self.args['LogArchiveSpecs'], \
                                           spec.parameters["JobName"] + ".xml") 
                    spec.save(jobspec)
                    self.publishCreateJob(jobspec)
                    logging.debug('JobSpec %s published' % spec.parameters["JobName"])
                    
                    ref = ref + self.args['maxLogs']
                    
                
                
    
    def createArchiveWF(self, wfName):
        """
        create the workflow
        """
        
        wf = LogCollectorTools.createLogCollectorWorkflowSpec(wfName)    
        
        if not os.path.exists(self.args['LogArchiveSpecs']):
            os.mkdir(self.args['LogArchiveSpecs'])
        
        wfspec = os.path.join(self.args['LogArchiveSpecs'],
                                wf.payload.workflow + '-workflow.xml' )
        wf.save(wfspec)

        self.ms.publish("NewWorkflow", wfspec)
        self.ms.commit()
        return wf
    
    
    def publishCreateJob(self, cleanupSpecURL):
        """
        _publishCreateJob_

        Publish create job event with cleanupSpecURL provided as PAYLOAD

        Arguments:
         cleanupSpecURL -- cleanup specification file name
  
        Return:
         None

        """

        if self.args.get("QueueJobMode", False):
            self.ms.publish("QueueJob", cleanupSpecURL)
        else:
            self.ms.publish("CreateJob", cleanupSpecURL)

        self.ms.commit()

        return
    
    
    def startComponent(self):
        """
        _startComponent_
        
        Start the servers required for this component

        """                                   
        # create message service
        self.ms = MessageService()
        #self.trigger = TriggerAPI(self.ms)
        self.trigger = Trigger(self.ms)
        # register
        self.ms.registerAs("LogCollector")
        
        # subscribe to messages
        self.ms.subscribeTo("LogCollector:StartDebug")
        self.ms.subscribeTo("LogCollector:EndDebug")
        self.ms.subscribeTo("JobSuccess")
        self.ms.subscribeTo("JobFailed")
        self.ms.subscribeTo("LogCollector:Poll")
        self.ms.subscribeTo("LogCollector:StopPoll")
        # tell JobCreator to set LogArchiver cleanup flag
        self.ms.subscribeTo("SetJobCleanupFlag")
        
        # restart publishing loop
        if self.args["Enabled"]:
            self.ms.publishUnique("LogCollector:Poll", "", self.args['pollInterval'])
        self.ms.commit()
        
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



