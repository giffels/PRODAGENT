# !/usr/bin/env python
"""
_ProdMonComponent_

ProdAgent Component that records job information from the job report in a DB
so that stats can be compiled.

Subscribes to JobSuccess/JobFailure messages and parses the job report,
and inserts the data into tables in the ProdAgentDB.

Derived from previous StatTracker and Monitoring components

"""
__version__ = "$Revision: 1.13 $"
__revision__ = "$Id: ProdMonComponent.py,v 1.13 2008/09/12 17:14:16 swakef Exp $"
__author__ = "stuart.wakefield@imperial.ac.uk"


import os
import string
import logging
from logging.handlers import RotatingFileHandler
from ProdAgentCore.Configuration import loadProdAgentConfiguration
from MessageService.MessageService import MessageService
from ProdMon.JobStatistics import jobStatsGroupedBySpecId, wasSuccess
from ProdCommon.FwkJobRep.ReportParser import readJobReport
#from Trigger.TriggerAPI.TriggerAPI import TriggerAPI
from ProdCommon.Database import Session
from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdAgent.Trigger.Trigger import Trigger
from ProdMon.DashboardInterface import exportToDashboard
from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec
from ProdMon.ProdMonDB import insertNewWorkflow, deleteOldJobs

class ProdMonComponent:
    """
    _ProdMonComponent_

    Component that stores job report info in DB tables for easy searching

    """
    def __init__(self, **args):
        
        # set default configuration
        self.args = {}
        self.args.setdefault("ComponentDir", None)
        self.args.setdefault("Logfile", None)
        self.args.setdefault("DashboardURL", "http://lxarda16.cern.ch/dashboard/request.py/getPAinfo")
        self.args.setdefault("exportMaxBatchSize", 500)
        self.args.setdefault("exportInterval", "00:05:00")
        self.args.setdefault("Team", "Unknown")
        self.args.setdefault("AgentName", "Unknown")
        self.args.setdefault("exportEnabled", False)
        self.args.setdefault("FailedDir", None)
        self.args.setdefault("expireEnabled", False)
        self.args.setdefault("expireInterval", "01:00:00")
        self.args.setdefault("expireRecordAfter", "96:00:00")
        self.args.setdefault("expireUnexported", False)
        
        
        # override default with provided values and convert to correct types
        try:
            self.args.update(args)
            self.args["exportMaxBatchSize"] = int(self.args["exportMaxBatchSize"])
            for value in ("exportEnabled", "expireEnabled", "expireUnexported"):
                if str(self.args[value]).lower() in ("true", "yes"):
                    self.args[value] = True
                else:
                    self.args[value] = False
            if self.args["FailedDir"] == None:
                self.args["FailedDir"] = os.path.join(self.args['ComponentDir'],
                                                "FailedJobReports")
        except StandardError, ex:
            msg = "Error handling configuration"
            msg += str(ex)
            raise RuntimeError, msg
        
        # find agent name and add to local config
        try:
            config = loadProdAgentConfiguration()
            compCfg = config.getConfig("ProdAgent")
            self.args["AgentName"] = compCfg["ProdAgentName"]
        except StandardError, ex:
            msg = "Error reading ProdAgent name:\n"
            msg += str(ex)
            # logging.error(msg)
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
        
        logging.info("ProdMon Component Started")
        logging.info("with config: %s" % self.args)


    def __call__(self, event, payload):
        """
        _operator()_

        Define call for this object to allow it to handle events that
        it is subscribed to
        """
        logging.debug("Event: %s Payload: %s" % (event, payload))
        if event == "ProdMon:StartDebug":
            logging.getLogger().setLevel(logging.DEBUG)
            return
        elif event == "ProdMon:EndDebug":
            logging.getLogger().setLevel(logging.INFO)
            return
        elif event in ("JobSuccess", "JobFailed"):
            self.extractStats(payload)
            return
        elif event == "ProdMon:StopExport":
            logging.info("Export halted by ProdMon:StopExport message")
            self.ms.remove("ProdMon:Export")
            self.args["exportEnabled"] = False
            return
        elif event == "ProdMon:StartExport":
            self.args["exportEnabled"] = True
            self.ms.remove("ProdMon:Export")
            logging.info("Export started")
            self.export()
            return
        elif event == "ProdMon:Export":
            self.export()
            return
        elif event == "NewWorkflow":
            self.newWorkflow(payload)
            return
        elif event == "ProdMon:RetryFailures":
            self.retryFailures()
        elif event == "ProdMon:ExpireRecords":
            self.expireRecords()
        return
    
    
    def newWorkflow(self, workflowFile):
        """
        _newWorkflow_
        
        parse workflow spec and save workflow and input/output
        datasets
        
        """
        
        # Access the WorkflowSpec file
        workflowFile = string.replace(workflowFile, 'file://', '')
        if not os.path.exists(workflowFile):
            logging.error("Workflow File Not Found: %s" % workflowFile)
            return
        try:
            workflowSpec = WorkflowSpec()
            workflowSpec.load(workflowFile)
        except Exception:
            logging.error("Invalid Workflow File: %s" % workflowFile)
            return

        try:
            inputDatasets = []
            for dataset in workflowSpec.inputDatasets():
                inputDatasets.append(dataset["PrimaryDataset"] + "/" \
                + dataset["ProcessedDataset"] + "/" + dataset["DataTier"])
            outputDatasets = []
            for dataset in workflowSpec.outputDatasets():
                outputDatasets.append(dataset["PrimaryDataset"] + "/" \
                + dataset["ProcessedDataset"] + "/" + dataset["DataTier"])
            
            # TODO: Warning: find out if this is suitable or if a function
            #     should be created for ProdRequestID and app_version
            insertNewWorkflow(workflowSpec.workflowName(), 
                              workflowSpec.parameters.get("ProdRequestID", None),
                              inputDatasets, outputDatasets, 
                              workflowSpec.payload.application.get("Version", None))
        except Exception, ex:
            logging.error("Error inserting workflow : %s", ex)
            return
            
        logging.debug("Workflow saved to DB: %s" % workflowSpec.workflowName())
        return


    def extractStats(self, jobReportFile):
        """
        _extractStats_

        parse the report, convert it into JobStatistics objects and
        insert them into the DB.
        

        """
        result = False
        try:
            fjrs = readJobReport(jobReportFile)
            reportsByInstances = jobStatsGroupedBySpecId(fjrs)
        except StandardError, ex:
            msg = "Error loading Job Report: %s\n" % jobReportFile
            msg += "Stats for this job not recorded"
            logging.error(msg)
            return
        
        logging.debug("ReportFile: %s" % jobReportFile)
        logging.debug("Contains: %s instances" % len(reportsByInstances))
        for reports in reportsByInstances:
            
            report = reports[0]
            logging.debug("Inserting into db %s" % report['job_spec_id'])
            logging.debug("With %s step(s)" % len(reports))
            try:
                try:
                    if report['job_type'] not in ('LogArchive', 'CleanUp'):
                        report.insertIntoDB(*reports[1:])
                        result = True
                    else:
                        # dont want to handle this job so skip it
                        result = True
                except StandardError, ex:
                    # If error on insert save for later retry
                    logging.error("Failed to insert job stats into the db: %s" % str(ex))
                    try:
                        # TODO: change for multi step processing
#                        if not os.path.isdir(self.args["FailedDir"]):
#                            os.mkdir(self.args["FailedDir"])
#                        fjrs.write(os.path.join(self.args["FailedDir"], report.jobSpecId))
                        msg = "Error inserting Stats into DB for report: %s\n" % report['job_spec_id']
                        msg += "report saved to %s\n" % str(self.args["FailedDir"])
                        msg += str(ex)
                        logging.error(msg)
                    except StandardError, ex:
                        logging.error("Unable to save job report to failure area: %s" % str(ex))

            finally:
                #   //
                #  // if report is a success, we also set the trigger
                # //  to allow the cleanup to procede.
                if wasSuccess(*reports):
                    try:
                        self.trigger.setFlag("cleanup",
                                             report['job_spec_id'],
                                             "ProdMon")
                    except Exception, ex:
                        msg = "Error setting cleanup flag for job: %s" % report['job_spec_id']
                        msg += str(ex)
                        logging.error(msg)
                        
        return result
    
    
    def export(self):
        """
        _export()_
        
        export records since the last export to external monitoring
        
        """
        
        if not self.args["exportEnabled"]:
            logging.error("Unable to publish to Dashboard, export disabled")
            return
        
        try:
            exportToDashboard(self.args['exportMaxBatchSize'],
                              self.args['DashboardURL'],
                              self.args["Team"],
                              self.args["AgentName"],
                              self.args["ComponentDir"])
        
        except Exception, ex:
            msg = "Error exporting data to external monitoring: "
            msg += str(ex)
            logging.error(msg)
            
            self.ms.publish("ProdMon:Export", "", self.args['exportInterval'])
        else:
            self.ms.publish("ProdMon:Export", "", self.args['exportInterval'])
        
        return
            

    def retryFailures(self):
        """
        Process list of insert failures and retry"
        """
        logging.info("Processing failed FrameworkJobReport inserts")
        if os.path.isdir(self.args["FailedDir"]):
            for file in os.listdir(self.args["FailedDir"]):
                try:
                    inserted = self.extractStats(os.path.join(self.args["FailedDir"], file))
                    if inserted:
                        os.remove(os.path.join(self.args["FailedDir"], file))
                except Exception, ex:
                    logging.error("Exception in retryFailures %s" % str(ex))
        logging.info("Finished processing failed reports")
        return


    def expireRecords(self):
        """
        remove records for jobs older than self.args['expireInterval']
        only remove exported jobs unless self.args['expireUnexported'] = True
        """
        
        if not self.args["expireEnabled"]:
            logging.error("Unable to expire job records, expire disabled")
            return
        
        try:
            deleteOldJobs(self.args['expireRecordAfter'], self.args['expireUnexported'])
        except Exception, ex:
            logging.error("Error expiring jobs: %s" % str(ex))
        
        self.ms.publishUnique("ProdMon:ExpireRecords", "", self.args['expireInterval'])


    def startComponent(self):
        """
        _startComponent_
        
        Start the servers required for this component

        """                                   
        # create message service
        self.ms = MessageService()
        #self.trigger = TriggerAPI(self.ms)
        self.trigger=Trigger(self.ms)
        # register
        self.ms.registerAs("ProdMon")
        
        # subscribe to messages
        self.ms.subscribeTo("ProdMon:StartDebug")
        self.ms.subscribeTo("ProdMon:EndDebug")
        self.ms.subscribeTo("JobSuccess")
        self.ms.subscribeTo("JobFailed")
        self.ms.subscribeTo("ProdMon:Export")
        self.ms.subscribeTo("ProdMon:StopExport")
        self.ms.subscribeTo("ProdMon:StartExport")
        self.ms.subscribeTo("NewWorkflow")
        self.ms.subscribeTo("ProdMon:RetryFailures")
        self.ms.subscribeTo("ProdMon:ExpireRecords")
        # tell JobCreator to set ProdMon cleanup flag
        self.ms.subscribeTo("SetJobCleanupFlag")
        
        # restart publishing loop
        if self.args["exportEnabled"]:
            self.ms.publishUnique("ProdMon:Export", "", self.args['exportInterval'])
        if self.args["expireEnabled"]:
            self.ms.publishUnique("ProdMon:ExpireRecords", "", self.args['expireInterval'])
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



