# !/usr/bin/env python
"""
_ProdMonComponent_

ProdAgent Component that records job information from the job report in a DB
so that stats can be compiled.

Subscribes to JobSuccess/JobFailure messages and parses the job report,
and inserts the data into tables in the ProdAgentDB.

Derived from previous StatTracker and Monitoring components

"""
__version__ = "$Revision: 1.5 $"
__revision__ = "$Id: ProdMonComponent.py,v 1.5 2007/07/25 15:13:23 swakef Exp $"
__author__ = "stuart.wakefield@imperial.ac.uk"


import os
import string
import logging
from logging.handlers import RotatingFileHandler
from ProdAgentCore.Configuration import loadProdAgentConfiguration
from MessageService.MessageService import MessageService
from ProdMon.JobStatistics import jobReportToJobStats
from FwkJobRep.ReportParser import readJobReport
#from Trigger.TriggerAPI.TriggerAPI import TriggerAPI
from ProdCommon.Database import Session
from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdAgent.Trigger.Trigger import Trigger
from ProdMon.DashboardInterface import exportToDashboard
from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec
from ProdMon.ProdMonDB import insertNewWorkflow

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
        
        # override default with provided values and convert to correct types
        try:
            self.args.update(args)
            self.args["exportMaxBatchSize"] = int(self.args["exportMaxBatchSize"])
            if str(self.args["exportEnabled"]).lower() in ("true", "yes"):
                self.args["exportEnabled"] = True
            else:
                self.args["exportEnabled"] = False
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
                              workflowSpec.parameters["ProdRequestID"],
                              inputDatasets, outputDatasets, 
                              workflowSpec.payload.application["Version"])
        except Exception, ex:
            logging.error("Error inserting workflow : %s", ex)
            return
            
        logging.debug("Workflow saved to DB: %s" % workflowSpec.workflowName())
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
            logging.debug("Inserting into db %s" % report.jobSpecId)
            try:
                stats = jobReportToJobStats(report)
                stats.insertIntoDB()
                #   //
                #  // if report is a success, we also set the trigger
                # //  to say we have finished with it.
                if report.wasSuccess():
                    try:
                        self.trigger.setFlag("cleanup",
                                             report.jobSpecId,
                                             "ProdMon")
                    except Exception, ex:
                        msg = "Error setting cleanup flag for job: %s" % report.jobSpecId
                        msg += str(ex)
                        logging.error(msg)

            except StandardError, ex:
                msg = "Error inserting Stats into DB for report: %s\n" % report.jobSpecId
                msg += str(ex)
                logging.error(msg)
                return
        return
    
    
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
        # tell JobCreator to set ProdMon cleanup flag
        self.ms.subscribeTo("SetJobCleanupFlag")
        
        # restart publishing loop
        # replace existing publish messages (if present)
        self.ms.remove("ProdMon:Export")
        if self.args["exportEnabled"]:
            self.ms.publish("ProdMon:Export", "", self.args['exportInterval'])
        
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



