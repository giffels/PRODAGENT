#!/usr/bin/env python
"""
_DBSComponent_

DBSComponent for ProdAgent

ProdAgent Events subscribed to by this Component

NewDataset - A New Dataset has been injected into the ProdAgent system
             via a processing request.
             Event Payload will be the workflow
             Create a new dataset in DBS means :
               - create a primary dataset (if not already there)
               - create a processed dataset (this include inserting algorithm info)

JobSuccess - Job completed, this event will include a ref to a Framework
            job report that contains info about the file (LFN,cksum,size,....)
            Event Payload will be a reference to the JobReport file

"""
import string
import socket

from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec
from ProdCommon.DataMgmt.DBS.DBSWriter import DBSWriter
from ProdCommon.DataMgmt.DBS.DBSErrors import DBSWriterError, formatEx
from DBSAPI.dbsApiException import DbsException

import os,base64,time,exceptions
from FwkJobRep.ReportParser import readJobReport
from MessageService.MessageService import MessageService
from Trigger.TriggerAPI.TriggerAPI import TriggerAPI

import logging
import ProdAgentCore.LoggingUtils  as LoggingUtils

from ProdAgentCore.PluginConfiguration import loadPluginConfig


# disable DBS info      
#logging.disable(logging.INFO)
                                                               
# ##############
class InvalidWorkFlowSpec(exceptions.Exception):
  def __init__(self,workflowfile):
   args="Invalid WorkFlowSpec file: %s\n"%workflowfile
   exceptions.Exception.__init__(self, args)
   pass

  def getClassName(self):
   """ Return class name. """
   return "%s" % (self.__class__.__name__)

  def getErrorMessage(self):
   """ Return exception error. """
   return "%s" % (self.args)

# ##############
class InvalidDataTier(exceptions.Exception):
  def __init__(self,datatier,DBSdatatiers):
   args=" DataTier: => %s <= not supported in DBS.\n Valid Data Tier is a combination of - separated tiers among: %s"%(datatier,DBSdatatiers)
   exceptions.Exception.__init__(self, args)
   pass
                                                                                                                      
  def getClassName(self):
   """ Return class name. """
   return "%s" % (self.__class__.__name__)
                                                                                                                      
  def getErrorMessage(self):
   """ Return exception error. """
   return "%s" % (self.args)

# ##############
class InvalidJobReport(exceptions.Exception):
  def __init__(self,jobreportfile):
   args="Invalid JobReport file: %s\n"%jobreportfile
   exceptions.Exception.__init__(self, args)
   pass
                                                                                
  def getClassName(self):
   """ Return class name. """
   return "%s" % (self.__class__.__name__)
                                                                                
  def getErrorMessage(self):
   """ Return exception error. """
   return "%s" % (self.args)

# ##############
class NoFileBlock(exceptions.Exception):
  def __init__(self,errmsg):
   args= errmsg
   exceptions.Exception.__init__(self, args)
   pass
                                                                                              
  def getClassName(self):
   """ Return class name. """
   return "%s" % (self.__class__.__name__)
                                                                                              
  def getErrorMessage(self):
   """ Return exception error. """
   return "%s" % (self.args)
                                                                                              

# ##############
class DBSComponent:
    """
    _DBSComponent_

    A Threaded Server that makes calls out to the DBS when events are recieved

    """
    def __init__(self, **args):
        self.args = {}

        self.args.setdefault("DBSURL","http://cmssrv18.fnal.gov:8989/DBS/servlet/DBSServlet")
        self.args.setdefault("Logfile", None)
        self.args.setdefault("BadReportfile", None)
        self.args.setdefault("CloseBlockSize", "None")  # No check on fileblock size

        self.args.update(args)

        if self.args['Logfile'] == None:
            self.args['Logfile'] = os.path.join(self.args['ComponentDir'],
                                                "ComponentLog")


# use the LoggingUtils
        LoggingUtils.installLogHandler(self)
        logging.info("DBSComponent Started...")

        #  //
        # // Log Failed FWJobReport registration into DBS
        #//
        if self.args['BadReportfile'] == None:
            self.args['BadReportfile'] = os.path.join(self.args['ComponentDir'],
                                                      "FailedJobReportList.txt")

        self.BadReport = open(self.args['BadReportfile'],'a')
        
        
    def __call__(self, event, payload):
        """
        _operator()_

        Define response to events
        """
        logging.debug("Recieved Event: %s" % event)
        logging.debug("Payload: %s" % payload)
        if event == "NewDataset":
            logging.info("New Dataset Event: %s" % payload)
            try:
                self.newDatasetEvent(payload)
                return
            except InvalidWorkFlowSpec, ex:
                logging.error("Failed to Create New Dataset: %s" % payload)
                logging.error("Details: %s Exception %s" %(ex.getClassName(), ex.getErrorMessage()))
#            except InvalidDataTier, ex:
#                logging.error("Failed to Create New Dataset: %s" % payload)
#                logging.error("Details: %s Exception %s" %(ex.getClassName(), ex.getErrorMessage()))
            except DBSWriterError, ex:
                logging.error("Failed to Create New Dataset: %s" % payload)
            except DbsException, ex:
                logging.error("Failed to Create New Dataset: %s" % payload)
                logging.error("Details: %s"% formatEx(ex))
                return
            except StandardError, ex:
                logging.error("Failed to Create New Dataset: %s" % payload)
                logging.error("Details: %s" % str(ex))
                return


            
        if event == "JobSuccess":
            logging.info("Job Succeeded: %s" % payload)
            try:
                self.handleJobReport(payload)
                return
            except InvalidJobReport, ex:
                logging.error("InvalidJobReport")
                logging.error("Failed to Handle Job Report: %s" % payload)
                logging.error("InvalidJobReport Details: %s Exception %s" %(ex.getClassName(), ex.getErrorMessage()))
                return
            except DBSWriterError, ex:
                logging.error("Failed to Handle Job Report: %s" % payload)
                #logging.error("Details: %s"%ex)
            except InvalidDataTier, ex:
                logging.error("InvalidDataTier")
                logging.error("Failed to Handle Job Report: %s" % payload)
                logging.error("Details: %s Exception %s" %(ex.getClassName(), ex.getErrorMessage()))
            except DbsException, ex:
                logging.error("Failed to Handle Job Report: %s" % payload)
                logging.error("DbsException Details: %s %s" %(ex.getClassName(), ex.getErrorMessage()))
                ## add the FWKJobReport to the failed list
                self.BadReport.write("%s\n" % payload)
                self.BadReport.flush()
                return
            except AssertionError, ex:
                logging.error("Failed to Handle Job Report: %s" % payload)
                logging.error("AssertionError Details: %s" % str(ex))
                return
            except NoFileBlock, ex:
                logging.error("Failed to Handle Job Report: %s" % payload)
                logging.error("Details: %s Exception %s" %(ex.getClassName(), ex.getErrorMessage()))
                ## add the FWKJobReport to the failed list
                self.BadReport.write("%s\n" % payload)
                self.BadReport.flush()
                return
            except StandardError, ex:
                logging.error("Failed to Handle Job Report: %s" % payload)
                logging.error("StandardError Details:%s" % str(ex))
                ## add the FWKJobReport to the failed list
                self.BadReport.write("%s\n" % payload)
                self.BadReport.flush()
                return

        if event == "DBSInterface:RetryFailures":
            logging.info("DBSInterface:RetryFailures Event")
            try:
                self.RetryFailures(self.args['BadReportfile'],self.BadReport)
                return
            except StandardError, ex:
                logging.error("Failed to RetryFailures")
                logging.error("Details: %s" % str(ex))
                return

        if event == "DBSInterface:StartDebug":
            logging.getLogger().setLevel(logging.DEBUG)
            return
        if event == "DBSInterface:EndDebug":
            logging.getLogger().setLevel(logging.INFO)
            return
        
        return

    def newDatasetEvent(self, workflowFile):
        """
        _newDatasetEvent_

        Extract relevant info from the WorkFlowSpecification and loop over Dataset
        """

        logging.debug("DBSComponent.newDatasetEvent: %s" % workflowFile)

        ### Access the WorkflowSpec file
        logging.debug("Reading the NewDataset event payload from WorkFlowSpec: ")
        workflowFile=string.replace(workflowFile,'file://','')
        if not os.path.exists(workflowFile):
            logging.error("Workflow File Not Found: %s" % workflowFile)
            raise InvalidWorkFlowSpec(workflowFile)
        try:
         workflowSpec = WorkflowSpec()
         workflowSpec.load(workflowFile)
        except:
          logging.error("Invalid Workflow File: %s" % workflowFile)
          raise InvalidWorkFlowSpec(workflowFile)
        #  //                                                                      
        # //  Contact DBS using the DBSWriter
        #//
        logging.info("DBSURL %s"%self.args['DBSURL'])
        #dbswriter = DBSWriter('fakeurl') 
        dbswriter = DBSWriter(self.args['DBSURL'])
        #  //
        # //  Create Processing Datsets based on workflow
        #//
        logging.info(">>>>> create Processing Dataset ")
        dbswriter.createDatasets(workflowSpec)
        #  //
        # //  Create Merged Datasets for that workflow as well
        #//
        logging.info(">>>>> create Merged Dataset ")
        dbswriter.createMergeDatasets(workflowSpec)
        return


    def readJobReportInfo(self,jobReportFile):
        """  
        _readJobReportInfo_

        Read the info from jobReport file
          
        """
        jobReportFile=string.replace(jobReportFile,'file://','')
        if not os.path.exists(jobReportFile):
            logging.error("JobReport Not Found: %s" %jobReportFile)
            raise InvalidJobReport(jobReportFile)
        try:
         jobreports=readJobReport(jobReportFile)
        except:
          logging.debug("Invalid JobReport File: %s" %jobReportFile)
          raise InvalidJobReport(jobReportFile) 

        return jobreports


    def handleJobReport(self, jobReportLocation):
        """
        _handleJobReport_

        Retrieve the JobReport from the location provided, read it in,
        extract the details of the datasets and event collections from
        it and inject them into the DBS

        """
        logging.debug("DBSComponent.handleJobReport: %s" % jobReportLocation)


        ### Extract Info from the Job Report
        jobreports=self.readJobReportInfo(jobReportLocation)
        #loop over the fwk jobreports 
        for jobreport in jobreports:
            #print jobreport.files
         #  //
         # //  Contact DBS using the DBSWriter
         #//
         logging.info("DBSURL %s"%self.args['DBSURL'])
         try:
          dbswriter = DBSWriter(self.args['DBSURL'])
         except DbsException, ex:
          logging.error("%s\n" % formatEx(ex))
          return
         #  //
         # // Insert Files to block and datasets 
         #//
         MergedBlockList=dbswriter.insertFiles(jobreport) 

         #  //
         # //  Check on block closure conditions for merged fileblocks
         #//
         if (jobreport.jobType == "Merge") and (CloseBlockSize != "None"):
            if len(MergedBlockList)>0:
               MigrateBlockList=[]
               for MergedBlockName in MergedBlockList:
                   closedBlock=dbswriter.manageFileBlock(MergedBlockName ,maxSize = CloseBlockSize)
                   if closedBlock:
                      MigrateBlockList.append(MergedBlockName)
               #  //
               # //   Trigger Migration of closed Blocks to Global DBS
               #//
               if len(MigrateBlockList)>0:
                  for BlockName in MigrateBlockList:
                     datasetPath= dbswriter.blockToDatasetPath(BlockName)
                     self.MigrateBlock(datasetPath, [BlockName])
                  #self.MigrateBlock(datasetPath, MigrateBlockList )
               # FIXME:
               #  if migration succesfull: trigger PhEDEx injection?? (If Phedex is configured)
               # FIXME:
         

# logging.debug("TEST: Placeholder for publishing MergeRegistered Event with payload %s"%(jobReportLocation,))
#  set the "cleanup" trigger in PhEDEX instead??
                    


##comments to check in insertFiles :
## 1) is the safety check applied? File Info must be associated to at least one dataset before we try any of this
#                if len(fileinfo.dataset) == 0:
#                    msg = "WARNING: File in job report is not associated to a dataset:\n"
#                   msg += "LFN: %s\n" % fileinfo['LFN']
#                    msg += "This file will not be added to a fileblock or dataset\n"
#                    logging.error(msg)
#                    continue
## 2) we skip the tier check or we do that before the dbswriter.insertFiles is called??/

            #  //
            # // On successful insertion of job report, set the trigger
            #//  to say we are done with it so that cleanup can be triggered.
         try:
                self.trigger.setFlag("cleanup", jobreport.jobSpecId,
                                     "DBSInterface")
         except Exception, ex:
                msg = "Error setting cleanup flag for job: "
                msg += "%s\n" % jobreport.jobSpecId
                msg += str(ex)
                logging.error(msg)

                    
        return


    def getSEname(self):
        """
         fake the SE name for the time being...until this is extracted form FWJReport
        """
        plugConfig = loadPluginConfig("JobCreator","Creator")
        if plugConfig.has_key("StageOut"):
          if plugConfig['StageOut']['TargetHostName']!='None':
            SEname=plugConfig['StageOut']['TargetHostName']
            return SEname
        SEname=None
        return SEname


    def getDataTier(self,DataTier,DBSDataTier):
        """
         guess the Application family and data tier from the POOL Output Module Name convention in .cfg 
        """

        DBSDataTierList = DBSDataTier.split(",")

        if DataTier not in DBSDataTierList:
            raise InvalidDataTier(DataTier, DBSDataTierList)
        return DataTier              


    def RetryFailures(self,fileName, filehandle):
        """                                                                     
        Read the list of FWKJobReport that failed DBS registration and re-try the registration. If the FWKJobReport registration is succesfull remove it form the list of failed ones. 
                                                                                
        """
        logging.info("*** Begin the RetryFailures procedure")

        ## Read the list of FWJobReport that failed DBS registration and re-try
        filehandle.close()
        BadReportfile = open(fileName, 'r')

        stillFailures = []
        discarded = []
        for line in BadReportfile.readlines():
           payload = os.path.expandvars(os.path.expanduser(string.strip(line)))
           if not os.path.exists(payload):
             logging.error("File Not Found : %s"%payload)
             discarded.append(payload)
             continue
           try:
             self.handleJobReport(payload)
           except InvalidJobReport, ex:
                logging.error("InvalidJobReport")
                logging.error("Failed to Handle Job Report: %s" % payload)
                logging.error("InvalidJobReport Details: %s Exception %s" %(ex.getClassName(), ex.getErrorMessage()))
                stillFailures.append(payload)
           except InvalidDataTier, ex:
                logging.error("InvalidDataTier")
                logging.error("Failed to Handle Job Report: %s" % payload)
                logging.error("Details: %s Exception %s" %(ex.getClassName(), ex.getErrorMessage()))
                stillFailures.append(payload)
           except DbsException, ex:
                logging.error("Failed to Handle Job Report: %s" % payload)
                logging.error("DbsException Details: %s %s" %(ex.getClassName(), ex.getErrorMessage()))
                stillFailures.append(payload)
           except AssertionError, ex:
                logging.error("Failed to Handle Job Report: %s" % payload)
                logging.error("AssertionError Details: %s" % str(ex))
                stillFailures.append(payload)
           except NoFileBlock, ex:
                logging.error("Failed to Handle Job Report: %s" % payload)
                logging.error("Details: %s Exception %s" %(ex.getClassName(), ex.getErrorMessage()))
                stillFailures.append(payload)
           except StandardError, ex:
                logging.error("Failed to Handle Job Report: %s" % payload)
                logging.error("StandardError Details:%s" % str(ex))
                stillFailures.append(payload)

        BadReportfile.close()
       
        ## Write the list of those still failing 

        BadReportfile = open(fileName, 'w')
        for item in stillFailures:
           jobreport=item.strip()
           BadReportfile.write("%s\n" % jobreport )
        BadReportfile.close()

        logging.info("*** End the RetryFailures procedures => Discarded: %s Failed logged in :%s "%(discarded,fileName))

    def MigrateBlock(self, datasetPath, fileblockList):
        """
        Migrate from Local to Global                                                                         
        """
        #//
        #// Global DBS and DLS API
        #//
        DBSConf,DLSConf= getGlobalDBSDLSConfig()
        GlobalDBSwriter= DBSWriter(DBSConf['DBSURL'])
        #self.GlobalDLSwriter = DLSWriter(DLSConf['DLSAddress'],DLSConf['DLSType'])

        logging.info(">> From Local DBS: %s "%(self.args['DBSURL'],))
        logging.info(">> To Global DBS: %s "%(DBSConf['DBSURL'],))       
        #logging.info(">> From Local DBS: %s DLS: %s"%(GlobalDBSreader,GlobalDLSreader))
        #logging.info(">> To Global DBS: %s DLS: %s"%(GlobalDBSwriter,GlobalDLSwriter))

        GlobalDBSwriter.migrateDatasetBlocks(self.args['DBSURL'],datasetPath,fileblockList)

        #//
        #// Upload to Global DLS
        #//
        #self.UploadtoDLS(fileblockList)


    def getGlobalDBSDLSConfig(self):
        """
        Extract the global DBS and DLS contact information from the prod agent config
        """                                                                                               
                                                                                               
        try:
            config = loadProdAgentConfiguration()
        except StandardError, ex:
            msg = "Error reading configuration:\n"
            msg += str(ex)
            logging.error(msg)
            raise RuntimeError, msg
                                                                                               
        if not config.has_key("GlobalDBSDLS"):
            msg = "Configuration block GlobalDBSDLS is missing from $PRODAGENT_CONFIG"
            logging.error(msg)
                                                                                               
        try:
             globalConfig = config.getConfig("GlobalDBSDLS")
        except StandardError, ex:
            msg = "Error reading configuration for GlobalDBSDLS:\n"
            msg += str(ex)
            logging.error(msg)
            raise RuntimeError, msg
                                                                                               
        logging.debug("GlobalDBSDLS Config: %s" % globalConfig)

        dbsConfig = {
        'DBSURL' : globalConfig['DBSURL'],
        }
        dlsConfig = {
        "DLSType" : globalConfig['DLSType'],
        "DLSAddress" : globalConfig['DLSAddress'],
        }

        return dbsConfig, dlsConfig

    def startComponent(self):
        """
        _startComponent_

        Start up the component

        """
        # create message service
        self.ms = MessageService()
        self.trigger=TriggerAPI(self.ms)                                                                      
        # register
        self.ms.registerAs("DBS2Component")
                                                                                
        # subscribe to messages
        self.ms.subscribeTo("NewDataset")
        self.ms.subscribeTo("JobSuccess")
        self.ms.subscribeTo("DBSInterface:RetryFailures")
        self.ms.subscribeTo("DBSInterface:StartDebug")
        self.ms.subscribeTo("DBSInterface:EndDebug")
                                                                                
        # wait for messages
        while True:
            type, payload = self.ms.get()
            self.ms.commit()
            logging.debug("DBSComponent: %s, %s" % (type, payload))
            self.__call__(type, payload)
                                                                                

