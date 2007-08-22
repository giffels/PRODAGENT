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

from ProdCommon.Database import Session
from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec
from ProdCommon.DataMgmt.DBS.DBSWriter import DBSWriter
from ProdCommon.DataMgmt.DBS.DBSErrors import DBSWriterError, formatEx,DBSReaderError
from ProdCommon.DataMgmt.DBS.DBSReader import DBSReader
from ProdCommon.DataMgmt.PhEDEx.TMDBInject import tmdbInjectBlock,TMDBInjectError
from ProdAgentCore.Configuration import loadProdAgentConfiguration
from DBSAPI.dbsApiException import DbsException

import os,base64,time,exceptions
from FwkJobRep.ReportParser import readJobReport
from MessageService.MessageService import MessageService
from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdAgent.Trigger.Trigger import Trigger

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
def getGlobalDBSDLSConfig():
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
        'ReadDBSURL' : globalConfig['ReadDBSURL'],
        }
        return dbsConfig

# ##############
class DBSComponent:
    """
    _DBSComponent_

    A Threaded Server that makes calls out to the DBS when events are recieved

    """
    def __init__(self, **args):
        self.args = {}

        self.args.setdefault("DBSURL","http://cmssrv18.fnal.gov:8989/DBS/servlet/DBSServlet")
        self.args.setdefault("ReadDBSURL","http://cmssrv18.fnal.gov/DBS/servlet/DBSServlet")
        self.args.setdefault("Logfile", None)
        self.args.setdefault("BadReportfile", None)
        self.args.setdefault("BadDatasetfile", None)
        self.args.setdefault("BadTMDBInjectfile", None)
        self.args.setdefault("BadMigrationfile", None)
        self.args.setdefault("CloseBlockSize", "None")  # No check on fileblock size
        self.args.setdefault("CloseBlockFiles", 100 )        
        self.args.setdefault("skipGlobalMigration", False )
        self.args.setdefault("skipPhEDExInjection", True )
        self.args.setdefault("DataMode", "mc" )

        self.args.update(args)

        if self.args['Logfile'] == None:
            self.args['Logfile'] = os.path.join(self.args['ComponentDir'],
                                                "ComponentLog")

        self.skipGlobalMigration = False
        if str(self.args['skipGlobalMigration']).lower() == "true":
            self.skipGlobalMigration = True

        if str(self.args['skipPhEDExInjection']).lower() == "true":
            self.skipPhEDExInjection = True
        else:
            self.skipPhEDExInjection = False


# use the LoggingUtils
        LoggingUtils.installLogHandler(self)
        logging.info("DBSComponent Started...")


        #  //
        # // Log Failed FWJobReport registration into DBS
        #//
        if self.args['BadReportfile'] == None:
            self.args['BadReportfile'] = os.path.join(self.args['ComponentDir'],
                                                      "FailedJobReportList.txt")

        #  //
        # // Log Failed NewDataset registration into DBS
        #//
        if self.args['BadDatasetfile'] == None:
            self.args['BadDatasetfile'] = os.path.join(self.args['ComponentDir'],                                                      "FailedDatasetList.txt")                                                                                
        #  //
        # // Log Failed fileblock migration to Global
        #//
        if self.args['BadMigrationfile'] == None:
            self.args['BadMigrationfile'] = os.path.join(self.args['ComponentDir'],
                                                      "FailedMigration.txt")

        #  //
        # // Log Failed fileblock injection into PhEDEx
        #//
        if self.args['BadTMDBInjectfile'] == None:
            self.args['BadTMDBInjectfile'] = os.path.join(self.args['ComponentDir'],
                                                      "FailedTMDBInject.txt")

        
        
    def __call__(self, event, payload):
        """

        Define response to events
        """
        logging.info("Recieved Event: %s" % event)
        logging.info("Payload: %s" % payload)

        if event == "NewDataset":
            self.BadDataset = open(self.args['BadDatasetfile'],'a')
            logging.info("New Dataset Event: %s" % payload)
            try:
                self.newDatasetEvent(payload)
                return
            except InvalidWorkFlowSpec, ex:
                logging.error("Failed to Create New Dataset: %s" % payload)
                logging.error("Details: %s Exception %s" %(ex.getClassName(), ex.getErrorMessage()))
            except DBSWriterError, ex:
                logging.error("Failed to Create New Dataset: %s" % payload)
                self.BadDataset.write("%s\n" % payload)
                self.BadDataset.flush()
                return
            except DBSReaderError, ex: 
                logging.error("Failed to Create New Dataset: %s" % payload)
                self.BadDataset.write("%s\n" % payload)
                self.BadDataset.flush()
                return
            except DbsException, ex:
                logging.error("Failed to Create New Dataset: %s" % payload)
                logging.error("Details: %s"% formatEx(ex))
                self.BadDataset.write("%s\n" % payload)
                self.BadDataset.flush()
                return
            except StandardError, ex:
                logging.error("Failed to Create New Dataset: %s" % payload)
                logging.error("Details: %s" % str(ex))
                self.BadDataset.write("%s\n" % payload)
                self.BadDataset.flush()
                return

            
        if event == "JobSuccess":
            self.BadReport = open(self.args['BadReportfile'],'a')
            #logging.info("Job Succeeded: %s" % payload)
            try:
                self.handleJobReport(payload)
                return
            except InvalidJobReport, ex:
                logging.error("InvalidJobReport")
                logging.error("Failed to Handle Job Report: %s" % payload)
                logging.error("InvalidJobReport Details: %s Exception %s" %(ex.getClassName(), ex.getErrorMessage()))
                return
            except InvalidDataTier, ex:
                logging.error("InvalidDataTier")
                logging.error("Failed to Handle Job Report: %s" % payload)
                logging.error("Details: %s Exception %s" %(ex.getClassName(), ex.getErrorMessage()))
            except DBSWriterError, ex:
                logging.error("Failed to Handle Job Report: %s" % payload)
                ## add the FWKJobReport to the failed list
                self.BadReport.write("%s\n" % payload)
                self.BadReport.flush()
                return
            except DBSReaderError, ex:
                logging.error("Failed to Handle Job Report: %s" % payload)
                ## add the FWKJobReport to the failed list
                self.BadReport.write("%s\n" % payload)
                self.BadReport.flush()
                return
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

        if event == "MigrationRetryFailures":
            self.BadMigration = open(self.args['BadMigrationfile'],'a')
            try:
                self.MigrationRetryFailures(self.args['BadMigrationfile'],self.BadMigration)
                return
            except StandardError, ex:
                logging.error("Failed to MigrationRetryFailures")
                logging.error("Details: %s" % str(ex))
                return

        if event == "PhEDExRetryFailures":
            self.BadTMDBInject = open(self.args['BadTMDBInjectfile'],'a')
            try:
                self.PhEDExRetryFailures(self.args['BadTMDBInjectfile'],self.BadTMDBInject)
                return
            except StandardError, ex:
                logging.error("Failed to PhEDExRetryFailures")
                logging.error("Details: %s" % str(ex))
                return

        if event == "DBSInterface:RetryFailures":
            #  //
            # // Retry dataset insertion failure first
            #//
            self.BadDataset = open(self.args['BadDatasetfile'],'a')
            try:
                self.DatasetRetryFailures(self.args['BadDatasetfile'],self.BadDataset)
            except DBSWriterError, ex:
                logging.error("Failed to Create New Dataset: %s" % payload)
            except DBSReaderError, ex:
                logging.error("Failed to Create New Dataset: %s" % payload)
            except DbsException, ex:
                logging.error("Failed to Create New Dataset: %s" % payload)
            except StandardError, ex:
                logging.error("Failed to RetryFailures")
                logging.error("Details: %s" % str(ex))
            #  //
            # // Retry files insertion failures
            #//
            self.BadReport = open(self.args['BadReportfile'],'a')
            try:
                self.RetryFailures(self.args['BadReportfile'],self.BadReport)
                return
            except DBSWriterError, ex:
                logging.error("Failed to Handle Job Report: %s" % payload)
                return
            except DBSReaderError, ex:
                logging.error("Failed to Handle Job Report: %s" % payload)
                return
            except DbsException, ex:
                logging.error("Failed to Handle Job Report: %s" % payload)
                logging.error("DbsException Details: %s %s" %(ex.getClassName(), ex.getErrorMessage()))
                return
            except StandardError, ex:
                logging.error("Failed to RetryFailures")
                logging.error("Details: %s" % str(ex))
                return


        if event == "DBSInterface:MigrateDatasetToGlobal":
            #logging.info("DBSInterface:MigrateDatasetToGlobal Event %s"% payload)
            try:
                self.MigrateDatasetToGlobal(payload)
                return
            except DBSWriterError, ex:
                logging.error("Failed to MigrateDatasetToGlobal: %s" % payload)
            except DBSReaderError, ex:
                logging.error("Failed to MigrateDatasetToGlobal: %s" % payload)
            except StandardError, ex:
                logging.error("Failed to MigrateDatasetToGlobal")
                logging.error("Details: %s" % str(ex))
                return

        if event == "DBSInterface:MigrateBlockToGlobal":
            #logging.info("DBSInterface:MigrateBlockToGlobal Event %s"% payload)
            try:
               self.MigrateBlockToGlobal(payload)
               return
            except: 
               logging.error("Failed to MigrateBlocktToGlobal %s"%payload)
            return 

        if event == "DBSInterface:SetCloseBlockSize":
            #logging.info("DBSInterface:SetCloseBlockSize Event %s"% payload)
            self.args['CloseBlockSize']=payload
            return
        if event == "DBSInterface:SetCloseBlockFiles":
            #logging.info("DBSInterface:SetCloseBlockFiles Event %s"% payload)
            self.args['CloseBlockFiles']=payload
            return

        if event == "DBSInterface:CloseBlock":
            try:
                self.CloseBlock(payload)
                return
            except DBSWriterError, ex:
                logging.error("Failed to CloseBlock: %s" % payload)
            except DBSReaderError, ex:
                logging.error("Failed to CloseBlock: %s" % payload)
            except StandardError, ex:
                logging.error("Failed to CloseBlock")
                logging.error("Details: %s" % str(ex))
            return

        if event == "PhEDExInjectBlock":
            self.handlePhEDExInjectBlock(payload)
            return

        if event == "PhEDExInjectDataset":
            try:
               self.handlePhEDExInjectDataset(payload)
               return
            except DBSReaderError, ex:
                logging.error("Failed to PhEDExInjectDataset: %s" % payload)
            except StandardError, ex:
                logging.error("Failed to PhEDExInjectDataset")
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
        #//`
        logging.info("DBSURL %s"%self.args['DBSURL'])
        #dbswriter = DBSWriter('fakeurl') 
        dbswriter = DBSWriter(self.args['DBSURL'],level='ERROR')
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
          dbswriter = DBSWriter(self.args['DBSURL'],level='ERROR')
         except DbsException, ex:
          logging.error("%s\n" % formatEx(ex))
          return

         #
         #  remove lumi sections fro mc data
         # 
         if ( self.args['DataMode'] == "mc" ):
            if len(jobreport.files)>0:
               for outFile in jobreport.files:
                   outFile.lumisections = {}

         #  //
         # // Insert Files to block and datasets 
         #//
         logging.info(">>>>> inserting Files")
         MergedBlockList=dbswriter.insertFiles(jobreport) 

         #  //
         # //  Check on block closure conditions for merged fileblocks
         #//
         if (jobreport.jobType == "Merge"):
            maxFiles=100
            maxSize=None
            if ( self.args['CloseBlockSize'] != "None"):  maxSize=float(self.args['CloseBlockSize'])
            if ( self.args['CloseBlockFiles'] != "None"): maxFiles=float(self.args['CloseBlockFiles'])
            if len(MergedBlockList)>0:
               MigrateBlockList=[]
               for MergedBlockName in MergedBlockList:
                   logging.info(">>>>> Checking Close-Block Condition: Size > %s or Files > %s for FileBlock %s"%(maxSize,maxFiles,MergedBlockName)) 
                   closedBlock=dbswriter.manageFileBlock(MergedBlockName , maxFiles= maxFiles, maxSize = maxSize)
                   if closedBlock:
                      MigrateBlockList.append(MergedBlockName)
               #  //
               # //   Trigger Migration of closed Blocks to Global DBS
               #//
               if len(MigrateBlockList)>0 and not self.skipGlobalMigration:
                  for BlockName in MigrateBlockList:
                     #datasetPath= dbswriter.reader.blockToDatasetPath(BlockName)
                     self.handleMigrateBlock(BlockName)

                     #  //
                     # // Trigger PhEDEx injection of migrated blocks
                     #//  (if the migration is not successfull this point is not reached)
                     if not self.skipPhEDExInjection:
                       self.handlePhEDExInjectBlock(BlockName)  

         #  //
         # // On successful insertion of job report, set the trigger
         #//  to say we are done with it so that cleanup can be triggered.
         try:
                self.trigger.setFlag("cleanup", jobreport.jobSpecId,
                                     "DBS2Interface")
         except Exception, ex:
                msg = "Error setting cleanup flag for job: "
                msg += "%s\n" % jobreport.jobSpecId
                msg += str(ex)
                logging.error(msg)

                    
        return



    def getDataTier(self,DataTier,DBSDataTier):
        """
         guess the Application family and data tier from the POOL Output Module Name convention in .cfg 
        """

        DBSDataTierList = DBSDataTier.split(",")

        if DataTier not in DBSDataTierList:
            raise InvalidDataTier(DataTier, DBSDataTierList)
        return DataTier              


    def MigrateDatasetToGlobal(self,datasetPath):
        """
        Migrate all the blocks of a dataset to Global DBS.
        Before migration the still open blocks are closed.
        """
        #  //
        # // Find all the blocks of the dataset
        #//
        LocalDBSurl=self.args['DBSURL']
        ReadLocalDBSurl=self.args['ReadDBSURL']
        reader = DBSReader(ReadLocalDBSurl,level='ERROR')
        #
        # Close the not empty blocks that are still open:
        #
        writer  = DBSWriter(LocalDBSurl)
        for blockName in reader.listFileBlocks(datasetPath):
             writer.manageFileBlock(blockName,maxFiles=1)
        #
        #  //
        # // Migrate to Global DBS all the Closed blocks of the dataset
        #//

        MigrateBlockList = reader.listFileBlocks(datasetPath, onlyClosedBlocks = True)
        for MigrateBlock in MigrateBlockList:
            self.handleMigrateBlock(MigrateBlock)
        #self.MigrateDatasetBlocks(datasetPath, MigrateBlockList)
 

    def MigrateBlockToGlobal(self,BlockName):
       """
        Migrate the block to Global DBS.
       """
       LocalDBSurl=self.args['DBSURL']
       writer  = DBSWriter(LocalDBSurl,level='ERROR')
       #
       # Migrate the block, closing it 
       #
       writer.manageFileBlock(BlockName,maxFiles=1)
       self.handleMigrateBlock(BlockName)


    def handleMigrateBlock(self, fileblock):
        """
        """
        self.BadMigration = open(self.args['BadMigrationfile'],'a')
        try:
                self.MigrateBlock(fileblock)
                return
        except DBSWriterError, ex:
                logging.error("Failed to MigrateBlock: %s "%fileblock)
                self.BadMigration.write("%s\n"%fileblock)
                self.BadMigration.flush()
                return
        except DBSReaderError, ex:
                logging.error("Failed to MigrateBlock: %s"%fileblock)
                self.BadMigration.write("%s\n"%fileblock)
                self.BadMigration.flush()
                return
        except StandardError, ex:
                logging.error("Failed to MigrateBlock: %s"%fileblock)
                logging.error("Details: %s" % str(ex))
                self.BadMigration.write("%s\n"%fileblock)
                self.BadMigration.flush()
                return
        return


    def MigrateBlock(self, fileblock):
        """
        Migrate from Local to Global                                                                                                 
        """
        writer  = DBSWriter(self.args['DBSURL'],level='ERROR')
        #
        # Get the datasetPath the block belong to
        #
        datasetPath= writer.reader.blockToDatasetPath(fileblock)
        #//
        #// Global DBS API
        #//
        DBSConf= getGlobalDBSDLSConfig()
        GlobalDBSwriter= DBSWriter(DBSConf['DBSURL'])

        logging.info(">> Migrating FileBlocks %s in Dataset %s"%(fileblock,datasetPath))
        logging.info(">> From Local DBS: %s "%(self.args['DBSURL'],))
        logging.info(">> To Global DBS: %s "%(DBSConf['DBSURL'],))
                                                                                                
        GlobalDBSwriter.migrateDatasetBlocks(self.args['DBSURL'],datasetPath,[fileblock])

    def MigrateDatasetBlocks(self,datasetPath,fileblockList):
        """
        Migrate from Local to Global
        """
        #//
        #// Global DBS API
        #//
        DBSConf= getGlobalDBSDLSConfig()
        GlobalDBSwriter= DBSWriter(DBSConf['DBSURL'])
                                                                                                
        logging.info(">> Migrating FileBlocks %s in Dataset %s"%(fileblock,datasetPath))
        logging.info(">> From Local DBS: %s "%(self.args['DBSURL'],))
        logging.info(">> To Global DBS: %s "%(DBSConf['DBSURL'],))
                                                                                                
        GlobalDBSwriter.migrateDatasetBlocks(self.args['DBSURL'],datasetPath,fileblockList)
         
    def MigrationRetryFailures(self,fileName, filehandle):
        """
        Read the list of fileblock whose migration failed
        """
        logging.info("*** Begin the MigrationRetryFailures procedure")
        ## Read the list of fileblock that failed Migration  and re-try
        filehandle.close()
        BadMigrationfile = open(fileName, 'r')
        stillFailures = []
        for line in BadMigrationfile.readlines():
           payload=string.strip(line)
           try:
             self.MigrateBlock(payload)
           except DBSReaderError,ex:
                logging.error("Failed to MigrateBlock: %s" % payload)
                if not stillFailures.count(payload): stillFailures.append(payload)
           except DBSWriterError,ex:
                logging.error("Failed to MigrateBlock: %s" % payload)
                if not stillFailures.count(payload): stillFailures.append(payload)
           except StandardError, ex:
                logging.error("Failed to MigrateBlock: %s" % payload)
                logging.error("StandardError Details:%s" % str(ex))
                if not stillFailures.count(payload): stillFailures.append(payload) 
        BadMigrationfile.close()

        ## Write the list of those still failing
        BadMigrationfile = open(fileName, 'w')
        for item in stillFailures:
           fileblock=item.strip()
           BadMigrationfile.write("%s\n" % fileblock )
        BadMigrationfile.close()

	logging.info("*** End the MigrationRetryFailures procedures => Failed migration logged in :%s "%(fileName))

    def CloseBlock(self,fileBlockName):
        """
        Close fileblock if it satisty the block closure conditions
        """
        dbswriter = DBSWriter(self.args['DBSURL'],level='ERROR')
        maxFiles=100
        maxSize=None
        if ( self.args['CloseBlockSize'] != "None"):  maxSize=float(self.args['CloseBlockSize'])
        if ( self.args['CloseBlockFiles'] != "None"): maxFiles=float(self.args['CloseBlockFiles'])
        logging.info(">>>>> Checking Close-Block Condition: Size > %s or Files > %s for FileBlock %s"%(maxSize,maxFiles,fileBlockName))
        closedBlock=dbswriter.manageFileBlock(fileBlockName , maxFiles= maxFiles, maxSize = maxSize)
        if closedBlock: logging.info("Closed FileBlock %s"%fileBlockName)
        return 

    def PhEDExInjectBlock(self,fileBlockName):
        """
        Inject a Fileblock from Global DBS to PhEDEx
        """
        DBSConf= getGlobalDBSDLSConfig()
        GlobalDBSURL=DBSConf['DBSURL']
        ReadGlobalDBSURL=DBSConf['ReadDBSURL']

        phedexConfig,dropdir,Nodes=self.getPhEDExConfig() 
        #  //
        # // Get the datasetPath the block belong to
        #//
        reader = DBSReader(ReadGlobalDBSURL)
        datasetPath= reader.blockToDatasetPath(fileBlockName)
        #  //
        # // Inject that block to PhEDEx
        #//
        if phedexConfig == "None":
            msg="DBPARAM not configured in PhEDExConfig block in $PRODAGENT_CONFIG"
            raise RuntimeError, msg
        workingdir="/tmp"
        if dropdir != "None": workingdir=dropdir 
        tmdbInjectBlock(GlobalDBSURL, datasetPath, fileBlockName, phedexConfig, workingDir=workingdir,nodes=Nodes)
        return

    def getPhEDExConfig(self):
        """
        Extract the PhEDEx information from the prod agent config
        """         
        try:
            config = loadProdAgentConfiguration()
        except StandardError, ex:
            msg = "Error reading configuration:\n"
            msg += str(ex)
            logging.error(msg)
            raise RuntimeError, msg
                                                                                                     
        if not config.has_key("PhEDExConfig"):
            msg = "Configuration block PhEDExConfig is missing from $PRODAGENT_CONFIG"
            logging.error(msg)
                                                                                                     
        try:
             PhEDExConfig = config.getConfig("PhEDExConfig")
        except StandardError, ex:
            msg = "Error reading configuration for PhEDExConfig:\n"
            msg += str(ex)
            logging.error(msg)
            raise RuntimeError, msg
                                                                                                     
        logging.debug("PhEDEx Config: %s" % PhEDExConfig)
                                                                       
        nodes = None
        if PhEDExConfig.has_key("Nodes"): 
           if PhEDExConfig['Nodes'] != "None":
             nodes = PhEDExConfig['Nodes']      
                     
        return PhEDExConfig['DBPARAM'],PhEDExConfig['PhEDExDropBox'],nodes


    def handlePhEDExInjectBlock(self,payload):
        """
        inject a fileblock to PhEDEx and log the failures
        """
        self.BadTMDBInject = open(self.args['BadTMDBInjectfile'],'a')
        try:
                self.PhEDExInjectBlock(payload)
                return
        except DBSWriterError, ex:
                logging.error("Failed to PhEDExInjectBlock: %s" % payload)
                self.BadTMDBInject.write("%s\n" % payload)
                self.BadTMDBInject.flush()
                return
        except DBSReaderError, ex:
                logging.error("Failed to PhEDExInjectBlock: %s" % payload)
                self.BadTMDBInject.write("%s\n" % payload)
                self.BadTMDBInject.flush()
                return
        except TMDBInjectError, ex:
                logging.error("Failed to PhEDExInjectBlock: %s" % payload)
                self.BadTMDBInject.write("%s\n" % payload)
                self.BadTMDBInject.flush()
                return
        except StandardError, ex:
                logging.error("Failed to PhEDExInjectBlock")
                logging.error("Details: %s" % str(ex))
                self.BadTMDBInject.write("%s\n" % payload)
                self.BadTMDBInject.flush()
                return
        except RuntimeError, ex:
                logging.error("Failed to PhEDExInjectBlock")
                logging.error("Details: %s" % str(ex))
                self.BadTMDBInject.write("%s\n" % payload)
                self.BadTMDBInject.flush()
                return
        return


    def handlePhEDExInjectDataset(self,datasetPath):
        """
        Inject a dataset from Global DBS to PhEDEx
        """
        DBSConf= getGlobalDBSDLSConfig()
        GlobalDBSURL=DBSConf['DBSURL']
        ReadGlobalDBSURL=DBSConf['ReadDBSURL']
        reader = DBSReader(ReadGlobalDBSURL,level='ERROR')
        #  //
        # // inject each fileblock of the dataset
        # //
        for blockName in reader.listFileBlocks(datasetPath):
           self.handlePhEDExInjectBlock(blockName)


    def PhEDExRetryFailures(self,fileName, filehandle):
        """
        Read the list of FWKJobReport that failed DBS registration and re-try the registration. If the FWKJobReport registration is succesfull remove it form the list of failed ones.
        """
        logging.info("*** Begin the PhEDExRetryFailures procedure")
                                                                                                                                          
        ## Read the list of fileblock that failed TMDBInjection  and re-try
        filehandle.close()
        BadTMDBInjectfile = open(fileName, 'r')

        stillFailures = []
        for line in BadTMDBInjectfile.readlines():
           payload=string.strip(line)
           try:
             self.PhEDExInjectBlock(payload)
           except TMDBInjectError, ex:
                logging.error("Failed to PhEDExInjectBlock: %s" % payload)
                if not stillFailures.count(payload): stillFailures.append(payload)
           except DBSReaderError,ex:
                logging.error("Failed to PhEDExInjectBlock: %s" % payload)
                if not stillFailures.count(payload): stillFailures.append(payload)
           except DBSWriterError,ex:
                logging.error("Failed to PhEDExInjectBlock: %s" % payload)
                if not stillFailures.count(payload): stillFailures.append(payload)

        BadTMDBInjectfile.close()

        ## Write the list of those still failing

        BadTMDBInjectfile = open(fileName, 'w')
        for item in stillFailures:
           fileblock=item.strip()
           BadTMDBInjectfile.write("%s\n" % fileblock )
        BadTMDBInjectfile.close()

        logging.info("*** End the PhEDExRetryFailures procedures => Failed injection logged in :%s "%(fileName))



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
           payload = string.replace(string.strip(line),'file://','')
           payload = os.path.expandvars(os.path.expanduser(payload))
           logging.info("--> %s"%payload)
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

    def DatasetRetryFailures(self,fileName, filehandle):
        """
        Read the list of dataset that failed DBS registration and re-try the registration. If the dataset registration is succesfull remove it form the list of failed ones.
                                                                                                          
        """
        logging.info("*** Begin the DatasetRetryFailures procedure")
                                                                                                          
        ## Read the list of dataset that failed DBS registration and re-try
        filehandle.close()
        Badfile = open(fileName, 'r')
                                                                                                          
        stillFailures = []
        discarded = []
        for line in Badfile.readlines():
           payload = string.replace(string.strip(line),'file://','')
           payload = os.path.expandvars(os.path.expanduser(payload))
           logging.info("--> %s"%payload)
           if not os.path.exists(payload):
             logging.error("File Not Found : %s"%payload)
             discarded.append(payload)
             continue
           try:
             self.newDatasetEvent(payload)
           except DbsException, ex:
                logging.error("Failed to Create New Dataset: %s" % payload)
                logging.error("DbsException Details: %s %s" %(ex.getClassName(), ex.getErrorMessage()))
                stillFailures.append(payload)
           except AssertionError, ex:
                logging.error("Failed to Create New Dataset: %s" % payload)
                stillFailures.append(payload)
           except StandardError, ex:
                logging.error("Failed to Create New Dataset: %s" % payload)
                logging.error("StandardError Details:%s" % str(ex))
                stillFailures.append(payload)
                                                                                                          
        Badfile.close()
                                                                                                          
        ## Write the list of those still failing
                                                                                                          
        Badfile = open(fileName, 'w')
        for item in stillFailures:
           dataset=item.strip()
           Badfile.write("%s\n" % dataset )
        Badfile.close()
                                                                                                          
        logging.info("*** End the DatasetRetryFailures procedures => Discarded: %s Failed logged in :%s "%(discarded,fileName))




    def startComponent(self):
        """
        _startComponent_

        Start up the component

        """
        # create message service
        self.ms = MessageService()
        self.trigger=Trigger(self.ms)                                                                      
        # register
        self.ms.registerAs("DBS2Interface")
                                                                                
        # subscribe to messages
        self.ms.subscribeTo("NewDataset")
        self.ms.subscribeTo("JobSuccess")
        self.ms.subscribeTo("DBSInterface:RetryFailures")
        self.ms.subscribeTo("DBSInterface:MigrateDatasetToGlobal")
        self.ms.subscribeTo("DBSInterface:MigrateBlockToGlobal")
        self.ms.subscribeTo("DBSInterface:SetCloseBlockSize")
        self.ms.subscribeTo("DBSInterface:SetCloseBlockFiles")
        self.ms.subscribeTo("DBSInterface:CloseBlock")
        self.ms.subscribeTo("DBSInterface:StartDebug")
        self.ms.subscribeTo("DBSInterface:EndDebug")
        self.ms.subscribeTo("PhEDExInjectBlock")
        self.ms.subscribeTo("PhEDExInjectDataset")
        self.ms.subscribeTo("PhEDExRetryFailures")
        self.ms.subscribeTo("SetJobCleanupFlag")
        self.ms.subscribeTo("MigrationRetryFailures")
                                                                                
        # wait for messages
        while True:
            type, payload = self.ms.get()
            self.ms.commit()
            logging.debug("DBSComponent: %s, %s" % (type, payload))
            Session.set_database(dbConfig)
            Session.connect()
            Session.start_transaction()
            self.__call__(type, payload)
            Session.commit_all()
            Session.close_all()
            self.ms.commit()

                                                                                

