#!/usr/bin/env python
"""
_DBSComponent_

Skeleton DBSComponent for ProdAgentLite

ProdAgent Events subscribed to by this Component

NewDataset - A New Dataset has been injected into the ProdAgent system
             via a processing request.
             Event Payload will be the dataset ID
         AF: Event Payload should be much more that the dataset ID
             Create a new dataset in DBS means :
               - create a primary dataset (if not already there)
               - create a processed dataset (this include inserting application
                configuration and processinf path info).

JobSuccess - Job completed, this event will include a ref to a Framework
            job report that needs to be handled and have the event collections
            that it describes be added to the DBS.
            Event Payload will be a reference to the JobReport file

"""
import string
import socket

from MCPayloads.WorkflowSpec import WorkflowSpec
import os,base64,exceptions
from FwkJobRep.ReportParser import readJobReport
from MessageService.MessageService import MessageService

import logging
from logging.handlers import RotatingFileHandler

## temporary waiting for SEname in FWKJobReport 
from ProdAgentCore.PluginConfiguration import loadPluginConfig

                                                                               
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
class DBSComponent:
    """
    _DBSComponent_

    A Threaded Server that makes calls out to the DBS when events are recieved

    """
    def __init__(self, **args):
        self.args = {}

        self.args.setdefault("DBSAddress", None)
        self.args.setdefault("DBSType", "CGI")
        self.args.setdefault("Logfile", None)
        self.args.setdefault("DBSDataTier", 'GEN,SIM,DIGI,RECO')
        self.args.setdefault("MaxBlockSize", None)  # No check on fileblock size

        self.args.update(args)

        if self.args['Logfile'] == None:
            self.args['Logfile'] = os.path.join(self.args['ComponentDir'],
                                                "ComponentLog")
        #  //
        # // Log Handler is a rotating file that rolls over when the
        #//  file hits 1MB size, 3 most recent files are kept
        logHandler = RotatingFileHandler(self.args['Logfile'],
                                         "a", 1000000, 3)
        #  //
        # // Set up formatting for the logger and set the 
        #//  logging level to info level
        #logFormatter = logging.Formatter("%(asctime)s:%(message)s")
        logFormatter = logging.Formatter("%(asctime)s:%(module)s:%(message)s")
        logHandler.setFormatter(logFormatter)
        logging.getLogger().addHandler(logHandler)
        logging.getLogger().setLevel(logging.INFO)
        
        logging.info("DBSComponent Started...")
        
    def __call__(self, event, payload):
        """
        _operator()_

        Define response to events
        """
        from dbsException import DbsException
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
            except InvalidDataTier, ex:
                logging.error("Failed to Create New Dataset: %s" % payload)
                logging.error("Details: %s Exception %s" %(ex.getClassName(), ex.getErrorMessage()))
            except DbsException, ex:
                logging.error("Failed to Create New Dataset: %s" % payload)
                logging.error("Details: %s %s" %(ex.getClassName(), ex.getErrorMessage()))
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
            except InvalidDataTier, ex:
                logging.error("InvalidDataTier")
                logging.error("Failed to Handle Job Report: %s" % payload)
                logging.error("Details: %s Exception %s" %(ex.getClassName(), ex.getErrorMessage()))
            except DbsException, ex:
                logging.error("Failed to Handle Job Report: %s" % payload)
                logging.error("DbsException Details: %s %s" %(ex.getClassName(), ex.getErrorMessage()))
                return
            except AssertionError, ex:
                logging.error("Failed to Handle Job Report: %s" % payload)
                logging.error("AssertionError Details: %s" % str(ex))
                return
            except StandardError, ex:
                logging.error("Failed to Handle Job Report: %s" % payload)
                logging.error("StandardError Details:%s" % str(ex))
                return

                
        if event == "DBSInterface:StartDebug":
            logging.getLogger().setLevel(logging.DEBUG)
            return
        if event == "DBSInterface:EndDebug":
            logging.getLogger().setLevel(logging.INFO)
            return
        
        return

    def newDatasetEvent(self, workflowspec):
        """
        _newDatasetEvent_

        Extract relevant info from the WorkFlowSpecification and loop over Dataset
        """

        #### Extract info from event payload :
        logging.debug("DBSComponent.newDatasetEvent: %s" % workflowspec)
        datasetinfoList=self.readNewDatasetInfo(workflowspec)
        for datasetinfo in datasetinfoList:
            self.newDataset(datasetinfo)


    def newDataset(self, datasetinfo):
        """
        _newDataset_
        Extract relevant info:
            - the application info (Version, etc..)
            - primary dataset, processed dataset, etc...
            - the encoded PSet (PSet Hash in future)
           what is missing(empty) is:
            - application family  \ can be guessed from the OutputModuleName
            - in/output data tier /
            - info to define the parent application
        The operations to perform then are:
         - Create a primary dataset (if not already there)
         - Create a processed dataset: this include inserting application
           configuration and processing path info.
         (- Create a fileblock associated to the processed dataset)
        """

        logging.debug("Inserting a Dataset in DBS: primary %s processed %s"%(datasetinfo['PrimaryDataset'],datasetinfo['ProcessedDataset']))
        logging.debug(" - ApplicationName %s"%datasetinfo['ApplicationName'])
        logging.debug(" - ApplicationVersion %s"%datasetinfo['ApplicationVersion'])

        ### Contact DBS using the DBS class
        if ( self.args['DBSType'] == 'CGI'):
          from DBS import DBS as DBSclient
        else:
          from DBS_Ws import DBS_Ws as DBSclient
        dbsinfo= DBSclient(self.args['DBSAddress'])

        ## create primary dataset

        dbsinfo.insertPrimaryDataset(datasetinfo['PrimaryDataset'])

        ## define app family from OutputModuleName
        applicationfamily = datasetinfo['OutputModuleName']
        ## check datatier
        datatier = self.getDataTier(datasetinfo['DataTier'], self.args['DBSDataTier'])
        logging.debug(" - ApplicationFamily %s" % applicationfamily)
        logging.debug(" - DataTier %s" % datatier)
        #if datatier=='Unknown' or applicationfamily=='Unknown' :
        # logging.error("Uknown datatier/Application Family")
        # return 

        ## create processed dataset ( + empty fileblock associated to it)

        dbsinfo.insertProcessedDataset(datasetinfo,datatier,applicationfamily)

        return

    def readNewDatasetInfo(self, workflowFile):
        """  
        _readNewDatasetInfo_

        Read the NewDataset event payload from WorkFlowSpec file
          
        """
        logging.debug("Reading the NewDataset event payload from WorkFlowSpec: ")
        workflowFile=string.replace(workflowFile,'file://','')
        if not os.path.exists(workflowFile):
            logging.error("Workflow File Not Found: %s" % workflowFile)
            raise InvalidWorkFlowSpec(workflowFile)
        try:
         workflowSpec = WorkflowSpec()
         workflowSpec.load(workflowFile)
         #payload = workflowSpec.payload
        except:
          logging.error("Invalid Workflow File: %s" % workflowFile)
          raise InvalidWorkFlowSpec(workflowFile)
        
        ListDatasetInfo=workflowSpec.outputDatasets()

        # pick up only the first dataset since I don't know how to handle multiple output datasets
        #DatasetInfo=ListDatasetInfo[0]
        #
        #return DatasetInfo
        return ListDatasetInfo

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
  
            ### Contact DBS using the DBS class
            if ( self.args['DBSType'] == 'CGI'):
                from DBS import DBS as DBSclient
            else:
                from DBS_Ws import DBS_Ws as DBSclient

            self.dbsinfo= DBSclient(self.args['DBSAddress'])

            # SEname=self.getSEname() 
            ## get Stage out SE from FWK report 
            SEname=jobreport.siteDetails['se-name']
            logging.debug(" SEname %s"%SEname)
            #  //
            # // handle output files information from FWK report
            #//  We first loop through files and add each file to the fileblock based
            #  //on the SE Name where it was placed.
            # // Then we loop through the datasets associated with that file
            #//  and insert the event collection for that file into each of those
            #  //datasets. 
            # // NOTE: Here I assume that the dataset has already been split into
            #//  basic tier datasets.
            for fileinfo in jobreport.files:
                #  //
                # // Safety check: File Info must be associated to at least one
                #//  dataset before we try any of this
                if len(fileinfo.dataset) == 0:
                    msg = "WARNING: File in job report is not associated to a dataset:\n"
                    msg += "LFN: %s\n" % fileinfo['LFN']
                    msg += "This file will not be added to a fileblock or dataset\n"
                    logging.error(msg)
                    continue
                
                #  //
                # // Define the fileblock to add files to,
                #//  look for just the first datasetPath
                firstDataset = fileinfo.dataset[0]
                firstDatasetPath = "/%s/%s/%s" % (
                      firstDataset['PrimaryDataset'],
                      firstDataset['DataTier'],
                      firstDataset['ProcessedDataset'],
                      )
                #  //
                # // check DataTier being valid 
                # //
                firstdatatier=self.getDataTier(firstDataset['DataTier'],self.args['DBSDataTier'])
                #  //
                # // Lookup the file block for this first dataset and SEName
                #//
                logging.debug("Searching for fileblock for SE: %s\n:  Dataset: %s\n" % (
                      SEname, firstDatasetPath)
                              )
                fileblock = self.checkFileBlockforSE(
                    firstDatasetPath,
                    SEname,
                    self.args['MaxBlockSize'],
                    firstDataset
                    )
                if fileblock is None:
                    msg += "No Fileblock found for dataset: %s" % firstDatasetPath
                    msg += "SEname: %s\n" % SEname 
                    logging.error(msg)
                    #continue
                    return
                
                #  //
                # // Insert files to block
                #//
                logging.info("Inserting File: %s \nInto FileBlock : %s\n" %(
                    fileinfo['LFN'], fileblock.get('blockName'))
                             )

                fList = self.dbsinfo.insertFiletoBlock(fileinfo, fileblock)

                logging.debug("FileList from FileBlock:\n%s\n" % fList)
                
                #  //
                # // For each dataset associated with the file,
                #//insert the Event Collection
                for dataset in fileinfo.dataset:
                    
                    datasetPath="/%s/%s/%s" % (
                      dataset['PrimaryDataset'],
                      dataset['DataTier'],
                      dataset['ProcessedDataset'],
                      )

           
                    #  //
                    # // Insert Event Collections
                    #//
                    logging.debug(
                      "Setting Event Collection For: %s\n in Dataset %s\n" % (
                          fileinfo['LFN'], datasetPath )
                      )
                    evcList = self.dbsinfo.setEVCollection(
                      fileinfo,     # File Details, LFN, PFN etc
                      fList,        # Details from FileBlock 
                      datasetPath)  # dataset to add to
                    logging.debug("Set Event Collection:%s\n" % evcList)

                    #  //
                    # // Insert into Dataset
                    #//
                    logging.debug(
                      "Inserting Event collections into Dataset"
                      )
                    self.dbsinfo.insertEVCtoDataset(datasetPath, evcList)

                    
                    
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

    def checkFileBlockforSE(self,datasetPath,SEname,MaxBlockSize,fileinfo):
        """
         o create a file block associated to a storage element (SE) 
           and add the fileblock-SE entry in DLS if one of the following conditions holds:
           1. a fileblock associated to this SE is not yet registered for the current dataset
           2. the file block associated to the SE is full
         o return the fileblock to add files to
        """
        from DLS import DLS 

        fileBlockList = self.dbsinfo.getDatasetFileBlocks(datasetPath)

        #if SEname is None: return fileBlockList[0]  ##temporary hack to behave as before until SEname is not defined in FWKJobReport

        ## get the type and endpoint from configuration DLS block 
        dlsinfo= DLS(self.args['DLSType'],self.args['DLSAddress'])
        #dlsinfo= DLS("DLS_TYPE_LFC","lfc-cms-test.cern.ch/grid/cms/DLS/LFC")

        ## look for an existing file block associated to the storage element and not yet full
        for fileBlock in fileBlockList:
          SEList=dlsinfo.getFileBlockLocation(fileBlock.get('blockName'))
          fileBlockSize=fileBlock.get('numberOfBytes')
          # if the SEname is not set use the  
          if SEname=="Unknown" and len(SEList)<=0:
              return fileBlock # found a fileblock not associated to any SE
          # check the fileblock at SE
          if SEList.count(SEname)>0:
            # check block size (need to check if fileblock is open too??)
           if MaxBlockSize==None:
              return fileBlock # found a fileblock associated to SE, no check on its size is performed
           elif fileBlockSize<=MaxBlockSize:
              return fileBlock # found a fileblock associated to SE and not full  

        ## create a new fileblock with the same processing from the empty fileblock created at the time of NewDataset
        #fileBlock = self.dbsinfo.addFileBlock(fileBlockList,datasetPath)
        fileBlock = self.dbsinfo.addFileBlock(fileinfo,datasetPath)
        if fileBlock is not None and SEname!="Unknown" :
         ## add the fileblock-SE entry to DLS
         dlsinfo.addEntryinDLS(fileBlock.get('blockName'),SEname)        
        return fileBlock



    def getDataTier(self,DataTier,DBSDataTier):
        """
         guess the Application family and data tier from the POOL Output Module Name convention in .cfg 
        """

        #DataTierList=DataTier.split("-")
        DBSDataTierList = DBSDataTier.split(",")

        if DataTier not in DBSDataTierList:
            raise InvalidDataTier(DataTier, DBSDataTierList)
        return DataTier              

    def startComponent(self):
        """
        _startComponent_

        Start up the component

        """
        # create message service
        self.ms = MessageService()
                                                                                
        # register
        self.ms.registerAs("DBSComponent")
                                                                                
        # subscribe to messages
        self.ms.subscribeTo("NewDataset")
        self.ms.subscribeTo("JobSuccess")
        self.ms.subscribeTo("DBSInterface:StartDebug")
        self.ms.subscribeTo("DBSInterface:EndDebug")
                                                                                
        # wait for messages
        while True:
            type, payload = self.ms.get()
            self.ms.commit()
            logging.debug("DBSComponent: %s, %s" % (type, payload))
            self.__call__(type, payload)
                                                                                

