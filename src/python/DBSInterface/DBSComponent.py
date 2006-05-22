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
        self.args['DBSAddress'] = None
        self.args['DBSType'] = 'CGI' # default to CGI
        self.args['Logfile'] = None
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
            logging.info("New Dataset: %s" % payload)
            try:
                self.newDataset(payload)
                return
            except InvalidWorkFlowSpec, ex:
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
                logging.error("Failed to Handle Job Report: %s" % payload)
                logging.error("InvalidJobReport Details: %s Exception %s" %(ex.getClassName(), ex.getErrorMessage()))
                return
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

    def newDataset(self, workflowspec):
        """
        _newDataset_

        Extract relevant info from the WorkFlowSpecification:
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

        #### Extract info from event payload :
        logging.debug("DBSComponent.newDataset: %s" % workflowspec)
        datasetinfo=self.readNewDatasetInfo(workflowspec)

        logging.debug(" - PrimaryDataset %s"%datasetinfo['PrimaryDataset'])
        logging.debug(" - ProcessedDataset %s"%datasetinfo['ProcessedDataset'])
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

        ## check datatier/app family
        applicationfamily=self.getAppFamily(datasetinfo['OutputModuleName'])
        #datatier=self.getDataTier(datasetinfo['OutputModuleName'])
        datatier=self.getDataTier(datasetinfo['DataTier'])
        logging.debug(" - ApplicationFamily %s"%applicationfamily)
        logging.debug(" - DataTier %s"%datatier)
        if datatier=='Unknown' or applicationfamily=='Unknown' :
         logging.error("Uknown datatier/Application Family")
         return 

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
        #for dataset in ListDatasetInfo:
        #  for key in dataset.keys():
        #    print "key %s value %s"%(key,dataset[key])

        # pick up only the first dataset since I don't know how to handle multiple output datasets
        DatasetInfo=ListDatasetInfo[0]
        #PSet = base64.encodestring(payload.configuration)

        return DatasetInfo

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
          dbsinfo= DBSclient(self.args['DBSAddress'])

          ## handle output files information from FWK report
          for fileinfo in jobreport.files:
 
           ## Retrieve the fileblock to add files to, look for just the first datasetPath
           datatier=self.getDataTier(fileinfo.dataset['DataTier'])
           
           tiers=string.split(datatier,'-')    # split the multi datatiers GEN-SIM-DIGI into single data tier 
           if ( self.args['DBSType'] != 'CGI'): tiers=[datatier]  # keep the datatier as a single piece 
           datasetPath="/"+fileinfo.dataset['PrimaryDataset']+"/"+tiers[0]+"/"+fileinfo.dataset['ProcessedDataset']
           fileblock = dbsinfo.getDatasetFileBlocks(datasetPath)
           if ( len(fileblock) == 0 ) or ( fileblock == None ) :
            logging.error("No Fileblock for the dataset %s"%datasetPath)
            return

           ## Insert files to block
           fList=dbsinfo.insertFiletoBlock(fileinfo,fileblock[0])

           ## Insert event collections: 
           for tier in tiers:
             datasetPath="/"+fileinfo.dataset['PrimaryDataset']+"/"+tier+"/"+fileinfo.dataset['ProcessedDataset']
             evcList=dbsinfo.setEVCollection(fileinfo,fList,datasetPath)
             dbsinfo.insertEVCtoDataset(datasetPath,evcList)

        return

 
    def getAppFamilyDataTier(self,OutModuleName):
        """
         guessing the Application family and data tier from the POOL Output Module Name convention in .cfg 
        """
        if ( OutModuleName=='Simulated' ) or ( OutModuleName=='writeOscar'):
             applicationfamily='Simulation'
             datatier='SIM'
        elif (OutModuleName=='Digitized' ) or ( OutModuleName=='writeDigis'):
             applicationfamily='Digitization'
             datatier='DIGI'
        elif (OutModuleName=='GenSimDigi') or (OutModuleName=='GEN-SIM-DIGI'):
             applicationfamily='GEN-SIM-DIGI'
             datatier='GEN-SIM-DIGI'
        else:
          applicationfamily='Unknown'
          datatier='Unknown'

        return applicationfamily,datatier

    def getAppFamily(self,ApplicationFamily):
        """
         guess the Application family and data tier from the POOL Output Module Name convention in .cfg 
        """
        if ( ApplicationFamily=='Simulated' ):
          applicationfamily='Simulation'
        elif ( ApplicationFamily=='Digitized' ):
          applicationfamily='Digitization'
        elif ( ApplicationFamily=='Merged' ):
          applicationfamily='Merging'
        elif ( ApplicationFamily=='GenSimDigi') or ( ApplicationFamily=='GEN-SIM-DIGI'):
          applicationfamily='GEN-SIM-DIGI'
        elif ( ApplicationFamily=='GEN-SIM'):
          applicationfamily='GEN-SIM'
        else:
          applicationfamily='Unknown'

        return applicationfamily

    def getDataTier(self,DataTier):
        """
         guess the Application family and data tier from the POOL Output Module Name convention in .cfg 
        """
        if ( DataTier=='Simulated' ):
          datatier='SIM'
        elif ( DataTier=='Digitized' ):
          datatier='DIGI'
        elif ( DataTier=='GenSimDigi') or (DataTier=='GEN-SIM-DIGI'):
          datatier='GEN-SIM-DIGI'
        elif ( DataTier=='GEN-SIM'):
          datatier='GEN-SIM'
        else:
          datatier='Unknown'

        return datatier

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
                                                                                

