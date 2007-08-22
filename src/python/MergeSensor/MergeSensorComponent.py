#!/usr/bin/env python
"""
_MergeSensor_

Component that watches the DBS to submit merge jobs when a subset of files in
a dataset are ready the be merged.

"""

__revision__ = "$Id: MergeSensorComponent.py,v 1.66 2007/06/08 10:31:14 ckavka Exp $"
__version__ = "$Revision: 1.66 $"
__author__ = "Carlos.Kavka@ts.infn.it"

import os
import time
import inspect

# Merge sensor import
from MergeSensor.WatchedDatasets import WatchedDatasets
from MergeSensor.MergeSensorError import MergeSensorError, \
                                         InvalidDataTier, \
                                         InvalidDataset, \
                                         DatasetNotInDatabase
from MergeSensor.Dataset import Dataset
from MergeSensor.MergeSensorDB import MergeSensorDB
from MergeSensor.Registry import retrieveMergePolicy
import MergeSensor.MergePolicies

# Message service import
from MessageService.MessageService import MessageService

# Workflow and Job specification
from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec
from ProdCommon.MCPayloads.LFNAlgorithm import mergedLFNBase, unmergedLFNBase
from ProdCommon.CMSConfigTools.ConfigAPI.CMSSWConfig import CMSSWConfig
import ProdCommon.MCPayloads.WorkflowTools as MCWorkflowTools
from ProdCommon.MCPayloads.MergeTools import createMergeJobWorkflow
# logging
import logging
import ProdAgentCore.LoggingUtils as LoggingUtils

# DBS2
from ProdCommon.DataMgmt.DBS.DBSReader import DBSReader
from ProdCommon.DataMgmt.DBS.DBSErrors import DBSReaderError
from DBSAPI.dbsApiException import DbsConnectionError

##############################################################################
# MergeSensorComponent class
##############################################################################
                                                                                
class MergeSensorComponent:
    """
    _MergeSensorComponent_

    Component that polls the DBS to look for datasets in need
    of merging

    """

    ##########################################################################
    # MergeSensor component initialization
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
        self.args.setdefault("ReadDBSURL",
          "http://cmssrv18.fnal.gov:8989/DBS/servlet/DBSServlet")
        self.args.setdefault("ComponentDir", None)
        self.args.setdefault("PollInterval", 60 )
        self.args.setdefault("Logfile", None)
        self.args.setdefault("StartMode", 'warm')
        self.args.setdefault("MaxMergeFileSize", None)
        self.args.setdefault("MinMergeFileSize", None)
        self.args.setdefault("MaxInputAccessFailures", 1)

        # default SE white/black lists are empty
        self.args.setdefault("MergeSiteWhitelist", None)
        self.args.setdefault("MergeSiteBlacklist", None)

        # fastMerge
        self.args.setdefault("FastMerge", None)

        # cleanup
        self.args.setdefault("CleanUp", None)

        # QueueJobMode
        self.args.setdefault('QueueJobMode', False)
        
        # merge policy plugin
        self.args.setdefault('MergePolicy', 'SizePolicy')
        #self.args.setdefault('MergePolicy', 'RunNumberPolicy')

        # update parameters
        self.args.update(args)

        # white list
        if self.args['MergeSiteWhitelist'] == None or \
           self.args['MergeSiteWhitelist'] == "None" or \
           self.args['MergeSiteWhitelist'] == "" :
            self.seWhitelist = []
        else:  
            self.seWhitelist = self.args['MergeSiteWhitelist'].split(',')

        # black list
        if self.args['MergeSiteBlacklist'] == None or \
           self.args['MergeSiteBlacklist'] == "None" or \
           self.args['MergeSiteBlacklist'] == "":
            self.seBlacklist = []
        else:
            self.seBlacklist = self.args['MergeSiteBlacklist'].split(',')

        # fast merge
        if self.args['FastMerge'] == None or \
           self.args['FastMerge'] == "NO" or \
           self.args['FastMerge'] == "no" or \
           self.args['FastMerge'] == "No" or \
           self.args['FastMerge'] == "" or \
           self.args['FastMerge'] == "False" or \
           self.args['FastMerge'] == 'false':
            self.fastMerge = False
        else:
            self.fastMerge = True


        # cleanup
        if str(self.args['CleanUp']).lower() in ("true", "yes"):
            self.doCleanUp = True
        else:
            self.doCleanUp = False

        # queue mode
        self.queueMode = False
        if str(self.args['QueueJobMode']).lower() == "true":
            self.queueMode = True

        
        # server directory
        self.args.setdefault("MergeJobSpecs", os.path.join(
            self.args['ComponentDir'], "merge-jobspecs"))

        # define log file
        if self.args['Logfile'] == None:
            self.args['Logfile'] = os.path.join(self.args['ComponentDir'], 
                                                "ComponentLog")
        # create log handler
        LoggingUtils.installLogHandler(self)

        # merge file size 
        if self.args["MaxMergeFileSize"] is None:
            self.args["MaxMergeFileSize"] = 2000000000
        else: 
            self.args["MaxMergeFileSize"] = int(self.args["MaxMergeFileSize"])
 
        if self.args["MinMergeFileSize"] is None:
            self.args["MinMergeFileSize"] = \
                int(self.args["MaxMergeFileSize"] * 0.75)
        else: 
            self.args["MinMergeFileSize"] = int(self.args["MinMergeFileSize"])
 
        if self.args["MaxMergeFileSize"] <=  self.args["MinMergeFileSize"]:
            logging.error("Wrong file size specifications, please check!") 

        # inital log information
        logging.info("MergeSensor starting... in >> %s << mode" % 
                     self.args["StartMode"])
        logging.info("with MaxMergeFileSize = %s" % \
                     self.args['MaxMergeFileSize'])
        logging.info("     MinMergeFileSize = %s" % \
                     self.args['MinMergeFileSize'])
        logging.info("     MergeSiteWhitelist = %s" % self.seWhitelist)
        logging.info("     MergeSiteBlacklist = %s" % self.seBlacklist)
        if self.fastMerge:
            logging.info("Using EDM fast merge.")
        else:
            logging.info("Using cmsRun merge.")
            
        if self.doCleanUp:
            logging.info("Using Auto CleanUp")
        else:
            logging.info("Auto CleanUp disabled.")

        # get DBS reader
        self.dbsReader = None
        self.delayDBS = 120
        self.connectToDBS()
        
        # merge workflow specification
        thisModule = os.path.abspath(
                          inspect.getsourcefile(MergeSensorComponent))
        baseDir = os.path.dirname(thisModule)        
        mergeWorkflow = os.path.join(baseDir, "mergeConfig.py")
        self.mergeWorkflow = file(mergeWorkflow).read()

        # load merge policy plugin
        self.policy = self.loadMergePolicy("SizePolicy")
        if self.policy == None:
            logging.error("Problems loading merge policy plugin")

        # compute poll delay
        delay = int(self.args['PollInterval'])
        if delay < 30:
            delay = 30 # a minimum value

        seconds = str(delay % 60)
        minutes = str((delay / 60) % 60)
        hours = str(delay / 3600)

        self.pollDelay = hours.zfill(2) + ':' + \
                         minutes.zfill(2) + ':' + \
                         seconds.zfill(2)
        
        # datasets still not initialized
        self.datasets = None
                
        # database connection not initialized
        self.database = None
        
        # message service instance
        self.ms = None 
        
        # by default merge is not forced for any dataset
        self.forceMergeList = []

        # dataset to be removed
        self.toBeRemoved = []

        # trigger
        self.trigger = None
       
    ##########################################################################
    # Connect to DBS waiting if necessary
    ##########################################################################
 
    def connectToDBS(self):
        """
        _connectToDBS_

        connect to DBS

        Arguments:

          none

        Return:

          none

        """

        connected = False

        while not connected:
            try:
                self.dbsReader = DBSReader(self.args["ReadDBSURL"])

            except (DBSReaderError, DbsConnectionError), ex:
                logging.error("""Failing to connect to DBS: (exception: %s)
                                 trying again in %s seconds""" % \
                                 (str(ex), self.delayDBS))
                time.sleep(self.delayDBS)

            else:
                connected = True

    ##########################################################################
    # add SE names to white list
    ##########################################################################

    def addWhitelistSE(self, sename):
        """
        _addWhitelistSE_

        Add a SE to the whitelist
        
        Arguments:
        
          sename -- SE name to add
          
        Return:
        
          none

        """
        
        if sename not in self.seWhitelist:
            self.seWhitelist.append(sename)
        return

    ##########################################################################
    # add SE name to black list
    ##########################################################################

    def addBlacklistSE(self, sename):
        """
        _addBlacklistSE_

        Add a SE to the blacklist

        Arguments:
        
          sename -- SE name to add
          
        Return:
        
          none

        """
        if sename not in self.seBlacklist:
            self.seBlacklist.append(sename)
        return

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

        # new dataset events
        if event == "NewDataset":
            logging.info("New Dataset: %s" % payload)
            self.newDataset(payload)
            return

        # start debug event
        if event == "MergeSensor:StartDebug":
            logging.getLogger().setLevel(logging.DEBUG)
            return

        # stop debug event
        if event == "MergeSensor:EndDebug":
            logging.getLogger().setLevel(logging.INFO)
            return

        # force merge event
        if event == "ForceMerge":
            self.forceMerge(payload)
            return

        # temporary stop event
        if event == "MergeSensor:TemporaryStop":
            self.temporaryStop(payload)
            return
        
        # restart
        if event == "MergeSensor:Restart":
            self.restart(payload)
            return

        # limit number of jobs
        if event == "MergeSensor:LimitNumberOfJobs":
            self.limitNumberOfJobs(payload)
            return

        # remove limits on number of jobs
        if event == "MergeSensor:NoJobLimits":
            self.noJobLimits()
            return

        # resubmit a Merge job
        if event == "MergeSensor:ReSubmit":
            self.reSubmit(payload)
            return

        # close a dataset
        if event == "MergeSensor:CloseDataset":
            self.stopWatching(payload)
            return

        # close a request
        if event == "CloseRequest":
            self.closeRequest(payload)
            return

        # poll DBS
        if event == "MergeSensor:pollDBS":
            self.poll()
            return

        # set policy plugin
        if event == "MergeSensor:SetPolicy":
            self.args['MergePolicy'] = payload
            logging.info("Merge policy set to: %s" % payload)
            policy = self.loadMergePolicy()
            if policy == None:
                msg = "Problems loading merge policy plugin %s" % str(payload)
                logging.error(msg)
            return

        # wrong event
        logging.debug("Unexpected event %s, ignored" % event)

    ##########################################################################
    # handle a new dataset event
    ##########################################################################

    def newDataset(self, workflowFile):
        """
        _newDataset_

        start watching the dataset with the Id provided to work out
        if it needs merging.

        Arguments:
            
          workflowFile -- WorkflowSpecFile describing dataset
          
        Return:
            
          none
          
        """
       
        # do not merge merged datasets
        if workflowFile.endswith("-merge.xml"):
            logging.info("Ignoring workflow file %s, it is a merged dataset" \
                         % workflowFile)
            return
        
        # add info in database
        try:
            datasetIdList = self.datasets.add(workflowFile)
        except (InvalidDataTier, MergeSensorError, InvalidDataset), ex:
            logging.error(str(ex))
            return

        # read workflow
        procSpec = WorkflowSpec()
        try:
            procSpec.load(workflowFile)
        except Exception, msg:
            logging.error("Cannot read workflow file: " + str(msg))
            return

        # generate merge worflows for all datasets
        mergeWFs = createMergeJobWorkflow(procSpec, self.fastMerge, self.doCleanUp)

        # create workflows for each dataset
        for watchedDatasetName, mergeWF in mergeWFs.items():

            # add bare cfg template to workflow
            cmsRun = mergeWF.payload
            cfg = CMSSWConfig()
            cmsRun.cfgInterface = cfg
            cfg.sourceType = "PoolSource"
            cfg.setInputMaxEvents(-1)
            outMod = cfg.getOutputModule("Merged")
            
            
            # save it
            fileName = watchedDatasetName.replace('/','#') + '-workflow.xml'
            workflowPath = os.path.join(self.args['MergeJobSpecs'], \
                                        fileName)
            mergeWF.save(workflowPath) 

            # If the workflow doesnt exist in the cache,
            # we write the WorkflowSpec and publish a NewWorkflow
            # event for it, so that the JobCreator gets a chance
            # to create a template for it for bulk submission

            # insert it into the database
            newWorkflow = self.database.insertWorkflow(watchedDatasetName, \
                                                       workflowPath)

            # is it a new workflow?
            if newWorkflow:
                
                # commit changes in database
                self.database.commit()

                # publish the message NewWorkflow
                self.ms.publish("NewWorkflow", workflowPath)
                self.ms.commit()

        # ignore not accepted datasets, logging messages already displayed
        if datasetIdList == []:
            return
        
        # log information
        logging.info("New Datasets: %s" % datasetIdList)

        return
   
    ##########################################################################
    # handle a force merge event
    ##########################################################################

    def forceMerge(self, datasetPath):
        """
        _forceMerge_

        Add the dataset specified in the payload to the list of datasets for
        which a forced merge should be performed.
 
        Arguments:

          datasetPath -- The dataset path

        Return:

          none

        """

        # get list of datasets and forced merge status
        datasetList = self.datasets.list()
        forceMergeList = self.forceMergeList

        # verify if it is currently watched
        if not datasetPath in datasetList:
            logging.error("Cannot force merge on non watched dataset %s" %
                          datasetPath)
            return

        # verify it is not currently in forced merge status
        if datasetPath in forceMergeList:
            logging.error("Forced merge already set for dataset %s" %
                          datasetPath)
            return

        # add to the list of datasets with force merge in next DBS poll cycle
        self.forceMergeList.append(datasetPath)

    ##########################################################################
    # handle a temporary stop event
    ##########################################################################

    def temporaryStop(self, payload):
        """
        _temporaryStop_

        used to temporary stop MergeSensor operation

        Arguments:
            
          payload -- empty string or dataset name
          
        Return:
            
          none
          
        """

        # empty payload means stop all processing
        if payload == "":
            
            # open a new connection (to avoid interfere with transactions
            # in the other thread)
            database = MergeSensorDB()
        
            # get database status
            status = database.getStatus()
        
            # check running condition
            if status['status'] == 'stopped':
                logging.error("MergeSensor is already in stopped condition!")
                database.closeDatabaseConnection()
                return
        
            # set it to stopped
            newStatus = {'status':'stopped'}
            database.setStatus(newStatus)
            database.commit()

            # log message
            logging.info("Temporary stopped.")
        
            # close connection and return
            database.closeDatabaseConnection()
            return

        # stop processing a particular dataset
        datasetName = payload
        
        # open a new connection (to avoid interfere with transactions
        # in the other thread)
        database = MergeSensorDB()
        
        # get dataset info
        try:
            datasetInfo = database.getDatasetInfo(datasetName)
        
        # it does not exist
        except DatasetNotInDatabase:
            
            logging.error( \
              "Processing on dataset %s cannot be blocked, not watched." \
              % datasetName)
            database.closeDatabaseConnection()
            return
        
        # get status
        status = datasetInfo['status']
        
        # check it
        if status == 'closed':
            
            # it is already stopped
            logging.warning( \
              "Processing on dataset %s is already blocked." % datasetName)
            database.closeDatabaseConnection()
            return
        
        # close it
        database.closeDataset(datasetName)
        database.commit()
        
        # log message
        logging.info("Processing of dataset %s blocked" % datasetName)

        # close connection and return
        database.closeDatabaseConnection()
        return
    
    ##########################################################################
    # handle a restart event
    ##########################################################################

    def restart(self, payload):
        """
        _restart_

        used to restart MergeSensor operation

        Arguments:
            
          payload -- empty string or dataset name
          
        Return:
            
          none
          
        """

        
        # empty payload means stop all processing
        if payload == "":
            
            # open a new connection (to avoid interfere with transactions in the
            # other thread)
            database = MergeSensorDB()
        
            # get database status
            status = database.getStatus()
        
            # check running condition
            if status['status'] == 'running':
                logging.error("MergeSensor is already in running condition!")
                database.closeDatabaseConnection()
                return
        
            # set it to stopped
            newStatus = {'status':'running'}
            database.setStatus(newStatus)
            database.commit()
        
            # log message
            logging.info("Restarting operation")
        
            # close connection and return
            database.closeDatabaseConnection()
            return 

        # restart processing a particular dataset
        datasetName = payload
        
        # open a new connection (to avoid interfere with transactions
        # in the other thread)
        database = MergeSensorDB()
        
        # get dataset info
        try:
            datasetInfo = database.getDatasetInfo(datasetName)
        
        # it does not exist
        except DatasetNotInDatabase:
            
            logging.error( \
              "Processing on dataset %s cannot be restarted, not watched." \
              % datasetName)
            database.closeDatabaseConnection()
            return
        
        # get status
        status = datasetInfo['status']
        
        # check it
        if status == 'open':
            
            # it is already running
            logging.warning( \
              "Asked restart processing on dataset %s, which is not stopped!" \
                   % datasetName)
            database.closeDatabaseConnection()
            return
        
        # open it
        database.startTransaction()
        database.updateDataset(datasetName)
        database.commit()
        
        # log message
        logging.info("Processing of dataset %s restarted" % datasetName)

        # close connection and return
        database.closeDatabaseConnection()
        return

    ##########################################################################
    # handle a limit number of jobs event
    ##########################################################################

    def limitNumberOfJobs(self, payload):
        """
        _restart_

        used to restart MergeSensor operation

        Arguments:
            
          payload -- the number of jobs
          
        Return:
            
          none
          
        """

        # get number of allows jobs
        try:
            remainingJobs = int(payload)
        except ValueError:
            logging.error("Wrong limit on the number of allowed jobs: %s" \
                          % payload)
            return
        
        # open a new connection (to avoid interfere with transactions in the
        # other thread)
        database = MergeSensorDB()
        
        # get database status
        status = database.getStatus()
        
        # check running condition
        if status['status'] == 'stopped':
            logging.warning( \
                 "MergeSensor is already in stopped, limit has no effect!")
        
        # limit processing
        newStatus = {'limited':'yes', 'remainingjobs':remainingJobs}
        database.setStatus(newStatus)
        database.commit()
        
        # log message
        logging.info("Processing limited to %s jobs." % remainingJobs)

        # close connection
        database.closeDatabaseConnection()

    ##########################################################################
    # no limits on job generations
    ##########################################################################

    def noJobLimits(self):
        """
        _restart_

        used to remove the job limitations

        Arguments:
            
          none
          
        Return:
            
          none
          
        """

        # open a new connection (to avoid interfere with transactions in the
        # other thread)
        database = MergeSensorDB()
        
        # get database status
        status = database.getStatus()
        
        # check running condition
        if status['limited'] == 'no':
            logging.error( \
                 "Asked to remove limits, and no limit is in effect!")
            database.closeDatabaseConnection()
            return
        
        # remove limit processing
        newStatus = {'limited':'no', 'remainingjobs':'0'}
        database.setStatus(newStatus)
        database.commit()
        
        # log message
        logging.info("Removed limit to job generation.")

        # close connection
        database.closeDatabaseConnection()

    ##########################################################################
    # resubmit a merge job
    ##########################################################################

    def reSubmit(self, payload):
        """
        _reSubmit_

        used to resubmit a merge job

        Arguments:
            
          the job name
          
        Return:
            
          none
          
        """

        # get jobName
        jobName = payload
        
        # open a new connection (to avoid interfere with transactions in the
        # other thread)
        database = MergeSensorDB()
        
        # redo job
        try:
            database.redoJob(jobName)

        except Exception, msg:
            logging.error(msg)
            database.closeDatabaseConnection()
            return

        # commit changes
        database.commit()
        
        # log message
        logging.info("Flagged job %s for resubmission." % jobName)

        # close connection
        database.closeDatabaseConnection()

    ##########################################################################
    # handle close dataset event
    ##########################################################################

    def stopWatching(self, datasetPath):
        """
        _stopWatching_

        Stop watching dataset, removing all associated information from the
        list of watched datasets, and also from the database.
 
        Arguments:

          datasetPath -- The dataset path

        Return:

          none

        """

        # get list of datasets
        datasetList = self.datasets.list()

        # verify if it is currently watched
        if not datasetPath in datasetList:
            logging.error("Cannot stop watching non watched dataset %s" %
                          datasetPath)
            return

        # add it to the list of datasets to be removed
        self.toBeRemoved.append(datasetPath)

        logging.info("Dataset %s scheduled for closing operation." % \
                     datasetPath)

    ##########################################################################
    # handle close request event
    ##########################################################################

    def closeRequest(self, workflowFile):
        """
        _closeRequest_

        Stop watching request, removing all associated information from the
        list of watched datasets, and also from the database for all
        datasets involved in the request
 
        Arguments:

          workFlow file -- The workflow file

        Return:

          none

        """

        # read the WorkflowSpecFile
        try:
            wfile = WorkflowSpec()
            wfile.load(workflowFile)

        # wrong dataset file
        except Exception, msg:
            logging.error( \
               "Error loading workflow specifications from %s (%s)" \
                          % (workflowFile, msg))
            return

        # get output dataset names
        try:
            outputDatasetsList = wfile.outputDatasets()
            
            outputDatasetsList = ["/%s/%s/%s" % (ds['PrimaryDataset'], \
                                                    ds['DataTier'], \
                                                    ds['ProcessedDataset']) \
                                   for ds in outputDatasetsList]
            
        except Exception, msg:
            
            logging.error("Error getting output datasets from %s" \
                          % workflowFile)
            return

        # get list of currently watched datasets
        datasetList = self.datasets.list()
        
        toBeRemovedList = []
        
        # verify list of datasets to be removed
        for ds in outputDatasetsList:
            
            # if found, then add to the list of datasets to be removed
            if ds in datasetList:
                
                toBeRemovedList.append(ds)
                
        # add refined list
        self.toBeRemoved.extend(toBeRemovedList)

        logging.info("Datasets %s scheduled for closing operation." % \
                     toBeRemovedList)

    ##########################################################################
    # poll DBS
    ##########################################################################

    def pollDBS(self, datasetPath, status):
        """
        _pollDBS_

        Check the merge conditions for the datasetId based on information
        provided in the local DBS. Create a job specification for merge
        jobs and publish a CreateJob event.

        Arguments:
            
          datasetPath -- the name of the dataset
          status -- MergeSensor status
          
        Return:
            
          none
          
        """
      
        # verify if the dataset has to be removed
        if (datasetPath in self.toBeRemoved):
            
            # remove it
            self.datasets.remove(datasetPath)
            
            # also from the list
            self.toBeRemoved.remove(datasetPath)
           
            logging.info("Dataset %s closed." % datasetPath)
            
            return
        
        # check for limits
        if status["limited"] == "yes" and status["remainingjobs"] == 0:
            
            # nothing to do (log message displayed before)
            return

        # get dataset information
        try:
            datasetStatus = self.database.getDatasetInfo(datasetPath)
        
        # removed
        except DatasetNotInDatabase:
            raise

        # check for blocked condition
        if datasetStatus['status'] == 'closed':
            logging.info("Polling DBS for %s blocked" % datasetPath)
            return
        
        # log information
        logging.info("Polling DBS for %s" % datasetPath)

        # check forced merge condition
        forceMerge = datasetPath in self.forceMergeList
 
        if forceMerge: 
            logging.info("Forced merge on dataset %s" % datasetPath)
 
        # get file list in dataset
        fileList = self.getFileListFromDBS(datasetPath)

        # ignore empty sets
        if fileList == {}:

            # reset force merge status if set
            if (forceMerge):

                logging.info( \
                  "Forced merge does not apply to dataset %s due to %s" \
                             % (datasetPath, "empty file condition"))

                # remove dataset from forced merged datasets
                self.forceMergeList.remove(datasetPath)

            # just return
            return
       
        # update file information
        self.datasets.updateFiles(datasetPath, fileList)
        
        # verify if it can be merged
        (mergeable,
         fileBlockId,
         selectedSet,
         originalJob) = self.datasets.mergeable(datasetPath, forceMerge)
 
        # generate one job for every mergeable set of files in dataset
        while (mergeable):
       
            # get SE list
            seList = None
            while True:
                try:
                    seList = self.dbsReader.listFileBlockLocation(fileBlockId)
                    break

                # errors with the file block (?)
                except DBSReaderError, ex:
                    logging.error("DBS error: " +  str(ex) + \
                              "\nCannot get block location for file block: " \
                              + fileBlockId)
                    break

                # connection error, retry
                except DbsConnectionError, ex:
                    logging.error("DBS connection lost, retrying: " + \
                                  str(ex))
                    self.connectToDBS()

            # problems getting SE list, then suspend job generation
            if seList is None:
                break

            # apply restrictions
            seList = self.processSElist(seList)
      
            # handle special case when DBS provide no information
            if seList == None:
                seList = []
                
            # get storage element name
            if seList == []:
                storageElement = "Unknown"
            elif len(seList) == 1:
                storageElement = seList[0]
            else:
                storageElement = "OneOf-" + '-'.join(seList)

            # yes, add job info to dataset and get target name
            jobId =  "%s-%s-mergejob-%s" % (datasetStatus['workflowName'], \
                                            storageElement, \
                                            time.time())

            # define merge job
            outFile = self.datasets.addMergeJob(datasetPath, jobId, \
                                                selectedSet, originalJob)

            # get properties
            properties = self.datasets.getProperties(datasetPath)

            # dataset name
            dataset = Dataset.getNameComponents(datasetPath)

            # target dataset
            targetDatasetPath = properties['targetDatasetPath']
            targetDataset = Dataset.getNameComponents(targetDatasetPath)
            
            # build workflow and job specifications
            jobSpecFile = self.buildWorkflowSpecFile(jobId,
                                   selectedSet, dataset, targetDataset,
                                   outFile, properties, seList)

            # cannot create job spec, abandon cycle
            if jobSpecFile is None:
                break
                
            # publish CreateJob event
            self.publishCreateJob(jobSpecFile)

            # log message
            logging.info("Merge job started:")
            logging.info("  input dataset:  %s files: %s" % \
                      (datasetPath, str(selectedSet)))
            logging.info("  output dataset: %s file: %s" % \
                      (targetDatasetPath, outFile))

            # update dataset status
            newStatus = {}
            newStatus["mergedjobs"] = status["mergedjobs"] + 1
            if status["limited"] == "yes":
                newStatus["remainingjobs"] = status["remainingjobs"] - 1
            
            # update MergeSensor status
            self.database.setStatus(newStatus)
            status = self.database.getStatus()
            
            # check for limits
            if status["limited"] == "yes" and newStatus["remainingjobs"] == 0:
                mergeable = False
                logging.info( \
                    "Limit on the number of generated jobs has been reached.")
                logging.info(" trying again in %s seconds." % \
                             self.args['PollInterval'])
                break

            # verify again the same dataset for another set
            (mergeable,
             fileBlockId,
             selectedSet,
             originalJob) = self.datasets.mergeable(datasetPath, forceMerge)

        # reset forceMerge status if set
        if (forceMerge):

            # remove dataset from forced merged datasets
            self.forceMergeList.remove(datasetPath)

        return

    ##########################################################################
    # build the merge job specification file
    ##########################################################################

    def buildWorkflowSpecFile(self, jobId, fileList, dataset, targetDataset,
                              outputFile, properties,
                              seList):
        """
        _buildWorkflowSpecFile_
        
        Build a workflow specification for the merge job. The xml file
        is stored in the control directory, with name:
            
            <jobId>--WorkflowSpec.xml
        
        Arguments:
            
          jobId -- the name of the job
          fileList -- the list of files to be merged
          dataset -- the name of the dataset
          targetDataset -- the target dataset name
          outputFile -- the name of the merged file
          properties -- dataset properties
          seList -- storage element lists associated to file block
                    
        Return:
            
         merge job spec (or None if there are problems)
          
        """

        # get Merge Sensor status
        status = self.database.getStatus()

        # get dataset properties
        tier = properties['dataTier']
        lfnBase = properties["mergedLFNBase"] 

        # compute LFN group based on merge jobs counter
        group = str(status['mergedjobs'] // 1000).zfill(4)
        lfnBase = "%s/%s" % (lfnBase, group)
        
        # create workflow
        spec = WorkflowSpec()
        fileName = '#' + '#'.join(dataset) + '-workflow.xml'
        workflowPath = os.path.join(self.args['MergeJobSpecs'], \
                                        fileName)
        try:
            spec.load(workflowPath)
        except Exception, msg:
            logging.error("cannot load base workflow file: " + str(msg))
            return None

        # create job specification
        jobSpec = spec.createJobSpec()
        jobSpec.setJobName(jobId)
        jobSpec.setJobType("Merge")

        # add SE list
        for storageElement in seList:
            jobSpec.addWhitelistSite(storageElement)

        # get PSet
        cfg = jobSpec.payload.cfgInterface

        # set output module
        outModule = cfg.getOutputModule('Merged')

        # set output file name
        baseFileName = "%s-%s-%s.root" % (dataset[0], outputFile, tier)
        outModule['fileName'] = baseFileName
        outModule['logicalFileName'] = os.path.join(lfnBase, baseFileName)
        
        # set output catalog
        outModule['catalog'] = "%s-merge.xml" % jobId
        
        # set input module
        

        # get input file names (expects a trivial catalog on site)
        cfg.inputFiles = ["%s" % fileName for fileName in fileList]
        
        
        # target file name        
        mergeJobSpecFile = "%s/%s-spec.xml" % (
               self.args['MergeJobSpecs'], jobId)

        # save job specification
        jobSpec.save(mergeJobSpecFile)

        return mergeJobSpecFile

    ##########################################################################
    # publish a CreateJob event
    ##########################################################################

    def publishCreateJob(self, mergeSpecURL):
        """
        _publishCreateJob_

        Publish a CreateJob event with the mergeSpecURL provided
        as the payload.

        Arguments:

          mergeSpecURL -- merge specification file name

        Return:

          none

        """

        # publish event
        if self.queueMode:
            self.ms.publish("QueueJob", mergeSpecURL)
        else:
            self.ms.publish("CreateJob", mergeSpecURL)
        self.ms.commit()

        return

    ##########################################################################
    # combine the SE list of a fileblock with white and black lists
    ##########################################################################

    def processSElist(self, seList):
        """
        
        Gets the SE list associated to a fileblock after applying restrictions
        defined by the white and black lists:
        
           whiteList  blackList  result
           
           []         []         seList
           []         non empty  seList - blackList
           non empty  []         intersection(whiteList,seList)
           non empty  non empty  intersection(whitelist,seList - blackList)
            
        Arguments:

          seList -- original SE list
                       
        Return:
        
          processed SE list after applying restrictions
                                                                                
        """
        
        # empty SE list, should not be case...
        if seList == []:
            return None
        
        # get lists
        whiteList = self.seWhitelist
        blackList = self.seBlacklist
        
        # both restrictions lists are empty, return original seList
        if whiteList == [] and blackList == []:
            return seList
        
        # white list empty and black list no, return seList - blackList
        if whiteList == [] and blackList != []:
            result = []
            for elem in seList:
                if elem not in blackList:
                    result.append(elem)
            return result
    
        # black list empty and white list no, return intersection
        # between whiteList and seList
        if whiteList != [] and blackList == []:
            result = []
            for elem in seList:
                if elem in whiteList:
                    result.append(elem)
            return result
            
        # both white and black lists are non empty, compute the
        # intersection between white list and the difference
        # between seList and black list
        result1 = []
        for elem in seList:
            if elem not in blackList:
                result1.append(elem)
        
        result2 = []
        for elem in result1:
            if elem in whiteList:
                result2.append(elem)
        
        return result2
    
    ##########################################################################
    # poll cycle to DBS 
    ##########################################################################

    def poll(self):
        """
        _poll_

        Poll the DBS for changes in watched datasets

        Arguments:
            
          none
          
        Return:
            
          none
          
        """

        # get Merge Sensor status
        status = self.database.getStatus()

        # check stopped status
        if status["status"] == "stopped":
            
            # yes, blocked
            logging.info("MergeSensor operation is currently blocked.")
            logging.info(" trying again in %s seconds." % \
                         self.args['PollInterval'])
            
            # sleep for the specified time
            time.sleep(float(self.args['PollInterval']))
            return
        
        # check limited status
        if status["limited"] == "yes":
        
            # yes, limited
            remainingJobs = status["remainingjobs"]
                
            # check for number of jobs
            if remainingJobs == 0:
                
                # no more jobs allows
                logging.info( \
                  "Limit on the number of generated jobs has been reached.")
                logging.info(" trying again in %s seconds." % \
                             self.args['PollInterval'])
                
                # sleep for the specified time
                time.sleep(float(self.args['PollInterval']))
                return
            
        # get list of datasets
        datasetList = self.datasets.list()

        logging.info("Start polling DBS")

        # check DBS for all watched datasets
        for dataset in datasetList:
            
            # poll dataset
            try:
                self.pollDBS(dataset, status)
                
            # dataset was removed
            except DatasetNotInDatabase, msg:
                logging.warning(msg)
                continue
            
            # update status
            status = self.database.getStatus()

        # generate new polling DBS cycle
        self.ms.publish('MergeSensor:pollDBS', '', self.pollDelay)
        self.ms.commit()
        return
   
    ##########################################################################
    # get list of unmerged files associated to a dataset from DBS
    ##########################################################################
    
    def getFileListFromDBS(self, datasetId):
        """
        _getFileListFromDBS_
        
        Get the list of files of the dataset from DBS
        
        Arguments:
            
          datasetId -- the name of the dataset
          
        Return:
            
          dictionary with index 'file block' and ech value defined as 
          a dictionary with index 'file name' and each value defined
          as a dictionary with at least 'NumberOfEvents', 'FileSize'.
          
        """

        # list of files
        fileList = {}

        # get list of files      
        while True:
 
            try:
                blocks = self.dbsReader.getFiles(datasetId)
                break

            # cannot get information from DBS, ignore then
            except DBSReaderError, ex:
                logging.error("DBS error: %s, cannot get files for %s" % \
                              (str(ex), str(datasetId)))
                return fileList

            # connection error, retry
            except DbsConnectionError, ex:
                logging.error("DBS connection lost, retrying: " + \
                              str(ex))
                self.connectToDBS()

        # check for empty datasets
        if blocks == {}:
            return fileList

        # get all file blocks
        blockList = blocks.keys()

        # process all file blocks
        for fileBlock in blockList:

            # get fileBlockId SE information
            seList = blocks[fileBlock]['StorageElements']

            # apply restrictions
            seList = self.processSElist(seList)
            
            # add files for non blocked SE
            if seList != [] or seList == None:
           
                # add block to result
                fileList[fileBlock] = blocks[fileBlock]
   
            else:
                logging.info("fileblock %s blocked" % fileBlock)

        # return list
        return fileList
       
    ##########################################################################
    # load policy plugin
    ##########################################################################

    def loadMergePolicy(self, default = None):
        """
        _loadMergePolicy_

        Load the merge policy plugin

        """

        # get policy name
        policyName = self.args['MergePolicy']

        # check name
        if policyName == None:

            # not defined, verify default name
            if default == None:
                msg = "No merge policy selected"
                logging.error(msg)
                return None

            # use default
            else:
                policyName = default

        # load plugin
        try:
            policy = retrieveMergePolicy(policyName)

        # oops, error
        except Exception, ex:
            msg = "Exception when loading merge policy plugin: %s\n" % (
                self.args['MergePolicy'],)
            msg += str(ex)
            logging.error(msg)
            policy = None

        # set policy for datasets
        Dataset.setMergePolicy(policy)

        logging.info("Policy plugin %s loaded." % policyName)

        # return it
        return policy
         
    ##########################################################################
    # start component execution
    ##########################################################################

    def startComponent(self):
        """
        _startComponent_

        Start the merge sensor component
        
        Arguments:
        
          none
          
        Return:
        
          none

        """
       
        # create control directory if necessary
        if not os.path.exists(self.args['MergeJobSpecs']):
            os.makedirs(self.args['MergeJobSpecs'])
            
        # set DB connection and give access to it to subclasses
        self.database = MergeSensorDB()
        Dataset.setDatabase(self.database)
        WatchedDatasets.setDatabase(self.database)
        
        # give access to logging facilities for WatchedDatasets and Datasets
        WatchedDatasets.setLogging(logging)
        Dataset.setLogging(logging)
        
        # initialize dataset structure
        try:
            self.datasets = WatchedDatasets(self.args['StartMode'])
        except MergeSensorError, message:
            logging.error(message)
            return
        
        # set merged file size
        Dataset.setMergeFileSize(int(self.args['MaxMergeFileSize']), \
                                 int(self.args['MinMergeFileSize']))

        # set maximum input file failures
        Dataset.setMaxInputFailures(int(self.args['MaxInputAccessFailures']))

        # create message service instance
        self.ms = MessageService()
        
        # register
        self.ms.registerAs("MergeSensor")
        
        # subscribe to messages
        self.ms.subscribeTo("NewDataset")
        self.ms.subscribeTo("MergeSensor:StartDebug")
        self.ms.subscribeTo("MergeSensor:EndDebug")
        self.ms.subscribeTo("ForceMerge")
        self.ms.subscribeTo("MergeSensor:TemporaryStop")
        self.ms.subscribeTo("MergeSensor:Restart")
        self.ms.subscribeTo("MergeSensor:LimitNumberOfJobs")
        self.ms.subscribeTo("MergeSensor:NoJobLimits")
        self.ms.subscribeTo("MergeSensor:ReSubmit")
        self.ms.subscribeTo("MergeSensor:CloseDataset")
        self.ms.subscribeTo("CloseRequest")
        self.ms.subscribeTo("MergeSensor:pollDBS")
        self.ms.subscribeTo("MergeSensor:SetPolicy")

        # generate first polling cycle
        self.ms.remove("MergeSensor:pollDBS")
        self.ms.publish("MergeSensor:pollDBS", "")
        self.ms.commit()

        # wait for messages
        while True:
            
            # get a single message
            messageType, payload = self.ms.get()

            # commit the reception of the message
            self.ms.commit()
            
            # perform task
            self.__call__(messageType, payload)
            
        
    ##########################################################################
    # get version information
    ##########################################################################

    @classmethod
    def getVersionInfo(cls):
        """
        _getVersionInfo_
        
        return version information of all components used by
        the MergeSensor
        """
        
        return "MergeSensor: " + __version__ + \
            "\nWatchedDatasets: " + WatchedDatasets.getVersionInfo() + \
            "\nDataset: " + Dataset.getVersionInfo() + \
            "\nMergeSensorDB: " + MergeSensorDB.getVersionInfo() + "\n"
    
