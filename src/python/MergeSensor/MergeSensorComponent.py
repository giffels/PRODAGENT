#!/usr/bin/env python
"""
_MergeSensor_

Component that watches the DBS to submit merge jobs when a subset of files in
a dataset are ready the be merged.

"""

__revision__ = "$Id: MergeSensorComponent.py,v 1.29 2006/09/18 14:25:18 ckavka Exp $"
__version__ = "$Revision: 1.29 $"
__author__ = "Carlos.Kavka@ts.infn.it"

import os
import time
import inspect
import sys

# Merge sensor import
from MergeSensor.WatchedDatasets import WatchedDatasets
from MergeSensor.MergeSensorError import MergeSensorError, \
                                         InvalidDataTier, \
                                         MergeSensorDBError
from MessageService.MessageService import MessageService
from MergeSensor.Dataset import Dataset
from MergeSensor.MergeSensorDB import MergeSensorDB

# Workflow and Job specification
from MCPayloads.WorkflowSpec import WorkflowSpec
from MCPayloads.LFNAlgorithm import mergedLFNBase, unmergedLFNBase
from CMSConfigTools.CfgInterface import CfgInterface

# DBS CGI API
from dbsCgiApi import DbsCgiApi
from dbsException import DbsException
from dbsProcessedDataset import DbsProcessedDataset

# DLS
from dlsDataObjects import dlsApi
import dlsClient

# threads
from threading import Thread, Condition

# logging
import logging
from logging.handlers import RotatingFileHandler

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
        self.args.setdefault("DBSURL",
          "http://cmsdoc.cern.ch/cms/test/aprom/DBS/CGIServer/prodquery")
        self.args.setdefault("DBSAddress", None)
        self.args.setdefault("DBSType", "CGI")
        self.args.setdefault("ComponentDir", None)
        self.args.setdefault("PollInterval", 30 )
        self.args.setdefault("Logfile", None)
        self.args.setdefault("StartMode", 'warm')
        self.args.setdefault("DBSDataTier", "GEN,SIM,DIGI")
        self.args.setdefault("DLSType", None)
        self.args.setdefault("DLSAddress", None)
        self.args.setdefault("MergeFileSize", 1000000000)

        # default SE white/black lists are empty
        self.args.setdefault("MergeSiteWhitelist", None)
        self.args.setdefault("MergeSiteBlacklist", None)

        # fastMerge
        self.args.setdefault("FastMerge", None)
        
        # update parameters
        self.args.update(args)

        # merge file size
        self.args["MergeFileSize"] = int(self.args["MergeFileSize"])

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
        
        # server directory
        self.args.setdefault("MergeJobSpecs", os.path.join(
            self.args['ComponentDir'], "merge-jobspecs"))

        # define log file
        if self.args['Logfile'] == None:
            self.args['Logfile'] = os.path.join(self.args['ComponentDir'], 
                                                "ComponentLog")
        # create log handler
        logHandler = RotatingFileHandler(self.args['Logfile'],
                                         "a", 1000000, 3)

        # define log format
        logFormatter = logging.Formatter("%(asctime)s:%(message)s")
        logHandler.setFormatter(logFormatter)
        logging.getLogger().addHandler(logHandler)
        logging.getLogger().setLevel(logging.INFO)

        # inital log information
        logging.info("MergeSensor starting... in >> %s << mode" % 
                     self.args["StartMode"])
        logging.info("with MergeFileSize = %s" % self.args['MergeFileSize'])
        logging.info("     MergeSiteWhitelist = %s" % self.seWhitelist)
        logging.info("     MergeSiteBlacklist = %s" % self.seBlacklist)
        if self.fastMerge:
            logging.info("Using EDM fast merge.")
        else:
            logging.info("Using cmsRun merge.")
            
        # check DBS type
        if self.args['DBSType'] != 'CGI':
            logging.error("Fatal error: only CGI DBS supported")
            sys.exit(1)

        # get DBS API
        connected = False
        self.delayDBS = 120
        
        while not connected:
            try:
                self.dbsApi = self.connectDBS()
                
            except DbsException, ex:
                logging.error( \
                    "Failing to connect to DBS: (exception: %s)" % ex)
                logging.error("  trying again in %s seconds" % self.delayDBS)
                time.sleep(self.delayDBS)

            else:
                connected = True
        
        # get DLS API
        connected = False
        self.delayDLS = 120

        while not connected:
            try:
                self.dlsApi = self.connectDLS()

            except dlsApi.DlsApiError, ex:
                logging.error( \
                    "Failing to connect to DLS: (exception: %s)" % ex)
                logging.error("  trying again in %s seconds" % self.delayDBS)
                time.sleep(self.delayDLS)

            else:
                connected = True
 
        # merge workflow specification
        thisModule = os.path.abspath(
                          inspect.getsourcefile(MergeSensorComponent))
        baseDir = os.path.dirname(thisModule)        
        mergeWorkflow = os.path.join(baseDir, "mergeConfig.py")
        self.mergeWorkflow = file(mergeWorkflow).read()

        # datasets still not initialized
        self.datasets = None
                
        # database connection not initialized
        self.database = None
        
        # create thread synchronization condition variable
        self.cond = Condition()
 
        # message service instances
        self.ms = None 
        self.msThread = None
        
        # by default merge is not forced for any dataset
        self.forceMergeList = []

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
            self.noJobLimits(payload)
            return

        # resubmit a Merge job
        if event == "MergeSensor:ReSubmit":
            self.reSubmit(payload)
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
       
        # critical region start
        self.cond.acquire()
                                                                                
        # add info
        try:
            datasetId = self.datasets.add(workflowFile)
        except (InvalidDataTier, MergeSensorError), ex:
            self.cond.release()
            logging.error(ex)
            return
        
        # critical region end
        self.cond.release()
                                                                                
        # ignore not accepted datasets, logging messages already displayed
        if datasetId is None:
            return
        
        # log information
        logging.info("New Dataset %s" % datasetId)

        # create workflow specification for new dataset
        spec = WorkflowSpec()
                                                                                
        # critical region start
        self.cond.acquire()
                                                                                
        # get parameters from source dataset
        properties = self.datasets.getProperties(datasetId)
        
        # critical region end
        self.cond.release()
                                                                                
        # get dataset info
        primary = properties["primaryDataset"]
        processed = properties["processedDataset"]
        tier = properties["dataTier"]
        workflowName = properties["workflowName"]
        category = properties["category"]
        version = properties["version"]
        timeStamp = properties["timeStamp"]
        psethash = properties["PSetHash"]
        secondaryOutputTiers = properties["secondaryOutputTiers"]
                                                 
        # set workflow values
        spec.setWorkflowName(workflowName)
        spec.setRequestCategory(category)
        spec.setRequestTimestamp(timeStamp)
        mergedLFNBase(spec)
        unmergedLFNBase(spec)
        
        # create a dummy task to pass information to DBS Component
        dummyTask = spec.payload
        dummyTask.name = "dummyTask"
        dummyTask.type = "CMSSW"
        dummyTask.application["Project"] = "CMSSW"
        dummyTask.application["Version"] = version
        dummyTask.application["Architecture"] = "slc3_ia32_gcc323"
        dummyTask.application["Executable"] = "cmsRun"
                                                                                
        # define output dataset
        out = dummyTask.addOutputDataset(primary, processed + "-merged", \
                                         "Merged")
        # define output dataset properties
        out["DataTier"] = tier
        out["ApplicationName"] = dummyTask.application["Executable"]
        out["ApplicationProject"] = dummyTask.application["Project"]
        out["ApplicationVersion"] = dummyTask.application["Version"]
        out["ApplicationFamily"] = "Merged"
        out["PSetHash"] = psethash;

        # add secondary output datasets
        for outDS in secondaryOutputTiers:
            out = dummyTask.addOutputDataset(primary, processed + "-merged", \
                                             "Merged")

            # define output dataset properties
            out["DataTier"] = outDS
            out["ApplicationName"] = dummyTask.application["Executable"]
            out["ApplicationProject"] = dummyTask.application["Project"]
            out["ApplicationVersion"] = dummyTask.application["Version"]
            out["ApplicationFamily"] = "Merged"
            out["PSetHash"] = psethash; 
           
        # set empty configuration
        dummyTask.configuration = ""
                                                                                
        # save it
        mergeNewDatasetFile = "%s/newdataset-%s-merge.xml" % (
               self.args['MergeJobSpecs'], time.time())
        spec.save(mergeNewDatasetFile)
                                                                                
        # publish new dataset event
        self.publishNewDataset(mergeNewDatasetFile)
        
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

        # critical region start
        self.cond.acquire()

        # get list of datasets and forced merge status
        datasetList = self.datasets.list()
        forceMergeList = self.forceMergeList

        # critical region end
        self.cond.release()

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

        # critical region start
        self.cond.acquire()

        # add to the list of datasets with force merge in next DBS poll cycle
        self.forceMergeList.append(datasetPath)

        # critical region end
        self.cond.release()

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
        except MergeSensorDBError, msg:
            
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
        except MergeSensorDBError, msg:
            
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
        except ValueError, msg:
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

    def noJobLimits(self, payload):
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
                
        # check for limits
        if status["limited"] == "yes" and status["remainingjobs"] == 0:
            
            # nothing to do (log message displayed before)
            return

        # check for blocked condition
        datasetStatus = self.database.getDatasetInfo(datasetPath)
        if datasetStatus['status'] == 'closed':
            logging.info("Polling DBS for %s blocked" % datasetPath)
            return
        
        # log information
        logging.info("Polling DBS for %s" % datasetPath)

        # critical region start
        self.cond.acquire()

        # check forced merge condition
        forceMerge = datasetPath in self.forceMergeList
 
        if forceMerge: 
            logging.info("Forced merge on dataset %s" % datasetPath)
 
        # critical region end
        self.cond.release()

        # get file list in dataset        
        fileList = self.getFileListFromDBS(datasetPath)
       
        # ignore empty sets
        if fileList == []:

            # reset force merge status if set
            if (forceMerge):

                logging.info( \
                  "Forced merge does not apply to dataset %s due to %s" \
                             % (datasetPath, "empty file condition"))

                # critical region start
                self.cond.acquire()

                # remove dataset from forced merged datasets
                self.forceMergeList.remove(datasetPath)

                # critical region end
                self.cond.release()

            # just return
            return
       
        # critical region start
        self.cond.acquire()

        # update file information
        self.datasets.updateFiles(datasetPath, fileList)
        
        # verify if it can be merged
        (mergeable,
         selectedSet,
         fileBlockId,
         oldFile) = self.datasets.mergeable(datasetPath, forceMerge)
 
        # critical region end
        self.cond.release()

        # generate one job for every mergeable set of files in dataset
        while (mergeable):
       
            # get SE list
            seList = self.getSElist(fileBlockId) 
        
            # apply restrictions
            seList = self.processSElist(seList)
      
            # handle special case when DLS provide no information
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

            # critical region start
            self.cond.acquire()

            # define merge job
            outFile = self.datasets.addMergeJob(datasetPath, selectedSet, \
                                                jobId, oldFile)

            # get properties
            properties = self.datasets.getProperties(datasetPath)

            # critical region end
            self.cond.release()
        
            # build specification files
            dataTier = properties["dataTier"]
            secondaryOutputTiers = properties["secondaryOutputTiers"]
            
            # dataset name
            dataset = Dataset.getNameComponents(datasetPath)
            
            # build workflow and job specifications
            jobSpecFile = self.buildWorkflowSpecFile(jobId,
                                   selectedSet, dataset, outFile,
                                   fileBlockId, properties, seList)

            # publish CreateJob event
            self.publishCreateJob(jobSpecFile)

            # log message
            logging.info("Merge job started:")
            logging.info("  input dataset:  %s files: %s" % \
                      (datasetPath, str(selectedSet)))
            logging.info("  output dataset: /%s/%s/%s-merged file: %s" % \
                      (dataset[0],dataTier,dataset[2],outFile))
            for secondaryTier in secondaryOutputTiers:
                logging.info("  output dataset: /%s/%s/%s-merged file: %s" % \
                          (dataset[0],secondaryTier,dataset[2],outFile))

            # update dataset status
            newStatus = {}
            newStatus["mergedjobs"] = status["mergedjobs"] + 1
            if status["limited"] == "yes":
                newStatus["remainingjobs"] = status["remainingjobs"] - 1
            
            #critical region start
            self.cond.acquire()

            # update MergeSensor status
            self.database.setStatus(newStatus)
            status = self.database.getStatus()
            
            # critical region end
            self.cond.release()
            
            # check for limits
            if status["limited"] == "yes" and newStatus["remainingjobs"] == 0:
                mergeable = False
                logging.info( \
                    "Limit on the number of generated jobs has been reached.")
                logging.info(" trying again in %s seconds." % \
                             self.args['PollInterval'])
                break

            #critical region start
            self.cond.acquire()
 
            # verify again the same dataset for another set
            (mergeable,
             selectedSet,
             fileBlockId,
             oldFile) = self.datasets.mergeable(datasetPath, forceMerge)

            # critical region end
            self.cond.release()

        # reset forceMerge status if set
        if (forceMerge):

            # critical region start
            self.cond.acquire()

            # remove dataset from forced merged datasets
            self.forceMergeList.remove(datasetPath)

            # critical region end
            self.cond.release()

        return

    ##########################################################################
    # build the merge job specification file
    ##########################################################################

    def buildWorkflowSpecFile(self, jobId, fileList, dataset,
                              outputFile, fileBlockId, properties,
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
          outputFile -- the name of the merged file
          fileBlockId -- the file block id
          properties -- dataset properties
          seList -- storage element lists associated to file block
                    
        Return:
            
          none
          
        """

        # get dataset properties
        workflowName = properties['workflowName']
        tier = properties['dataTier']
        pollTier = properties['pollTier']
        category = properties["category"]
        version = properties["version"]
        timeStamp = properties["timeStamp"]
        lfnBase = properties["mergedLFNBase"]
        psethash = properties["PSetHash"]
        secondaryOutputTiers = properties["secondaryOutputTiers"]

        # create a new workflow
        spec = WorkflowSpec()
        
        # set its properties
        spec.setWorkflowName(workflowName)
        spec.setRequestCategory(category)
        spec.setRequestTimestamp(timeStamp)
        
        # describe it as a cmsRun job
        cmsRun = spec.payload
        cmsRun.name = "cmsRun1"
        cmsRun.type = "CMSSW"
        cmsRun.application["Project"] = "CMSSW"
        cmsRun.application["Version"] = version
        cmsRun.application["Architecture"] = "slc3_ia32_gcc323"
        
        # specify merge job type
        if self.fastMerge:
            cmsRun.application["Executable"] = "EdmFastMerge"
        else:
            cmsRun.application["Executable"] = "cmsRun"
            
        # input dataset (primary, processed)
        inputDataset = cmsRun.addInputDataset(dataset[0], dataset[2])
        inputDataset["DataTier"] = pollTier         
        
        # output dataset (primary, processed, module name, tier)
        outputDataset = cmsRun.addOutputDataset(dataset[0], \
                                                dataset[2]+"-merged", \
                                                "Merged")
        outputDataset["DataTier"] = tier
        outputDataset["PSetHash"] = psethash

        # add secondary output datasets
        for outDS in secondaryOutputTiers:
            outputDataset = cmsRun.addOutputDataset(dataset[0],
                                                    dataset[2]+"-merged", \
                                                   "Merged")
            outputDataset["DataTier"] = outDS
            outputDataset["PSetHash"] = psethash
           
        # get PSet
        cfg = CfgInterface(self.mergeWorkflow, True)
        
        # set output module
        outModule = cfg.outputModules['Merged']

        # set output file name
        baseFileName = "%s-%s-%s.root" % (dataset[0], outputFile, tier)
        outModule.setFileName(baseFileName)
        outModule.setLogicalFileName(os.path.join(lfnBase, baseFileName))

        # set output catalog
        outModule.setCatalog("%s-merge.xml" % jobId)

        # set input module
        inModule = cfg.inputSource

        # get input file names (expects a trivial catalog on site)
        inputFiles = ["%s" % fileName for fileName in fileList]

        inModule.setFileNames(*inputFiles)

        # get configuration from template
        cmsRun.configuration = str(cfg)

        # generate merge and unmerged specifications
        mergedLFNBase(spec)
        unmergedLFNBase(spec)


        # add stage out 
        stageOut = cmsRun.newNode("stageOut1")
        stageOut.type = "StageOut"
        stageOut.application["Project"] = ""
        stageOut.application["Version"] = ""
        stageOut.application["Architecture"] = ""
        stageOut.application["Executable"] = "RuntimeStageOut.py" 
        stageOut.configuration = ""
        
        # Clone the workflow into a job spec and set the job name
        jobSpec = spec.createJobSpec()
        jobSpec.setJobName(jobId)
        jobSpec.setJobType("Merge")

        # add SE list
        for storageElement in seList:
            jobSpec.addWhitelistSite(storageElement)
 
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
        self.msThread.publish("CreateJob", mergeSpecURL)
        self.msThread.commit()

        return

    ##########################################################################
    # publish a NewDataset event
    ##########################################################################
    
    def publishNewDataset(self, mergeNewDatasetFile):
        """
        Arguments:
        
          mergeNewDatasetFile -- dataset specification file name
            
        Return:
                                                                                
          none
                                                                                
        """

        # publish event
        self.ms.publish("NewDataset", mergeNewDatasetFile)
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
        # critical region start
        self.cond.acquire()

        # get Merge Sensor status
        status = self.database.getStatus()

        # critical region end
        self.cond.release()

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
            
        # critical region start
        self.cond.acquire()

        # get list of datasets
        datasetList = self.datasets.list()

        # critical region end
        self.cond.release()

        logging.info("Start polling DBS")

        # check DBS for all watched datasets
        for dataset in datasetList:
            
            # poll dataset
            self.pollDBS(dataset, status)
            
            # critical region start
            self.cond.acquire()

            # update status
            status = self.database.getStatus()

            # critical region end
            self.cond.release()

        # sleep for the specified time
        time.sleep(float(self.args['PollInterval']))
        return
   
    ##########################################################################
    # get SE list from DLS
    ##########################################################################

    def getSElist(self, fileBlockId):
        """
        return SE list associated to a file block
        """

        entryList = []
        
        # get info from DLS
        try:
            entryList = self.dlsApi.getLocations(fileBlockId)

        # DLS is not answering properly (or fileBlock is not defined!)
        except Exception, ex:
 
            # wait for connection to DLS
            connected = False
            while not connected:
                
                # try to reconnect
                try:
                    self.dlsApi = self.connectDLS()

                except Exception, ex:
                    logging.error( \
                       "  failing to reconnect to DLS (exception: %s)" % ex)
                    logging.error( \
                       "  trying again in %s seconds" % self.delayDLS)
                    time.sleep(self.delayDLS)

                else:
                    connected = True
                    
            # reconnection OK, insist to get information
            try:
                entryList = self.dlsApi.getLocations(fileBlockId)

            # connection is fine, but data is wrong, follow agreement
            except Exception, ex:
                logging.warning( \
                   "DLS error, assuming empty SE for fileblock %s" % \
                   fileBlockId)

                # try again later
                return []
           
        # build SE list
        seList = []
        for entry in entryList:
            for loc in entry.locations:
                seList.append(str(loc.host))

        return seList

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
            
          list of tuples (name,size,fileBlockId) for all files in dataset
          
        """

        # list of files
        fileList = []

        # get list of files       
        blockList = self.getDatasetContents(datasetId)

        # check for empty datasets
        if blockList == []:
            return fileList

        # use CGI API
        for fileBlock in blockList:

            # get file block ID           
            fileBlockId = fileBlock.get('blockName')

            # get fileBlockId SE information
            seList = self.getSElist(fileBlockId)

            # apply restrictions
            seList = self.processSElist(seList)
            
            # add files for non blocked SE
            if seList != [] or seList == None:
            
                # append (file name,size,fileblockId)
                # to the list of files
                for aFile in fileBlock.get('fileList'):

                    name = aFile.get('logicalFileName')
                    size = aFile.get('fileSize')
                    fileList.append((name, size, fileBlockId))
                    
            else:
                logging.info("fileblock %s blocked" % fileBlockId)

        # return list
        return fileList
                
    ##########################################################################
    # connect to DBS
    ##########################################################################

    def connectDBS(self):
        """
        open a connection with the DBS server
        
        Arguments:
        
          none
          
        Return:
        
          DBS connection
          
        """

        # parameters
        url = self.args['DBSURL']
        args = {}
        args['instance'] = self.args['DBSAddress']

        # create API
        try:
            dbs = DbsCgiApi(url, args)
        except DbsException, ex:
            logging.error("Error: cannot contact DBS: %s" % ex)
            raise

        # return DBS API instance
        return dbs

    ##########################################################################
    # connect to DLS
    ##########################################################################

    def connectDLS(self):
        """
        open a connection with the DLS server
        
        Arguments:
        
          none
          
        Return:
        
          DLS connection

        """

        # create API
        try:
            dlsapi = dlsClient.getDlsApi(dls_type = self.args["DLSType"],
                                  dls_endpoint = self.args["DLSAddress"])
        except dlsApi.DlsApiError, inst:
            logging.error("Error: Cannot contact DLS: %s" % str(inst))
            raise

        # return DLS API instance
        return dlsapi

    ##########################################################################
    # get list of files of a dataset organized by fileblock
    ##########################################################################

    def getDatasetContents(self, path):
        """
        get contents of a dataset
        
        Arguments:
        
          path -- dataset name
          
        Return:
        
          list of files organized by fileblock
          
        """

        # initialize block list
        blockList = []

        # get information from DBS
        try:
            processed = DbsProcessedDataset(datasetPathName = path)
            blockList = self.dbsApi.getDatasetFileBlocks(processed)

        # DBS is not answering properly
        except Exception, ex:
            logging.error("DBS error (exception: %s)" % ex)

            # wait for connection to DBS
            connected = False
            while not connected:
                
                # try to reconnect
                try:
                    self.dbsApi = self.connectDBS()

                except Exception, ex:
                    logging.error( \
                      "  failing to reconnect to DBS: (exception: %s)" % ex)
                    logging.error( \
                      "  trying again in %s seconds" % self.delayDBS)
                    time.sleep(self.delayDBS)
                    
                else:
                    connected = True
            
            # reconnection OK, insist to get information
            logging.error("  reconnected to DBS!")
            try:
                processed = DbsProcessedDataset(datasetPathName = path)
                blockList = self.dbsApi.getDatasetFileBlocks(processed)

            # no, it does not work
            except Exception, ex:
                logging.error("DBS error (exception: %s)" % ex)

                # try again later
                return []
            
        # check for empty block
        if blockList is None:
            logging.info("Empty file block for dataset %s" % path)
            return []

        # return blockList
        return blockList

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
            logging.error(message);
            return
        
        # set merged file size
        Dataset.setMergeFileSize(int(self.args['MergeFileSize']))

        # set Datatier possible names
        Dataset.setDataTierList(self.args['DBSDataTier'])

        # create message service instances
        self.ms = MessageService()
        self.msThread = MessageService()
        
        # register
        self.ms.registerAs("MergeSensor")
        self.msThread.registerAs("MergeSensorThread")
        
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

        # start polling thread
        pollingThread = PollDBS(self.poll)
        pollingThread.start()
        
        # wait for messages
        while True:
            messageType, payload = self.ms.get()
            self.ms.commit()
            self.__call__(messageType, payload)
        
##############################################################################
# PollDBS class
##############################################################################
                                                                                
class PollDBS(Thread):
    """
    Thread that performs DBS polling 
    """

    ##########################################################################
    # thread initialization
    ##########################################################################

    def __init__(self, poll):
        """
        __init__

        Initialize thread and set polling callback

        Arguments:
        
          poll -- the DBS polling function
          
        Return:
        
          list of files organized by fileblock

        """
        Thread.__init__(self)
        self.poll = poll;

    ##########################################################################
    # thread main body
    ##########################################################################

    def run(self):
        """
        __run__

        Performs polling on DBS
        
        Arguments:
        
          none
          
        Return:
        
          none

        """

        # performs DBS polling indefinitely
        
        while True:
            self.poll()
