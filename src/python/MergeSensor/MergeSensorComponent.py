#!/usr/bin/env python
"""
_MergeSensor_

Component that watches the DBS to submit merge jobs when a subset of files in
a dataset are ready the be merged.

This component needs to be both an event subscriber and a publisher. It
subscribes to the event newDataset and publishes CreateJob events.

Original implementation by: evansde@fnal.gov  
"""

__revision__ = "$Id: MergeSensorComponent.py,v 1.3 2006/04/13 10:56:24 ckavka Exp $"
__version__ = "$Revision: 1.3 $"
__author__ = "Carlos.Kavka@ts.infn.it"

import os
import time
import socket
import re
import inspect
import sys


from MergeSensor.WatchedDatasets import WatchedDatasets
from MergeSensor.MergeSensorError import MergeSensorError
from MessageService.MessageService import MessageService
from JobState.JobStateAPI import JobStateChangeAPI                                                                                
# Workflow and Job specification
from MCPayloads.WorkflowSpec import WorkflowSpec
from CMSConfigTools.CfgInterface import CfgInterface

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
    of merging based on some predetermined criteria/algorithm

    The ProdAgentClient is implemented as a thread, while this component
    provides its own simple "server" architecture.

    """

    def __init__(self,**args):
        """
        
        Arguments:
        
          args -- all arguments from StartComponent.
          
        Return:
            
          none

        """
        
        # initialize the server
        self.args = {}
        self.args.setdefault("DBSAddress", None)
        self.args.setdefault("DBSType", "CGI")
        self.args.setdefault("ComponentDir", None)
        self.args.setdefault("PollInterval", 30 )
        self.args.setdefault("Logfile", None)
        self.args.setdefault("StartMode", 'cold')

        # update
        self.args.update(args)

        # server directories
        self.args.setdefault("WatchedDatasets", os.path.join(
            self.args['ComponentDir'], "merge-watching"))
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

        # get DBS API
        self.dbsApi = self.connectDBS()
    
        # merge workflow specification
        thisModule = os.path.abspath(
                          inspect.getsourcefile(MergeSensorComponent))
        baseDir = os.path.dirname(thisModule)        
        mergeWorkflow = os.path.join(baseDir, "mergeConfig.py")
        self.mergeWorkflow = file(mergeWorkflow).read()

        # datasets still not initialized
        self.datasets = None
                
        # create thread synchronization condition variable
        self.cond = Condition()
 
        # use message server or xmlrpc interface?
        self.ms = None 
        
        # JobState
        self.args["JobState"] = True
        self.job_state = self.args["JobState"]

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

        if event == "NewDataset":
            logging.info("New Dataset: %s" % payload)
            self.newDataset(payload)
            return

        if event == "MergeSensor:StartDebug":
            logging.getLogger().setLevel(logging.DEBUG)
            return

        if event == "MergeSensor:EndDebug":
            logging.getLogger().setLevel(logging.INFO)
            return

        logging.debug("Unexpected event %s, ignored" % event)


    def newDataset(self, workflowFile):
        """
        _newDataset_

        start watching the dataset with the Id provided to work out
        if it needs merging.

        Keeping track of datasets to be watched is done by named
        files in a directory, so that some level of persistency is
        maintained.

        Arguments:
            
          workflowFile -- WorkflowSpecFile describing dataset,
                          as generated by ReqInjComponent
          
        Return:
            
          none
          
        """
       
        # critical region start
        self.cond.acquire()
                                                                                
        # add info
        datasetId = self.datasets.add(workflowFile)

        # critical region end
        self.cond.release()
                                                                                
        # do not start merging merged-datasets
        if datasetId is None:
            logging.info("No watching started on dataset") 
            return
        
        # log information
        logging.info("New Dataset %s" % datasetId)

        # create workflow specification for new dataset
        spec = WorkflowSpec()
        spec.setWorkflowName("merge")
                                                                                
        # critical region start
        self.cond.acquire()
                                                                                
        # get parameters from source dataset
        properties = self.datasets.getProperties(datasetId)
                                                                                
        # critical region end
        self.cond.release()
                                                                                
        # get dataset info
        primary = properties["primaryDataset"]
        processed = properties["processedDataset"]
        tier = properties["realDataTier"]
                                                                                
        # create a dummy task to pass information to DBS Component
        dummyTask = spec.payload
        dummyTask.name = "dummyTask"
        dummyTask.type = "CMSSW"
        dummyTask.application["Project"] = "CMSSW"
        dummyTask.application["Version"] = "CMSSW_0_6_0_pre7"
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
        out["PSetHash"] = "12345678901234567890" # dummy value
                                                                                
        # set empty configuration
        dummyTask.configuration = ""
                                                                                
        # save it
        mergeNewDatasetFile = "%s/newdataset-%s-merge.xml" % (
               self.args['MergeJobSpecs'], time.time())
        spec.save(mergeNewDatasetFile)
                                                                                
        # publish new dataset event
        self.publishNewDataset(mergeNewDatasetFile)
        
        return
    
    def pollDBS(self, datasetId):
        """
        _pollDBS_

        Check the merge conditions for the datasetId based on information
        provided in the local DBS.

        When a merge is found, a JobSpecification for the Merge job
        is created in the MergeJobSpecs directory, and the
        CreateJob event published with the URL of that specification
        
        Arguments:
            
          datasetId -- the name of the dataset
          
        Return:
            
          none
          
        """
        
        # build dataset path
        pattern = "^\[([\w\-]+)\]\[([\w\-]+)\]\[([\w\-]+)]"
        match = re.search(pattern, datasetId)
        data = match.groups()            
        datasetPath = '/%s/%s/%s' % data
        
        # log information
        logging.info("Polling DBS for %s" % datasetPath)
    
        # get file list in dataset        
        fileList = self.getFileListFromDBS(datasetPath)
        
        # ignore empty sets
        if fileList == []:
            return
       
        # critical region start
        self.cond.acquire()

        # update it
        self.datasets.updateFiles(datasetId, fileList)
        
        # verify if it can be merged
        (mergeable, selectedSet) = self.datasets.mergeable(datasetId)
 
        # critical region end
        self.cond.release()
       
        # generate one job for every mergeable set of files in dataset
        while (mergeable):
       
            # yes, add job info to dataset and get target name
            jobId = "mergejob-%s" % time.time()

            # critical region start
            self.cond.acquire()

            # define merge job
            outFile = self.datasets.addMergeJob(datasetId, selectedSet)

            # get real data tier name 
            properties = self.datasets.getProperties(datasetId)
            tier = properties["realDataTier"]

            # critical region end
            self.cond.release()
        
            # build specification files
            pattern ="^\[([\w\-]+)\]\[([\w\-]+)\]\[([\w\-]+)]"
            match = re.search(pattern, datasetId)
            dataset = match.groups()

            # build workflow and job specifications
            jobSpecFile = self.buildWorkflowSpecFile(jobId, selectedSet,
                                                      dataset, outFile, tier)
            # publish CreateJob event
            self.publishCreateJob(jobSpecFile)

            # log message
            logging.info("Merge job started:")
            logging.info("  input dataset:  %s files: %s" % \
                      (datasetPath,str(selectedSet)))
            logging.info("  output dataset: /%s/%s/%s-merged file: %s" % \
                      (dataset[0],dataset[1],dataset[2],outFile))
             
            # critical region start
            self.cond.acquire()
 
            # verify again the same dataset for another set
            (mergeable, selectedSet) = self.datasets.mergeable(datasetId)

            # critical region end
            self.cond.release()

        return

    def buildWorkflowSpecFile(self, jobId, fileList, dataset, outputFile, tier):
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
          tier -- the real data tier name
                    
        Return:
            
          none
          
        """
        
        # create a new workflow
        spec = WorkflowSpec()
        spec.setWorkflowName("job-%s" % jobId)

        # update job state
        if self.job_state:
            try:
                JobStateChangeAPI.register(jobId, 'processing', 10, 1)
            except StandardError, ex:
                logging.error('JobStateAPI error: '+str(ex))

        # describe it as a cmsRun job
        cmsRun = spec.payload
        cmsRun.name = "cmsRun1"
        cmsRun.type = "CMSSW"
        cmsRun.application["Project"] = "CMSSW"
        cmsRun.application["Version"] = "CMSSW_0_6_0_pre7"
        cmsRun.application["Architecture"] = "slc3_ia32_gcc323"
        cmsRun.application["Executable"] = "cmsRun"
 
        # input dataset (primary, processed)
        cmsRun.addInputDataset(dataset[0], dataset[2])
         
        # output dataset (primary, processed, module name, tier)
        out = cmsRun.addOutputDataset(dataset[0], dataset[2]+"-merged", "Merged")
        out["DataTier"] = tier
 
        # get PSet
        cfg = CfgInterface(self.mergeWorkflow, True)
        
        # set output module
        outModule = cfg.outputModules['Merged']
        outModule.setFileName("file:%s-%s-merged.root" % (dataset[0],outputFile))
        outModule.setCatalog("%s-merge.xml" % jobId)

        # set input module
        inModule = cfg.inputSource

        # get input file names (expects a trivial catalog on site)
        inputFiles = ["%s" % fileName for fileName in fileList]

        inModule.setFileNames(*inputFiles)

        # get configuration from template
        cmsRun.configuration = str(cfg)

        
        
        #  //
        # // Clone the workflow into a job spec
        #//  and set the job name
        jobSpec = spec.createJobSpec()
        jobSpec.parameters['JobName'] = jobId        
        
        


        # add stage out 
        stageOut = cmsRun.newNode("stageOut1")
        stageOut.type = "StageOut"
        stageOut.application["Project"] = ""
        stageOut.application["Version"] = ""
        stageOut.application["Architecture"] = ""
        stageOut.application["Executable"] = "RuntimeStageOut.py" 
        stageOut.configuration = ""

        # target file name        
        mergeJobSpecFile = "%s/%s-spec.xml" % (
               self.args['MergeJobSpecs'], jobId)

        jobSpec = spec.createJobSpec()
        jobSpec.setJobName(jobId)

        # save job specification
        jobSpec.save(mergeJobSpecFile)

        return mergeJobSpecFile

    def publishCreateJob(self, mergeSpecURL):
        """
        _publishCreateJob_

        Publish a CreateJob event with the mergeSpecURL provided
        as the payload, this should be a file:/// URL for a
        spec in the MergeJobSpecs dir

        Arguments:

          mergeSpecURL -- file name of the merge specification file for the job

        Return:

          none

        """
        self.ms.publish("CreateJob", mergeSpecURL)
        self.ms.commit()

        return

    def publishNewDataset(self, mergeNewDatasetFile):
        """
        Arguments:
                                                                                
        Return:
                                                                                
          none
                                                                                
        """
        # generate NewDataset event
        self.ms.publish("NewDataset", mergeNewDatasetFile)
        self.ms.commit()
 
        return


    def poll(self):
        """
        _poll_

        Poll the DBS for changes in watched datasets

        Arguments:
            
          none
          
        Return:
            
          none
          
        """
        logging.info("Start polling DBS")

        # critical region start
        self.cond.acquire()

        # get list of datasets
        datasetList = self.datasets.list()

        # critical region end
        self.cond.release()

        # check DBS for all watched datasets
        for dataset in datasetList:
            self.pollDBS(dataset)

        # sleep for the specified time
        time.sleep(float(self.args['PollInterval']))
        return
    
    def getFileListFromDBS(self, datasetId):
        """
        _getFileListFromDBS_
        
        Get the list of files of the dataset from DBS
        
        Arguments:
            
          datasetId -- the name of the dataset
          
        Return:
            
          list of tuples (name,size) for all files in dataset
          
        """

        # list of files
        fileList = []

        # get list of files       
        blockList = self.getDatasetContents(datasetId)

        # check for empty datasets
        if blockList == []:
            return fileList

        # check DBS API
        if self.args['DBSType'] == 'CGI':

            # use CGI API
            for fileBlock in blockList:
            
                # append (file name,size) to the list of files
                for aFile in fileBlock.getFileList():

                    name = aFile.getLogicalFileName()
                    size = aFile.getFileSize()
                    fileList.append((name, size))
        else:

            # use Web Services API
            for block in blockList:
        
                # get list of event collections
                eventCollectionList = block._eventCollectionList

                # get FileList
                for event in eventCollectionList:
            
                    filesInDS = event._fileList

                    # append (file name,size) to the list of files
                    for aFile in filesInDS:
                    
                        name = aFile._logicalFileName
                        size = aFile._fileSize
                        fileList.append((name, size))
   
        # return list
        return fileList
                
    def connectDBS(self):
        """
        open a connection with the DBS server
        """

        # check DBS API
        if self.args['DBSType'] == 'CGI':

            # use CGI API
            import dbsApi
            from dbsCgiApi import DbsCgiApi
            from dbsException import DbsException

            # parameters
            url = "http://cmsdoc.cern.ch/cms/aprom/DBS/CGIServer/prodquery"
            args = {}
            args['instance'] = self.args['DBSAddress']

            # create API
            try:
                dbs = DbsCgiApi(url, args)
            except DbsException, ex:
                logging.error("Fatal error: cannot contact DBS: %s" % ex)
                sys.exit(1)

        else:

            # use Web Service API
            import dbsWsApi
            import dbsApi
            import dbsException

            # create DBS Api with new API
            try:
                dbs = dbsWsApi.DbsWsApi()
            except dbsException.DbsException, ex:
                logging.error("Fatal error: cannot contact DBS: %s" % ex)
                sys.exit(1)

        # quiet DBS interaction
        dbs.setLogLevel(dbsApi.DBS_LOG_LEVEL_QUIET_)

        # return DBS API instance
        return dbs

    def getDatasetContents(self, path):
        """
        get contents of a dataset
        """

        blockList = []

        # check DBS API
        if self.args['DBSType'] == 'CGI':

            # use CGI API
            try:
                from dbsProcessedDataset import DbsProcessedDataset
                processed = DbsProcessedDataset(datasetPath = path)
                blockList = self.dbsApi.getDatasetFileBlocks(processed)

            except Exception, ex:
                # dbs is not answering properly, just wait
                logging.error("DBS error (exception: %s)" % \
                                ex.getErrorMessage())
                return blockList

        else:

            # use Web Service API
            import dbsApi
            import dbsWsClient

            try:
                blockList = self.dbsApi.getDatasetContents(path, True)
            except dbsWsClient.InvalidDatasetPathName:
                # not yet inserted in DBS? Wait next poll cycle
                logging.info("Dataset %s not yet inserted in DBS?" % \
                                path)
                return blockList

            except dbsApi.DbsApiException:
                # no datablocks yet?. Wait next poll cycle
                logging.info("No file blocks in dataset %s?" % \
                                path)
                return blockList

            except Exception, ex:
                # dbs is not answering properly, just wait
                logging.error("DBS error (exception: %s)" % \
                                ex.getErrorMessage())
                return blockList

        # check for empty block
        if blockList is None:
            logging.info("Empty file block for dataset %s" % path)
            return []

        # return blockList
        return blockList

    def startComponent(self):
        """
        _startComponent_

        Fire up a MergeSensor and start polling the DBS

        """
       
        # create control directories if necessary
        if not os.path.exists(self.args['WatchedDatasets']):
            os.makedirs(self.args['WatchedDatasets'])
        if not os.path.exists(self.args['MergeJobSpecs']):
            os.makedirs(self.args['MergeJobSpecs'])
            
        # initialize dataset structure
        try:
            self.datasets = WatchedDatasets(self.args['WatchedDatasets'],
                                            self.args['StartMode'])
        except MergeSensorError, message:
            logging.error(message);
            return
        
        # set policy parameters (100 MB)
        self.datasets.setMergeFileSize(100000000)

        # create message server
        self.ms = MessageService()
        
        # register
        self.ms.registerAs("MergeSensor")
        
        # subscribe to messages
        self.ms.subscribeTo("NewDataset")
        self.ms.subscribeTo("MergeSensor:StartDebug")
        self.ms.subscribeTo("MergeSensor:EndDebug")
        
        # start polling thread
        pollingThread = PollDBS(self.poll)
        pollingThread.start()
        
        # wait for messages
        while True:
            messageType, payload = self.ms.get()
            self.__call__(messageType, payload)
            self.ms.commit()
            
##############################################################################
# PollDBS class
##############################################################################
                                                                                
class PollDBS(Thread):
    """
    Thread that performs DBS polling 
    """

    def __init__(self, poll):
        """
        __init__

        Initialize thread and set polling callback
        """
        Thread.__init__(self)
        self.poll = poll;

    def run(self):
        """
        __run__

        Performs polling on DBS
        """

        while True:
            self.poll()

