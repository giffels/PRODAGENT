#!/usr/bin/env python
"""
_WatchedDatasets_

Define the class WatchedDatasests, used to keep information on the set of
currently watched datasets.
 
"""
 
__revision__ = "$Id: WatchedDatasets.py,v 1.15 2008/08/11 19:11:19 swakef Exp $"
__version__ = "$Revision: 1.15 $"
__author__ = "Carlos.Kavka@ts.infn.it"
 
# MergeSensor
from MergeSensor.Dataset import Dataset
from MergeSensor.MergeSensorError import MergeSensorError, \
                                         InvalidDataset, \
                                         NonMergeableDataset, \
                                         DatasetNotInDatabase

# workflow specifications
from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec

##############################################################################
# WatchedDatasets class
##############################################################################

class WatchedDatasets:
    """
    WatchedDatasets
    
    An instance of this class represents the set of datasets currently
    watched by the Merge Sensor component of the Production Agent.
            
    """ 
    
    # logging instance
    logging = None
    
    # database instance
    database = None
    
    def __init__(self, start = "warm"):
        """

        Initialize a WatchedDataset object. 
        
        In a "warm" start, the list of watched datasets is initialized from
        the persistent information stored in database.
        In a "cold" start, all datasets are closed. However, new dataset
        events open them in their original state.
        In a "scratch" start, the list of watched datasets is initialized
        as an empty list and the database is wiped out.

        Arguments:
            
          start -- used to select "warm", "cold" or "scratch" start mode

        """

        # dictionary used to store info on watched datasets
        self.datasets = {}
        
        # database instance
        self.database = self.__class__.database
              
        # logging instance
        self.logging = self.__class__.logging
        
        # initialize dataset structure
        if start not in ['warm', 'cold', 'scratch']:
            self.logging.error("Start mode %s not valid, assuming warm mode" \
                          % start)
            start = "warm"
            
        # warm mode: keep all datasets as specified in the database
        if start == "warm":
            
            # get information for database
            oldDatasets = self.database.getDatasetList()

            for datasetId in oldDatasets:

                # get database information
                try:
                    dataset = Dataset(datasetId)
                
                except DatasetNotInDatabase, msg:

                    # does not exist, ignore it!
                    continue;

                self.datasets[datasetId] = dataset

        # cold mode: close all open datasets
        elif start == "cold":
            
            # start transaction
            self.database.startTransaction()
            
            # get dataset information
            oldDatasets = self.database.getDatasetList()
            for datasetId in oldDatasets:

                # close dataset
                self.database.closeDataset(datasetId)

            # commit changes
            self.database.commit()
            
        # scratch mode: wipe out all information
        else:
            
            # drop all contents of the database
            self.database.eraseDB()

            # commit changes 
            self.database.commit()
            
    ##########################################################################
    # add a dataset
    ##########################################################################

    def add(self, workflowFile):
        """
        _add_
        
        Add a dataset to the list of watched datasets.
        
        Arguments:
            
          workflowFile -- the workflow specification file
        
        Return:
            
          the datasetId

        """

        # read the WorkflowSpecFile
        try:
            wfile = WorkflowSpec()
            wfile.load(workflowFile)

        # wrong dataset file
        except Exception, msg:
            raise InvalidDataset, \
                  "Error loading workflow specifications from %s" % workflowFile

        # get output modules
        try:
            outputDatasetsList = wfile.outputDatasets()
            
            outputModules = [outDS['OutputModuleName'] \
                             for outDS in outputDatasetsList]
            
            # count outputs per module
            outputModulesList = {}
            for module in outputModules:
                outputModulesList.setdefault(module, 0)
                outputModulesList[module] += 1
            
        except (IndexError, KeyError):
            raise MergeSensorError( \
                    "MergeSensor exception: wrong output dataset specification")

        # list of watched datasets
        datasetIdList = []
        
        # create a dataset instances for each output module
        for outputModule, numDS in outputModulesList.items():
            
            for counter in range(numDS):
            
                try:
                    dataset = Dataset(wfile, outputModule=outputModule, \
                                      dsCounter = counter, \
                                      fromFile = True)
                except InvalidDataset, message:
                    self.logging.error(message)
                    continue
                except NonMergeableDataset, message:
                    self.logging.info(message)
                    continue
                
                # get dataset name
                datasetId = dataset.getName()
            
                # check: dataset should not exist, ignore if it is registered
                if datasetId in self.datasets.keys():
                    self.logging.info( \
                           "Ignoring workflow %s, is currently watched" % \
                           workflowFile)
                    continue
    
                # add it
                self.datasets[datasetId] = dataset
                datasetIdList.append(datasetId)
            
        # return list of added datasets
        return datasetIdList
        
    ##########################################################################
    # remove a dataset
    ##########################################################################

    def remove(self, datasetId):
        """
        _remove_
        
        Remove a dataset from the list of watched datasets.
        
        Arguments:
            
          datasetId -- name of dataset
        
        Return:
            
          none
        """
    
        # check: it should exist in the list of datasets
        if datasetId not in self.datasets.keys():
            raise MergeSensorError, \
                  'cannot remove dataset %s, it does not exist' % datasetId   
        
        # remove its information from the database
        self.datasets[datasetId].remove()
         
        # remove it from dataset structure
        del self.datasets[datasetId]
        
    ##########################################################################
    # get the list of watched datasets
    ##########################################################################

    def list(self):
        """
        _list_
        
        Return a list of the currently watched datasets.
        
        Arguments:
            
          none
          
        Return:
            
          list of Dataset instances
          
        """
            
        return [value.getName() for value in self.datasets.values()]
        
    ##########################################################################
    # update the files in a dataset
    ##########################################################################

    def updateFiles(self, datasetId, fileList):
        """
        _updateFiles_
        
        Used to update the list of files in the dataset. WatchedDatasets
        expects to receive by this message the complete list of files in
        the dataset. Consistency checkings are performed between calls
        applied to the same dataset
        
        Arguments:
            
          datasetId -- the name of the dataset
          fileList -- a list of tuples (file,size) that specifies the list
          of all files in the named dataset together with their size, as
          returned by the DBS component.
          
        Return:
            
          none
          
        """
        self.datasets[datasetId].setFiles(fileList)
        
    ##########################################################################
    # add a merge job
    ##########################################################################

    def addMergeJob(self, datasetId, jobId, selectedSet, oldJobId):
        """
        _addMergeJobs_
        
        Add a new merge job to the dataset.
        
        Arguments:
            
          datasetId -- the name of the dataset
          jobId -- the job name
          selectedSet -- this list of input files to merge
          oldJobId -- the old job id in case of resubmission 
          
        Return:
            
          the name of the output file
          
        """
        return self.datasets[datasetId].addMergeJob(jobId, selectedSet, \
                                                    oldJobId)

    ##########################################################################
    # determine mergeable status of a dataset
    ##########################################################################

    def mergeable(self, datasetId, forceMerge):
        """
        _mergeable_
        
        Test if dataset can be merged.
        
        Arguments:
            
          datasetId -- the name of the dataset
          forceMerge -- True indicates merge should be performed
                        independently of file sizes
        Return:
            
          tuple (condition, fileBlockId, fileList, oldJobId)          
          where condition is True if there is a subset of files eligible for
          merging and False if not. fileList contains the selected list of
          files, which can be directly used as an argument to addMergeJob.
          If there is no enough files to be merged, check for requirements
          of merge jobs resubmissions. oldJobId is None if it is a new
          merge, or the id of the old job in case of resubmission. 
          
        """
        
        # select file set
        (fileList, fileBlockId) = \
               self.datasets[datasetId].selectFiles(forceMerge)
               
        # return merge condition, file list and file block 
        if fileList != []:
            return (True, fileBlockId, fileList, None)
    
        # check for merge jobs to be resubmitted (as new jobs)
        (jobName, fileBlockId, fileList) = self.datasets[datasetId].getNewJob()
    
        # return merge condition, file list and file block
        return (jobName != None, fileBlockId, fileList, jobName)
    
    ##########################################################################
    ##########################################################################

    def close(self, datasetId):
        """
        _close_
        
        Mark the dataset as closed, meaning that no more files are expected
        to be included.
        
        Arguments:
            
          datasetId -- the name of the dataset
          
        Return:
            
          none
                    
        """
        self.datasets[datasetId].setStatus("closed")

    ##########################################################################
    # get properties of a dataset
    ##########################################################################

    def getProperties(self, datasetId):
        """
        _getProperties_

        Return the properties of the dataset datasetId

        Arguments:

          datasetId -- the name of the dataset

        Return:

          a dictionary with all exposed properties

        """            

        # check dataset existence
        if datasetId in self.datasets.keys():

            # yes, return a copy of it
            return self.datasets[datasetId].getData()
        else:

            # no, return an empty dictionary
            return {}

    ##########################################################################
    # set database instance
    ##########################################################################

    @classmethod
    def setDatabase(cls, dbInstance):
        """
        _setDatabase_
        
        Set the database access instance.
        
        Arguments:
            
          database -- the database access object
        
        Return:
            
          none

        """
        cls.database = dbInstance
    
    ##########################################################################
    # set logging instance
    ##########################################################################

    @classmethod
    def setLogging(cls, loggingInstance):
        """
        _setLogging_
        
        Set logging facilities.
        
        Arguments:
            
          logging -- the initialized logging object
        
        Return:
            
          none

        """
        cls.logging = loggingInstance
        
    ##########################################################################
    # convert dataset list to a string
    ##########################################################################

    def __str__(self):
        """
        __str__
        
        Build a printable representation of the WatchedDataset object.
        
        Arguments:
            
          none
          
        Return:
            
          the WatchedDataset as a string
          
        """
        return "\n\n".join([str(dataset) for dataset in self.datasets.values()])
        
    ##########################################################################
    # get version information
    ##########################################################################

    @classmethod
    def getVersionInfo(cls):
        """
        _getVersionInfo_
        
        return version information
        """
        
        return __version__

