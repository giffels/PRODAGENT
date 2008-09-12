#!/usr/bin/env python
"""
_WatchedDatasets_

Define the class WatchedDatasests, used to keep information on the set of
currently watched datasets.
 
"""
 
__revision__ = "$Id: WatchedDatasets.py,v 1.5 2006/04/12 09:16:12 ckavka Exp $"
__version__ = "$Revision: 1.5 $"
__author__ = "Carlos.Kavka@ts.infn.it"
 
import os

from MergeSensor.MergeSensorError import MergeSensorError
from MergeSensor.Dataset import Dataset

class WatchedDatasets:
    """
    WatchedDatasets
    
    An instance of this class represents the set of datasets currently
    watched by the Merge Sensor component of the Production Agent.
    
    Public methods:
        
     * add(workflowFile)

       add a dataset to the list of watched datasets.

     * remove(datasetId)

       remove the dataset from the list of watched datasets.

     * list()

       return a list of the currently watched datasets.

     * setMergeFileSize(self, mergeFileSize)
     
       set the expected merge file size (applicable to all datasets).
          
     * updateFiles(self, datasetId, fileList)

       used to update the list of files in the dataset.
        
     * addMergeJob(self, datasetId, fileList)

       add a new merge job to the dataset.
       
     * mergeable(self, datasetId)

       test if dataset can be merged.
       
     * closeDataset(datasetId)
     
       used to signal that a dataset is closed.
               
    All methods can generate the exception MergeSensorError
        
    """ 
    
    def __init__(self, basePath, start = "cold"):
        """

        Initialize a WatchedDataset object. 
        
        In a "warm" start, the list of watched datasets is initialized from
        the persistent information stored in the files in control directory.
        Usually it is expected to be used after a reinicialization of the
        Production Agent (or Merge Sensor component). In a "cold" start,
        the list of watched datasets is initialized as an empty list, and
        all files in control directory are removed.

        Arguments:
            
          basePath -- path of the directory where persistent information
                        is stored
          start -- used to select "warm" or "cold" start

        """

        # set base path       
        self.basePath = basePath
 
        # set path for all datasets
        Dataset.basePath = basePath

        # dictionary used to store info on watched datasets
        self.datasets = {}
                
        # get old dataset information (if any)
        oldDatasets = os.listdir(self.basePath)

        if start == "warm":
            # rebuild dataset information from persistence directory
            # it should only happen after a restart of Merge Sensor
            for datasetId in oldDatasets:
                dataset = Dataset(datasetId)
                self.datasets[datasetId] = dataset
        else:
            # cold starts: remove everything
            for dataset in oldDatasets:
                path = os.path.join(basePath, dataset)
                os.remove(path)

    def add(self, workflowFile):
        """
        _add_
        
        Add a dataset to the list of watched datasets.
        
        Arguments:
            
          workflowFile -- the workflow specification file
        
        Return:
            
          the datasetId

        """

        # create a new dataset
        dataset = Dataset(workflowFile)
        datasetId = dataset.getId()
        
        # check for merged datasets, which should not be merged again
        if datasetId is None:
            return None
        
        # check: dataset should not exist, ignore if it is registered
        if datasetId in self.datasets.keys():
            return None

        # add it
        self.datasets[datasetId] = dataset
        
        # return its name
        return datasetId
        
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
            
        # check: dataset pathname should exist
        pathname = os.path.join(self.basePath, datasetId)
        if not os.path.exists(pathname):
            raise MergeSensorError, \
                  'cannot remove dataset %s, its file does not exists' % \
                  datasetId

        # remove it and its associated file
        del self.datasets[datasetId]
        os.remove(pathname)

    def list(self):
        """
        _list_
        
        Return a list of the currently watched datasets.
        
        Arguments:
            
          none
          
        Return:
            
          list of Dataset instances
          
        """
            
        return [value.getId() for value in self.datasets.values()]
    
    def setMergeFileSize(self, mergeFileSize):
        """
        _setMergeFileSize_
        
        Set the expected merge file size applicable to all datasets in the
        list of currently watched datasets.
        
        Arguments:
            
          mergeFileSize -- expected size of the resulting merged files.
          
        Return:
            
          none
          
        """
        Dataset.mergeFileSize = mergeFileSize
        
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
        
    def addMergeJob(self, datasetId, fileList):
        """
        _addMergeJobs_
        
        Add a new merge job to the dataset.
        
        Arguments:
            
          datasetId -- the name of the dataset
          fileList -- the list of files that the job will start to merge
          
        Return:
            
          the name of the output file
          
        """
        return self.datasets[datasetId].addMergeJob(fileList)

    def mergeable(self, datasetId):
        """
        _mergeable_
        
        Test if dataset can be merged.
        
        Arguments:
            
          datasetId -- the name of the dataset
          
        Return:
            
          tuple (condition, listFiles)
          
          where condition is True if there is a subset of files eligible for
          merging and False if not. listFiles contains the selected list of
          files, which can be directly used as an argument to addMergeJob.
          
        """
        fileList = self.datasets[datasetId].selectFiles()
        return (fileList != [], fileList)
    
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
        
