#!/usr/bin/env python
"""
_Dataset_

Define the class Dataset, used to store information on a single dataset.
 
"""

import os
import pickle
import time
import re

__revision__ = "$Id: Dataset.py,v 1.1 2006/04/12 15:12:11 evansde Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "Carlos.Kavka@ts.infn.it"

from MergeSensor.MergeSensorError import MergeSensorError
from MCPayloads.WorkflowSpec import WorkflowSpec

class Dataset:
    """
    Dataset

    Instances of this class represent a single dataset watched by the
    MergeSensor component of the Production Agent.

    Public methods:

     * setBasePath(self, basePath)
     
       set the path where persistence files are stored
        
     * setMergeFileSize(self, size)
     
       set the expected merge file size (applicable to all datasets).
        
     * setFiles(self, fileList)
     
       used to update the list of files in the dataset.

     * selectFiles(self)
     
       select a set of files to be merged based.
       
     * addMergeJob(self, fileList)
     
       add a new merge job to the dataset.

     * setStatus(self, status)
     
       set status of the dataset

     * getStatus(self)
     
       get status of the dataset
       
     * getName(self):
        
       return the name of the dataset.
     
    All methods can generate the exception MergeSensorError    

    Example of the representation of a dataset object:
   
      {'id' : '[Primary][Tier][Processed]',
       'name' : '/Primary/Tier/Processed',
       'primaryDataset' : 'Primary',
       'dataTier' : 'Tier',
       'realDataTier' : 'realTier',
       'processedDataset' : 'Processed',
       'status' : 'open',
       'started' : 'Thu Jan 12 10:41:09 2006',
       'last_updated' : 'Thu Jan 12 10:44:28 2006'
       'files' : [{'file1.root','file2.root','file3.root','file4.root',
                  'file5.root','file6.root','file7.root']
       'remaining_files' : [('file5.root',1024)},
                            ('file6.root',5607),
                            ('file7.root',75349)],
      }
      
    where the fields have the following meaning:
            
      'id' -- the id of the dataset (cannot use / inside :-(
      'name' -- the real name
      'status' -- can be 'open' or 'closed'
      'started' -- time when watching started
      'last_updated' -- time of last update
      'files' -- all files in dataset
      'remaining_files' -- list of tuples (file,size) ready to be merged
             
    """

    # base path where the persistence file is stored
    basePath = "" 
    
    # default size for merged files (1MB)
    mergeFileSize = 1000000
    
    def __init__(self, fileName):
        """

        Initialize a Dataset. 
        
        The dataset is loaded from the persistence file if it exists. If
        not, a new dataset instance is created from the Workflow
        specification file
         
        Arguments:
            
          file -- the workflow specification file or persistence file
                  (warm restart)

        """

        # check if it is a persistence file
        pattern ="^\[(\w+)\]\[(\w+)\]\[(\w+)]"
        match = re.search(pattern, fileName)

        # yes, read the persistence file
        if match is not None:
            self.__read(fileName)
            return
        

        # no, it is a new dataset, read the WorkflowSpecFile
        try:
            wfile = WorkflowSpec()
            wfile.load(fileName)

        # wrong dataset file, ignore it
        except:
            self.data = None
            return

        # get primary dataset name
        try:
            outputDataset = (wfile.outputDatasets())[0]
            primaryDataset = outputDataset['PrimaryDataset']
        except KeyError:
            primaryDataset = "dummyDataset"

        # get datatier
        try:
            dataTierName =  outputDataset['DataTier']
        except KeyError:
            dataTierName = "dummyDataTier"

        # process it
        dataTier = self.encodeTier(dataTierName)

        # get processed
        try:
            processedDataset = outputDataset['ProcessedDataset']
        except KeyError:
            processedDataset = "dummyProcessedDataset"

        # do not merge merged datasets
        if processedDataset.endswith("-merged"):
            self.data = None
            return
        
        # build dataset id and name
        datasetId = "[%s][%s][%s]" % \
                    (primaryDataset, dataTier, processedDataset)
        name = "/%s/%s/%s" % (primaryDataset, dataTier, \
                              processedDataset)
        
        # initialize it
        date = time.asctime(time.localtime(time.time()))
        self.data = {'id' : datasetId,
                     'name' : name,
                     'primaryDataset' : primaryDataset,
                     'dataTier' : dataTier,
                     'realDataTier' : dataTierName,
                     'processedDataset' : processedDataset,
                     'status' : 'open',
                     'started' : date,
                     'last_updated' : date,
                     'files' : [],
                     'remaining_files' : [],
                    }

        # initialize sequence number for output files
        self.outSeqNumber = 1
        
        # write to the persistence file    
        self.__write()
            
    def setBasePath(self, basePath):
        """
        _setBasePath_
        
        Set the path to store dataset information files. Note that the path
        is stored in a class variable, meaning that all datasets are stored
        in the same directory.
        
        Arguments:
            
          basePath -- path of the directory where persistent information
                      is stored
        
        Return:
            
          none

        """
        self.__class__.basePath = basePath
        
    def setMergeFileSize(self, size):
        """
        _setMergeFileSize_
        
        Set the target size of the merged files. Note that the size is stored
        in a class variable, meaning that its value is applicable to all
        datasets.
        
        Arguments:
            
          size -- expected size of the resulting merged files.
        
        Return:
            
          none

        """
        self.__class__.mergeFileSize = size
        
    def setFiles(self, fileList):
        """
        _setFiles_
        
        Used to update the list of files in the dataset. The argument must
        provide the complete list of files in the dataset as returned
        currently by DBS. Consistency checkings are performed between calls
        applied to the same dataset.
        
        Arguments:
            
          fileList -- a list of tuples (file,size) that specifies the list
          of all files in the named dataset together with their size.
          
        Return:
            
          none
          
        """
        
        # add files not present in original structure
        for fileName, size in fileList:
            if fileName not in self.data['files']:
                self.data['files'].append(fileName)
                self.data['remaining_files'].append((fileName, size))

        # paranoid consistency check
        # possible error: files removed in dataset
        if len(self.data['files']) != len(fileList):
            raise MergeSensorError, \
                  'inconsistency on dataset %s, a file was removed' % \
                  self.data['name']
        
        # paranoid consistency check
        # possible error: file change size
        for fileName in self.data['remaining_files']:
            if fileName not in fileList:
                raise MergeSensorError, \
                      'inconsistency on dataset %s, a file changed size' % \
                      self.data['name']

        # update time
        date = time.asctime(time.localtime(time.time()))
        self.data['last_update'] = date
        
        # write dataset
        self.__write()
        
    def addMergeJob(self, fileList):
        """
        _addMergeJobs_
        
        Add a new merge job to the dataset.
        
        Arguments:
            
          fileList -- the list of files that the job will start to merge
          
        Return:
            
          the name of the output file
          
        """
        
        # check for files not in dataset 
        wrongFiles = [fileName for fileName in fileList
                           if fileName not in self.data['files']]
        if wrongFiles:
            raise MergeSensorError, \
                  'cannot merge files %s, not in dataset' % str(wrongFiles)
        
        # check for files already under merging
        remainingFiles = [fileName for fileName, size in
                                       self.data['remaining_files']]
        wrongFiles = [fileName for fileName in fileList
                           if fileName not in remainingFiles]
        if wrongFiles:
            raise MergeSensorError, \
                  'cannot merge files %s, already under merging' % \
                  str(wrongFiles)
                
        # remove selected files from set of files
        self.data['remaining_files'] = [(fileName, size) for fileName, size
                                             in self.data['remaining_files']
                                             if fileName not in fileList]

        # determine output file name
        outputFile = "set" + str(self.outSeqNumber)
        self.outSeqNumber = self.outSeqNumber + 1
                
        # update time
        date = time.asctime(time.localtime(time.time()))
        self.data['last_update'] = date
        
        # write dataset
        self.__write()
        
        return outputFile

    def selectFiles(self):
        """
        _selectFiles_
        
        Select a set of files to be merged based on a selection policy.
        Currently, it is based just on the size of the files and the
        expected merge size.
        
        Arguments:
            
          none
                    
        Return:
            
          the list of files to be merged
          
        """

        # get file size
        mergeFileSize = self.__class__.mergeFileSize

        # get set of remaining files
        files = self.data['remaining_files']
        
        # select set of files with at least mergeFileSize size
        totalSize = 0
        selectedSet = []
        for fileName, size in files:
            selectedSet.append(fileName)
            totalSize = totalSize + size
            if totalSize > mergeFileSize:
                return selectedSet
               
        return []

    def getStatus(self):
        """
        _getStatus_
        
        Used to get the status of the dataset.
        
        Arguments:
            
          none
                    
        Return:
            
          the status of the dataset. It can be "open" or "closed"
          
        """
        return self.data['status']
    

    def setStatus(self, status):
        """
        _setStatus_
        
        Used to set the status of the dataset.
        
        Arguments:
            
          status -- the status of the dataset
                    
        Return:
            
          none
          
        """
        self.data['status'] = status

        # update time
        date = time.asctime(time.localtime(time.time()))
        self.data['last_update'] = date
        
        # write dataset
        self.__write()
        
    def __read(self, datasetId):
        """
        __read_
        
        Private method used to restore dataset from persistence file.
        
        Arguments:
            
          datasetId -- the file name where dataset is stored
                    
        Return:
            
          reference to dataset
          
        """
        # build file name
        pathname = os.path.join(self.__class__.basePath, datasetId)

        # get information from dataset file
        try:
            aFile = file(pathname,"r")
            dataset = pickle.load(aFile)
            aFile.close()
        except IOError:
            raise MergeSensorError, \
                  'cannot read from dataset %s' % datasetId            
                  
        # return reference to dataset
        self.data = dataset
        
    def __write(self):
        """
        __write_
        
        Private method used to store dataset into a persistence file.
        
        Arguments:
            
          none
                    
        Return:
            
          none
          
        """
        # build file name
        pathname = os.path.join(self.__class__.basePath, self.data['id'])
        
        # write information into dataset file
        try:       
            aFile = file(pathname,"w")
            pickle.dump(self.data, aFile)
            aFile.close()
        except IOError:
            raise MergeSensorError, \
                  'cannot write to dataset %s' % self.data['id']           

    def encodeTier(self, dataTier):
        """
        _encodeTier_
                                                                                
       Encode the name of the dataTier to be used to store the
       dataset in DBS 
                                                                                
        Arguments:
                                                                                
          dataTier - the name of the tier as from workflow
                                                                                
        Return:

          the encoded tier name as a string
                                                                                
        """

        if dataTier == 'Simulated':
            dataTier = 'SIM'
        elif dataTier == 'Digitized':
            dataTier = 'DIGI'
        elif (dataTier == 'GenSimDigi') or (dataTier == 'GEN-SIM-DIGI'):
            dataTier = 'GEN-SIM-DIGI'
        elif dataTier == 'GEN-SIM':
  	    dataTier = 'GEN-SIM'
        else:
            dataTier = 'Unknown'

        return dataTier

    def getName(self):
        """
        _getName_
        
        Return the name of the dataset.
        
        Arguments:
            
          none
          
        Return:
            
          the dataset name as a string
          
        """
        return self.data['name']
    
    def getId(self):
        """
        _getId_
        
        Return the id of the dataset.
        
        Arguments:
            
          none
          
        Return:
            
          the dataset id as a string, None if it was not created
          
        """
        if self.data is None:
            return None
        else:
            return self.data['id']

    def getData(self):
        """
        _getData_

        Return information on dataset

        Arguments:

            none

        Return:

            A dictionary with all information on dataset

        """

        import copy
        return copy.deepcopy(self.data)

    def __str__(self):
        """
        __str__
        
        Build a printable representation of the Dataset object.
        
        Arguments:
            
          none
          
        Return:
            
          the complete dataset as a string
          
        """
        string = "name: %s\n" % self.data['name']
        return string + "\n".join(["%s: %s" % (key, value) for (key, value) 
                                                           in self.data.items()
                                                           if key != "name"])
