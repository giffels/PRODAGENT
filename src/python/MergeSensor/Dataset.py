#!/usr/bin/env python
"""
_Dataset_

Define the class Dataset, used to store information on a single dataset.
 
"""

import os
import pickle
import time
import re

__revision__ = "$Id: Dataset.py,v 1.12 2006/08/07 22:32:36 hufnagel Exp $"
__version__ = "$Revision: 1.12 $"
__author__ = "Carlos.Kavka@ts.infn.it"

from MergeSensor.MergeSensorError import MergeSensorError, InvalidDataTier
from MCPayloads.WorkflowSpec import WorkflowSpec

# logging
import logging
from logging.handlers import RotatingFileHandler

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
     
       select a set of files to be merged.
       
     * addMergeJob(self, fileList, fileBlockId)
     
       add a new merge job to the dataset.

     * setStatus(self, status)
     
       set status of the dataset

     * getStatus(self)
     
       get status of the dataset
       
     * getName(self):
        
       return the name of the dataset.
            
    All methods can generate the exception MergeSensorError    

    Example of the representation of a dataset object:
   
      {'id' : '[Primary][PollTier][Processed]',
       'name' : '/Primary/Tier/Processed',
       'primaryDataset' : 'Primary',
       'dataTier' : 'SIM-GEN-DIGI',
       'pollTier' : 'SIM',
       'secondaryOutputTiers' : 'RECO',
       'processedDataset' : 'Processed',
       'PSetHash' : '123456789012345678190',
       'status' : 'open',
       'started' : 'Thu Jan 12 10:41:09 2006',
       'last_updated' : 'Thu Jan 12 10:44:28 2006'
       'files' : [{'file1.root','file2.root','file3.root','file4.root',
                  'file5.root','file6.root','file7.root']
       'remaining_files'[98] : [('file5.root',1024)},
                                ('file6.root',5607),
                                ('file7.root',75349)],
       'remaining_files'[44] : [('file4.root',1024)},
                                ('file8.root',506)],
       'version' : 'CMSSW_0_6_1',
       'workflowName' : 'Test060pre5Mu10GeV',
       'mergedLFNBase' : '/store/PreProd/2006/6/6/Test060pre5Mu10GeV',
       'category' : 'PreProd',
       'timeStamp' : 1149604662,
       'outSeqNumber' : 6
      }
      
    where the fields have the following meaning:
            
      'id' -- the id of the dataset (cannot use / inside :-(
      'name' -- the real name
      'status' -- can be 'open' or 'closed'
      'started' -- time when watching started
      'last_updated' -- time of last update
      'files' -- all files in dataset
      'remaining_files' -- list of tuples (file,size) ready to be merged
      indexed by file blocks.
      'version' : CMSSW version
      'workflowName' : name of original workflow
      'mergedLFNBase' : LFN for merged files
      'category' : the category (pre production, etc.)
      'timeStamp' : time stamp of last modification
      'outSeqNumber' : index of output files
             
    """

    # base path where the persistence file is stored
    basePath = "" 
    
    # default size for merged files (1MB)
    mergeFileSize = 1000000
    
    # list of data tiers
    dataTierList = []
    
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

        pattern ="^\[([\w\-]+)\]\[([\w\-]+)\]\[([\w\-]+)]"
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

        # get output datasets
        try:
            outputDatasetsList = wfile.outputDatasets()
            
            # the first one
            outputDataset = outputDatasetsList[0]
            
            # the others
            others = outputDatasetsList[1:]
            secondaryOutputTiers = [outDS['DataTier'] for outDS in others]
            
        except (IndexError, KeyError):
            raise MergeSensorError("MergeSensor exception: wrong output dataset specification")
        
        # get primary Dataset
        try:
            primaryDataset = outputDataset['PrimaryDataset']
        except KeyError:
            raise MergeSensorError("MergeSensor exception: invalid primary dataset specification")

        # get datatier
        try:
            dataTier =  outputDataset['DataTier']
        except KeyError:
            raise InvalidDataTier("MergeSensor exception: DataTier not specified")

        # verify if valid
        if not self.validDataTier(dataTier):
            raise InvalidDataTier("MergeSensor exception: invalid DataTier %s" % dataTier)
         
        # get poll datatier
        pollTier = dataTier.split("-")[0]
        
        # get processed
        try:
            processedDataset = outputDataset['ProcessedDataset']
        except KeyError:
            raise MergeSensorError("MergeSensor exception: invalid processed dataset specification")

        # do not merge merged datasets
        if processedDataset.endswith("-merged"):
            self.data = None
            return
        
        # build dataset id and name
        datasetId = "[%s][%s][%s]" % \
                    (primaryDataset, pollTier, processedDataset)
        name = "/%s/%s/%s" % (primaryDataset, dataTier, \
                              processedDataset)
        
        # get Merged LFN base
        try:
            mergedLFNBase = wfile.parameters['MergedLFNBase']
        except KeyError:
            mergedLFNBase = ''
            
        # get workflow name, category and time stamp
        workflowName = wfile.workflowName()
        category = wfile.requestCategory()
        timeStamp = wfile.requestTimestamp()
        
        # get application version
        try:
            version = outputDataset['ApplicationVersion']
        except:
            version = 'CMSSW_0_7_0'
        
        # get PSetHash
        try:
            psethash = outputDataset["PSetHash"]
        except:
            psethash = "12345678901234567890"

        # initialize it
        date = time.asctime(time.localtime(time.time()))
        self.data = {'id' : datasetId,
                     'name' : name,
                     'primaryDataset' : primaryDataset,
                     'dataTier' : dataTier,
                     'pollTier' : pollTier,
                     'secondaryOutputTiers' : secondaryOutputTiers,
                     'processedDataset' : processedDataset,
                     'PSetHash' : psethash,
                     'version' : version,
                     'workflowName' : workflowName,
                     'mergedLFNBase' : mergedLFNBase,
                     'category' : category,
                     'timeStamp' : timeStamp,
                     'status' : 'open',
                     'started' : date,
                     'last_updated' : date,
                     'files' : [],
                     'remaining_files' : {},
                     'outSeqNumber' : 1
                    }

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
        
    @classmethod
    def setDataTierList(cls, tierList):
        """
        _setDataTiers_

        Set the list of possible datatiers.

        Arguments:

          list -- list of possible datatiers

        Return:

          none

        """
        cls.dataTierList = tierList.split(',')

    @classmethod
    def validDataTier(cls, dataTierName):
        """
        _validDataTiers_

        check dataTier validity

        Arguments:

          true if fine, false if wrong

        Return:

          none

        """
        dataTiers = dataTierName.split("-")

        if dataTiers == []:
            return False
        
        for elem in dataTiers:
            if not elem in cls.dataTierList:
                return False
      
        return True
    
    @classmethod
    def setMergeFileSize(cls, size):
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
        cls.mergeFileSize = size
        
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
        for fileName, size, fileBlockId in fileList:
            if fileName not in self.data['files']:
                self.data['files'].append(fileName)
               
                # insert in current file block or create a new one
                try:
                    self.data['remaining_files'][fileBlockId].append( \
                                                        (fileName, size))
                except KeyError:
                    self.data['remaining_files'][fileBlockId] = [(fileName, \
                                                                size)]

        # paranoid consistency check
        # possible error: files removed in dataset
        if len(self.data['files']) != len(fileList):
            raise MergeSensorError, \
                  'inconsistency on dataset %s, a file was removed' % \
                  self.data['name']
        
        # update time
        date = time.asctime(time.localtime(time.time()))
        self.data['last_update'] = date
        
        # write dataset
        self.__write()
        
    def addMergeJob(self, fileList, fileBlockId):
        """
        _addMergeJobs_
        
        Add a new merge job to the dataset.
        
        Arguments:
            
          fileList -- the list of files that the job will start to merge
          fileBlockId -- the file block id as returned by DBS

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
                                     self.data['remaining_files'][fileBlockId]]
        wrongFiles = [fileName for fileName in fileList
                           if fileName not in remainingFiles]
        if wrongFiles:
            raise MergeSensorError, \
                  'cannot merge files %s, already under merging' % \
                  str(wrongFiles)
                
        # remove selected files from set of files
        self.data['remaining_files'][fileBlockId] = [(fileName, size)
            for fileName, size in self.data['remaining_files'][fileBlockId]
                if fileName not in fileList]

        # determine output file name
        outputFile = "set" + str(self.data['outSeqNumber'])
        self.data['outSeqNumber'] = self.data['outSeqNumber'] + 1
                
        # update time
        date = time.asctime(time.localtime(time.time()))
        self.data['last_update'] = date
        
        # write dataset
        self.__write()
        
        return outputFile

    def selectFiles(self, forceMerge):
        """
        _selectFiles_
        
        Select a set of files to be merged based on a selection policy.
        Currently, it is based just on the size of the files and the
        expected merge size. When forceMerge is True, a set of files
        is returned even if the size requirement is not fullfilled.
        
        Arguments:
            
          forceMerge -- True indicates size is not a requirement.
                    
        Return:
            
          the list of files to be merged and their fileBlockId
          
        """

        # get file size
        mergeFileSize = self.__class__.mergeFileSize

        # check all file blocks in dataset
        for fileBlockId in self.data['remaining_files'].keys():
       
            # get set of remaining files
            files = self.data['remaining_files'][fileBlockId]

            # select set of files with at least mergeFileSize size
            totalSize = 0
            selectedSet = []
            for fileName, size in files:
                selectedSet.append(fileName)
                totalSize = totalSize + size
                if totalSize > mergeFileSize:
                    return (selectedSet, fileBlockId)

            # not enough files, continue to next fileBlock
            # if forceMerge and list non-empty, return what we have
            # if forceMerge and list empty, make log entry and continue to next fileBlock
            if forceMerge:
                if selectedSet == []:
                    logging.info("Forced merge does not apply to FileBlock %s due to non mergeable condition"
                             % fileBlockId)
                    continue
                else:
                    return(selectedSet, fileBlockId)
            else:
                continue

        return ([], 0)

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
        
        # write information into temporary file
        tmpFile = "data.tmp"
        try:       
            aFile = file(tmpFile, "w")
            pickle.dump(self.data, aFile)
            aFile.close()
        except IOError:
            raise MergeSensorError, \
                  'cannot write to temporary file for dataset %s' % self.data['id']           

        # rename with the real file name
        os.rename(tmpFile, pathname)
        
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
