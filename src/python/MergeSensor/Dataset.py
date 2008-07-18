#!/usr/bin/env python
"""
_Dataset_

Define the class Dataset, used to store information on a single dataset.
 
"""

import time
import re
import MySQLdb

__revision__ = "$Id: Dataset.py,v 1.31 2007/06/06 14:34:11 ckavka Exp $"
__version__ = "$Revision: 1.31 $"
__author__ = "Carlos.Kavka@ts.infn.it"

# MergeSensor errors
from MergeSensor.MergeSensorError import MergeSensorError, \
                                         InvalidDataTier, \
                                         InvalidDataset, \
                                         NonMergeableDataset, \
                                         DatasetNotInDatabase

##############################################################################
# Dataset class
##############################################################################

class Dataset:
    """
    Dataset

    Instances of this class represent a single dataset watched by the
    MergeSensor component of the Production Agent.
    
    """

    # base path where the persistence file is stored
    basePath = "" 
    
    # default maximum size for merged files (2 GB)
    maxMergeFileSize = 2000000000
    
    # default minimum size for merged files (75% of maximum size)
    minMergeFileSize = 1500000000

    # logging instance
    logging = None
    
    # database instance
    database = None
    
    # maximum allowed input file failures
    maxInputAccessFailures = None

    # merge policy
    policy = None

    ##########################################################################
    # Dataset initialization
    ##########################################################################

    def __init__(self, info, outputModule = None, dsCounter = 0, fromFile = False):
        """

        Initialize a Dataset. 
        
        The dataset information is loaded from the database. If
        not, a new dataset instance is created from the Workflow
        specification file
         
        Arguments:
            
          info -- the workflow specification or the dataset name
          outputModule -- the output module name
          dsCounter -- which dataset in the file to take (integer offset)
          fromFile -- indicates if initialization is from a workflow
                      file or from the database

        """

        # get database instance
        self.database = self.__class__.database

        # get logging instance
        self.logging = self.__class__.logging
        
        # verify source from dataset information
        if not fromFile:

            try:
                self.data = self.database.getDatasetInfo(info)
                
            except DatasetNotInDatabase, msg:
                self.logging.error( \
                    "Cannot initialize dataset %s from database (%s)" % \
                    (info, msg))
                raise
                
            # compute target dataset path
            primaryDataset = self.data['primaryDataset']
            processedDataset = self.data['processedDataset']
            dataTier = self.data['dataTier']
            
            if processedDataset.endswith('-unmerged'):
                targetDatasetPath = "/" + primaryDataset + "/" + \
                        str.replace(processedDataset,'-unmerged','')+ "/" + \
                        dataTier
            else:
                targetDatasetPath = "/" + primaryDataset + "/" + \
                        processedDataset + '-merged' + "/" + \
                        dataTier
                                    
            self.data['targetDatasetPath'] = targetDatasetPath
            
            # dataset loaded
            return 
                        
        try:
            
            # get output datasets
            outputDatasetsList = info.outputDatasets()

            # select the ones associated to the current output module
            datasetsToProcess = [ outDS \
                                  for outDS in outputDatasetsList \
                                  if outDS['OutputModuleName'] == outputModule]

            # there must be only one
            if len(datasetsToProcess) == 0:
                raise MergeSensorError( \
                    "MergeSensor exception: no output datasets specified")

            elif len(datasetsToProcess) > 1:
                self.logging.warning( \
                    "More than one dataset, processing  %s" % dsCounter)

            # get the dataset
            outputDataset = datasetsToProcess[dsCounter]
            
        except (IndexError, KeyError):
            raise MergeSensorError( \
                    "MergeSensor exception: wrong output dataset specification")
        
        # get primary Dataset
        try:
            primaryDataset = outputDataset['PrimaryDataset']
        except KeyError:
            raise MergeSensorError( \
              "MergeSensor exception: invalid primary dataset specification")

        # get datatier
        try:
            dataTier =  outputDataset['DataTier']
        except KeyError:
            raise InvalidDataTier( \
              "MergeSensor exception: DataTier not specified")

        # get processed
        try:
            processedDataset = outputDataset['ProcessedDataset']
        except KeyError:
            raise MergeSensorError( \
              "MergeSensor exception: invalid processed dataset specification")

        # build dataset name
        name = "/%s/%s/%s" % (primaryDataset, processedDataset, dataTier)
        
        # do not merge merged datasets
        if processedDataset.endswith("-merged"):
            raise NonMergeableDataset, \
              "Dataset %s is already merged, ignoring it" % name
        
        # get Merged LFN base
        try:
            mergedLFNBase = info.parameters['MergedLFNBase']
        except KeyError:
            mergedLFNBase = ''
            
        # get workflow name, category and time stamp
        workflowName = info.workflowName()
        category = info.requestCategory()
        timeStamp = info.requestTimestamp()
        
        # get application version
        try:
            version = outputDataset['ApplicationVersion']
        except:
            version = 'CMSSW_0_8_2'
        
        # get PSetHash
        try:
            psethash = outputDataset["PSetHash"]
        except:
            psethash = "12345678901234567890"

        # initialize it
        date = time.strftime('%Y-%m-%d %H:%M:%S')
        
        # define target dataset path
        if processedDataset.endswith('-unmerged'):
            targetDatasetPath = "/" + primaryDataset + "/" + \
                       str.replace(processedDataset,'-unmerged','') + "/" + \
                       dataTier
        else:
            targetDatasetPath = "/" + primaryDataset + "/" + \
                       processedDataset + '-merged' + "/" + \
                       dataTier
                                
        self.data = {'name' : name,
                     'primaryDataset' : primaryDataset,
                     'dataTier' : dataTier,
                     'processedDataset' : processedDataset,
                     'targetDatasetPath' : targetDatasetPath,
                     'PSetHash' : psethash,
                     'version' : version,
                     'workflowName' : workflowName,
                     'mergedLFNBase' : mergedLFNBase,
                     'category' : category,
                     'timeStamp' : timeStamp,
                     'status' : 'open',
                     'started' : date,
                     'lastUpdated' : date,
                     'outSeqNumber' : 1
                    }

        # test if it was inserted before
        try:
            info = self.database.getDatasetInfo(name)

        # not there, fine
        except DatasetNotInDatabase, msg:
            pass

        # it was registered before
        else:

            # if it is open, indicate it is currently watched
            if info['status'] == 'open':
                raise InvalidDataset, "Dataset %s is currently watched" % name
            
            # it it is closed, update its information and open it
            self.database.startTransaction()
            self.database.updateDataset(name)
            self.database.commit()
            return
        
        # new dataset in database, insert its information
        info = self.getData()
        self.database.startTransaction()
        
        try:
            self.database.insertDataset(info)
        except MySQLdb.IntegrityError:
            
            raise InvalidDataset, \
                  "Cannot insert dataset %s in database, duplicated?" % \
                       info['name']
        
        self.database.commit()
        
    ##########################################################################
    # set directory base path
    ##########################################################################
        
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
        
    ##########################################################################
    # set merge file size
    ##########################################################################

    @classmethod
    def setMergeFileSize(cls, maximum, minimum):
        """
        _setMergeFileSize_
        
        Set the target size of the merged files. Note that the size is stored
        in a class variable, meaning that its value is applicable to all
        datasets.
        
        Arguments:
            
          max -- maximum size of the resulting merged files.
          min -- minimum size of the resulting merged files
        
        Return:
            
          none

        """
        
        if max is not None:
            cls.maxMergeFileSize = maximum

        if min is not None: 
            cls.minMergeFileSize = minimum
        
    ##########################################################################
    # get components of a dataset name
    ##########################################################################
                          
    @classmethod
    def getNameComponents(cls, datasetName): 
        """
        __getNameComponents__
        
        Partition dataset name into components
        
        """
        
        pattern ="^/([\w\-]+)/([\w\-]+)/([\w\-]+)"
        match = re.search(pattern, datasetName)

        # wrong name
        if match is None:
            return ('', '', '')
        
        # get components
        return match.groups()

    ##########################################################################
    # define logging instance
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
    # define database
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
    # set maximum attempts for reading input files
    ##########################################################################

    @classmethod
    def setMaxInputFailures(cls, value):
        """
        _setMaxInputFailures_

        Set maximum attempts for reading input files that are causing
        troubles.

        Arguments:

          value -- the value for the attempts number

        Return:

          none

        """

        cls.maxInputAccessFailures = value

    ##########################################################################
    # set merge policy
    ##########################################################################

    @classmethod
    def setMergePolicy(cls, policy):
        """
        _setMergePolicy_

        Set the merge policy

        Arguments:

           policy -- the policy object

        Return:

          none

        """

        cls.policy = policy

    ##########################################################################
    # set the list of files in a dataset
    ##########################################################################

    def setFiles(self, fileList):
        """
        _setFiles_
        
        Used to update the list of files in the dataset. The argument must
        provide the complete list of files in the dataset as returned
        currently by DBS. Consistency checkings are performed between calls
        applied to the same dataset.
        
        Arguments:
            
          fileList -- a dictionary that specifies the list of all files
          in the named dataset organized by file block, providing for each
          file its size and number of events.
          
        Return:
            
          none
          
        """
        
        # get dataset id
        datasetId = self.database.getDatasetId(self.data['name'])
        
        # closed or wrong?
        if datasetId is None:
            
            # better do nothing...
            return
        
        # get file list from database
        files = self.database.getFileList(datasetId)

        # build a dictionary (thanks to Dan Bradley for suggesting it)
        fileDict = {}
        for afile in files:
            fileDict[afile['name']] = 1 

        # start transaction
        self.database.startTransaction()
        
        # add files not present in original structure
        for fileBlock in fileList.keys():
        
            # get all files
            allFiles = fileList[fileBlock]['Files']

            # check all of them
            for aFile in allFiles:

                # get file name
                lfn = aFile['LogicalFileName']

                # verify membership
                if not fileDict.has_key(lfn):

                    # not there, add it
                    self.database.addFile(datasetId, lfn, fileBlock, aFile)

        # update time
        date = time.asctime(time.localtime(time.time()))
        self.data['lastUpdated'] = date
        
        # update dataset info
        self.database.updateDataset(self.data['name'])
        
        # commit all changes
        self.database.commit()
        
    ##########################################################################
    # add a merge job
    ##########################################################################

    def addMergeJob(self, jobId, fileList, oldJobId):
        """
        _addMergeJobs_
        
        Add a new merge job to the dataset.
        
        Arguments:
            
          fileList -- the list of files that the job will start to merge
          jobId -- the job name
          fileList -- the list of input files
          oldJobId -- the name of the old job to resubmit

        Return:
            
          the name of the output file
          
        """
       
        # start transaction
        self.database.startTransaction()
        
        # get dataset id
        datasetId = self.database.getDatasetId(self.data['name'])
        
        # verify output file name
        if oldJobId is None:

            # new submission, create a name
            outputFile = "set" + str(self.data['outSeqNumber'])
            self.data['outSeqNumber'] = self.data['outSeqNumber'] + 1

            # add the job
            self.database.addJob(datasetId, outputFile, jobId, fileList)

        else:

            # use provided name
            self.logging.info('Resubmitting job as required')
            outputFile = self.database.resubmitJob(datasetId, oldJobId, \
                                                   jobId)
            
        # update time
        date = time.asctime(time.localtime(time.time()))
        self.data['last_update'] = date

        # update dataset
        self.database.updateDataset(self.data['name'], \
                                    sequenceNumber=self.data['outSeqNumber'])
        
        # commit changes
        self.database.commit()
        
        return outputFile

    ##########################################################################
    # select files for merging
    ##########################################################################

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

        # default selected set
        selectedSet = ([], 0)

        # set parameters
        parameters = {}
        parameters['maxMergeFileSize'] = self.__class__.maxMergeFileSize
        parameters['minMergeFileSize'] = self.__class__.minMergeFileSize

        # get dataset id
        datasetId = self.database.getDatasetId(self.data['name'])
        
        # get unmerged file lists organized by fileblocks
        fileList = self.database.getUnmergedFileList(datasetId)
        
        # check plugin
        policy = self.__class__.policy
        if policy == None:
            self.logging.error("Merge policy not set")
            return selectedSet

        # apply merge policy
        try:
            selectedSet = policy.applySelectionPolicy(fileList, parameters, \
                                                      forceMerge)

        # problems
        except Exception, msg:
            self.logging.error("Cannot apply merge policy: %s" % str(msg))
            return selectedSet

        # done, return it
        return selectedSet

    ##########################################################################
    # check for job resubmissions
    ##########################################################################

    def getNewJob(self):
        """
        _getNewJob_
        
        Check for merge jobs that have to be recreated, as indicated by
        field status in the corresponding entry in the merge database.
        Jobs are submitted as new jobs, not resubmissions of original
        ones. 
        
        Arguments:
            
          none
                    
        Return:
          
          the job name, the file block id and the file list
          
        """

        # get dataset id
        datasetId = self.database.getDatasetId(self.data['name'])
        
        # check for an output file in status 'do_it_again'
        newMerge = self.database.getMergeToBeDoneAgain(datasetId)
        
        # nothing to merge
        if newMerge == None:
            return (None, 0, [])
        
        # return job name and fileBlock
        return newMerge

    ##########################################################################
    # get dataset status
    ##########################################################################

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
    
    ##########################################################################
    # set dataset status
    ##########################################################################

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
        
        # start transaction
        self.database.startTransaction()
        
        # write dataset
        self.database.updateDataset(self.data['name'])
        
        # commit changes
        self.database.commit()

    ##########################################################################
    # get the name of a dataset
    ##########################################################################

    def getName(self):
        """
        _getName_
        
        Return the name of the dataset.
        
        Arguments:
            
          none
          
        Return:
            
          the dataset name as a string
          
        """
        
        if self.data is None:
            return None
        return self.data['name']
    
    ##########################################################################
    # get a copy of all dataset properties
    ##########################################################################

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

    ##########################################################################
    # remove dataset
    ##########################################################################

    def remove(self):
        """
        _getStatus_
        
        Remove all information from database
        
        Arguments:
            
          none
                    
        Return:
            
          none
          
        """
        
        # start transaction
        self.database.startTransaction()
        
        # remove all information
        self.database.removeDataset(self.data['name'])
        
        # commit changes
        self.database.commit()
        
    
    ##########################################################################
    # convert dataset to a string
    ##########################################################################

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
