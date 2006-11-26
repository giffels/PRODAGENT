#!/usr/bin/env python
"""
_Dataset_

Define the class Dataset, used to store information on a single dataset.
 
"""

import time
import re
import MySQLdb

__revision__ = "$Id: Dataset.py,v 1.24 2006/10/25 16:22:24 ckavka Exp $"
__version__ = "$Revision: 1.24 $"
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

    # list of data tiers
    dataTierList = []

    # logging instance
    logging = None
    
    # database instance
    database = None

    ##########################################################################
    # Dataset initialization
    ##########################################################################

    def __init__(self, info, outputModule = None, fromFile = False):
        """

        Initialize a Dataset. 
        
        The dataset information is loaded from the database. If
        not, a new dataset instance is created from the Workflow
        specification file
         
        Arguments:
            
          info -- the workflow specification or the dataset name
          outputModule -- the output module name
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
                                    dataTier + "/" + \
                                    str.replace(processedDataset,'-unmerged','')
            else:
                targetDatasetPath = "/" + primaryDataset + "/" + \
                                    dataTier + "/" + \
                                    processedDataset + '-merged'
                                    
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

            # the first one
            outputDataset = datasetsToProcess[0]
            
            # the others
            others = datasetsToProcess[1:]
            secondaryOutputTiers = [outDS['DataTier'] for outDS in others]
            
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

        # verify if valid
        if not self.validDataTier(dataTier):
            self.logging.info( \
              "Not valid dataTier %s, continuing..." % dataTier)
         
        # get poll datatier
        pollTier = dataTier.split("-")[0]
        
        # get processed
        try:
            processedDataset = outputDataset['ProcessedDataset']
        except KeyError:
            raise MergeSensorError( \
              "MergeSensor exception: invalid processed dataset specification")

        # build dataset name
        name = "/%s/%s/%s" % (primaryDataset, dataTier, \
                              processedDataset)
        
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
            targetDatasetPath = "/" + primaryDataset + "/" + dataTier + "/" + \
                                str.replace(processedDataset,'-unmerged','')
        else:
            targetDatasetPath = "/" + primaryDataset + "/" + dataTier + "/" + \
                                processedDataset + '-merged'
                                
        self.data = {'name' : name,
                     'primaryDataset' : primaryDataset,
                     'dataTier' : dataTier,
                     'pollTier' : pollTier,
                     'secondaryOutputTiers' : secondaryOutputTiers,
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
    # set list of possible datatiers
    ##########################################################################

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

    ##########################################################################
    # validates datatier
    ##########################################################################

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
        
        # names are separated with hyphens
        dataTiers = dataTierName.split("-")

        if dataTiers == []:
            return False
        
        for elem in dataTiers:
            if not elem in cls.dataTierList:
                return False
      
        return True
    
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
            
          fileList -- a list of tuples (file,size,fileBlock) that specifies
          the list of all files in the named dataset together with their size.
          
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
        
        # start transaction
        self.database.startTransaction()
        
        # add files not present in original structure
        for fileName, size, fileBlock in fileList:

            # verify membership
            found = False
            for aFile in files:
                if fileName == aFile['name']:
                    
                    # found
                    found = True
                    break
                
            if not found:
                self.database.addFile(datasetId, fileName, size, fileBlock)
                        
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

    def addMergeJob(self, fileList, jobId, oldFile):
        """
        _addMergeJobs_
        
        Add a new merge job to the dataset.
        
        Arguments:
            
          fileList -- the list of files that the job will start to merge
          jobId -- the job name
          oldFile -- the name of the old file in case of resubmission

        Return:
            
          the name of the output file
          
        """
        
        # start transaction
        self.database.startTransaction()
        
        # get dataset id
        datasetId = self.database.getDatasetId(self.data['name'])
        
        # verify output file name
        if oldFile == None:
        
            # new submission, create a name
            outputFile = "set" + str(self.data['outSeqNumber'])
            self.data['outSeqNumber'] = self.data['outSeqNumber'] + 1

        else:
            
            # use provided name
            self.logging.info('Resubmitting job as required')
            outputFile = oldFile
            
        # create outputFile
        (instance, fileId) = self.database.addOutputFile(datasetId, \
                                                outputFile, jobId)
                
        # mark input files as merged for new created output file
        if instance == 0:
            for aFile in fileList:    
                self.database.updateInputFile(datasetId, aFile, \
                                              status = "undermerge", \
                                              mergedFile = fileId)
        else:
            
            # add instance number to output file in case of resubmission
            outputFile = outputFile + "_" + str(instance)
            
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

          Algorithm is now reduced, not trying to do best fit,
          due to some changes in requirements. See CVS history
          for best fit bin allocation.          
        """

        # get file size
        maxMergeFileSize = self.__class__.maxMergeFileSize
        minMergeFileSize = self.__class__.minMergeFileSize

        # get dataset id
        datasetId = self.database.getDatasetId(self.data['name'])
        
        # get unmerged file lists organized by fileblocks
        fileList = self.database.getUnmergedFileList(datasetId)
        
        # check all file blocks in dataset
        for fileBlock in fileList:
       
            # get data
            fileBlockId, files = fileBlock

            # select set of files with at least mergeFileSize size
            totalSize = 0
            selectedSet = []
            numFiles = len(files)

            # start with the longest file
            startingFile = 0

            # ignore too large files in force merge
            tooLargeFiles = 0

            # try to start filling a bin
            while startingFile < numFiles:

                selectedSet = [files[startingFile][0]]
                totalSize = files[startingFile][1]
                leftIndex = startingFile + 1

                # verify that the file is not larger that maximum
                if totalSize > maxMergeFileSize:
                    self.logging.warning( \
                                    "File %s is too big, will not be merged" \
                                    % files[startingFile][0])
                    startingFile = startingFile + 1
                    tooLargeFiles = tooLargeFiles + 1
                    continue

                # continue filling it
                while totalSize < minMergeFileSize and \
                      totalSize < maxMergeFileSize and \
                      leftIndex < numFiles: 
            
                    # attempt to add other file
                    newSize = totalSize + files[leftIndex][1]

                    # check if we have not gone over maximum
                    if newSize < maxMergeFileSize:

                        # great, add it
                        selectedSet.append(files[leftIndex][0])
                        totalSize = newSize
                   
                    # still space, try to add the next one
                    leftIndex = leftIndex + 1

                # verify results
                if totalSize >= minMergeFileSize and \
                  totalSize < maxMergeFileSize:

                    # done
                    return (selectedSet, fileBlockId)

                # try starting bin from second file
                startingFile = startingFile + 1

            # not enough files, continue to next fileBlock
            # if forceMerge and list non-empty, return what we have
            # if forceMerge and list empty, make log entry and continue
            # with next fileBlock
            
            if forceMerge:

                # get a set of files which will not go over the maximum
                # even if the size can be smaller that minimum
                totalSize = 0
                selectedSet = []
                for fileName, size in files:

                    # ignore too large files
                    if tooLargeFiles > 0:
                        tooLargeFiles = tooLargeFiles - 1
                        continue

                    # try adding a new file
                    newSize = totalSize + size

                    # verify size
                    if newSize > maxMergeFileSize:

                        # too large
                        break

                    # add it
                    selectedSet.append(fileName)
                    totalSize = totalSize + size

                # verify if some files were selected or not
                if selectedSet == []:
                    self.logging.info( \
                       "Forced merge does not apply to fileblock %s " + \
                       "due to non mergeable condition" % fileBlockId)
                    continue
                else:

                    # ok, return them
                    return(selectedSet, fileBlockId)
            else:

                # no, try next file block
                continue

        # nothing to merge
        return ([], 0)

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
            
          the list of files to be merged, their fileBlockId and the
          name of the old output file.
          
        """

        # get dataset id
        datasetId = self.database.getDatasetId(self.data['name'])
        
        # check for an output file in status 'do_it_again'
        newMerge = self.database.getMergeToBeDoneAgain(datasetId)
        
        # nothing to merge
        if newMerge == None:
            return ([], 0, None)
        
        # return fileList, fileBlock and old file name
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
