#!/usr/bin/env python
#pylint: disable-msg=C0103

"""
_MergeSensorDB_

This is an interface module which on request calls the dialect specefic code at the backend. This module provides the database interface
for MergeSensor Component
"""
__author__ = "ahmad.hassan@cern.ch"

import os
import MySQLdb
import logging
from MergeSensor.MergeSensorDB.GetDAOFactory import getDAOFactory
from MergeSensor.Dataset import Dataset
from MergeSensor.MergeSensorError import MergeSensorDBError, \
                                         DatasetNotInDatabase, \
                                         DuplicateLFNError

#  //
# // MergeSensorDB interface class
#//


class MergeSensorDB:
    """
    _MergeSensorDB_
      
    This is an interface module which on request calls the dialect specefic code at the backend. This module provides the database interface
    for MergeSensor Component
    """  

    #  //
    # // Class Initialization
    #//
    def __init__ (self):
        """
        _init_
	   
        Initialization fucntion
        """  
          	  
        self.factory = getDAOFactory()
        self.transaction = False
        self.connection = None
        self.trans = None
	    
        return  #// End init
	  

    def connect (self):
        """
        _connect_
 
        Return DB Connection
        """ 	
	  
        if self.connection is None:
	  
           self.connection = self.factory.dbinterface.engine.connect()   
        else:
             
           self.commit()
           self.connection = self.factory.dbinterface.engine.connect()   

        self.trans =  None
        self.transaction = False

	   
        return  #//End connect
	  
    def commit (self):  	  
        """
        _commit_
	  
        Commits the on going transaction 
        """
	  
        try:
          if self.connection is not None and self.trans is not None:
               self.trans.commit()
               self.connection = None
               self.transaction = False
               self.trans = None
	     
        except Exception, ex:
	     msg = 'Commit FAILED'
	     msg += str(ex)
	     logging.error(ex) 
	  
        return #// End commit
	  
    def startTransaction (self):
        """
        __startTransaciton__
        
        Start a transaction, performing an implicit commit if
        necessary.

        Arguments:
        
          none
          
        Return:
            
          none

        """
	  
        self.connect()
        self.trans = self.connection.begin()
        self.transaction = True
	  
        return  #// End startTransaction
	  
    def rollback(self):
        """
        __rollback__
        
        The operation rollback discards all operations in the
        current transaction. 

        Arguments:
        
          none
          
        Return:
            
          none
        """
	  
        try:
	  
           self.trans.rollback()
           self.transaction = False
           self.connection = None
           self.trans = None
	     
        except Exception, ex:
             msg = 'Commit FAILED'
	     msg += str(ex)
	     logging.error(ex) 
	  
	  #// End rollback  



    def getDatasetList(self): 
        """
        __getDatasetList__
        
        Get the list of all datasets.
        
        Arguments:
        
          none
          
        Return:
           
          the list of all dataset names which are currently in open status
        """ 	  
	
        getDatasetList = self.factory(classname = "Dataset.GetDatasetList")	
        rows = getDatasetList.execute(conn = self.connection, trans = self.transaction)
  
	
        # return empty list if no watched datasets
        if len(rows) == 0:
            
           # emtpy list
           return []
	    
        #//Convert each tuple to one string
        datasetList = ["/%s/%s/%s" % elem for elem in rows]


	
        return  datasetList   #// End getDatasetList
	
    ##########################################################################
    # get dataset name from workflow name
    ##########################################################################
    def getDatasetListFromWorkflow(self, workflowName):
        """
        __getDatasetListFromWorkflow__
        Get the list of datasets belonging to a workflow
                                                                                
        Arguments:
                                                                                
          workflow name
                                                                                
        Return:
                                                                                
          the list of all dataset names which are currently in open status
          and belong to the workflow
        """
	  
        getDatasetList = self.factory(classname = "Dataset.GetDatasetListFromWorkflow")	
        rows = getDatasetList.execute(workflowName, conn = self.connection, trans = self.transaction)
	  
        # return empty list if no watched datasets
        if rows == 0:                                                                               
                                                                            
          # emtpy list
          return []
                                                                                
        #//Convert each tuple to one string
        datasetList = ["/%s/%s/%s" % elem for elem in rows]
	  
        return datasetList  #// End getDatasetListFromWorkflow
	  
    ##########################################################################
    # get dataset information
    ##########################################################################
	  
    def getDatasetInfo(self, datasetName): 
        """
        __getDatasetInfo__
         
        Get information on dataset (on any status)
          
        Arguments:
        
          datasetName -- the name of the dataset
          
        Return:
            
          a dictionary with all dataset information from database

        """ 
        # get name components
        (prim, processed, tier) = Dataset.getNameComponents(datasetName)
	  
        getDatasetList = self.factory(classname = "Dataset.GetDatasetInfo")	
        rows = getDatasetList.execute(prim, tier, processed, conn = self.connection, trans = self.transaction)
	  
        # dataset not registered
        if len(rows) == 0:
            
          # generate exception
          raise DatasetNotInDatabase, \
                 'Dataset %s is not registered in database' % datasetName    

        # store information        
        datasetInfo = rows[0]

        # add extra fields
        datasetInfo['name'] = datasetName

        
        return datasetInfo #//END GetDatasetInfo  
	  
	 
    ##########################################################################
    # get dataset id
    ##########################################################################
            
    def getDatasetId(self, datasetName):
        """
        __getDatasetId__
         
        Get the id of the dataset
        
        Arguments:
        
          datasetName -- the name of the dataset
          
        Return:
            
          the dataset id identification in the database
          
        """ 
	  
        # get name components
        (prim, processed, tier) = Dataset.getNameComponents(datasetName)
	  
        getDatasetId = self.factory(classname = "Dataset.GetDatasetId")	
        row = getDatasetId.execute(prim, tier, processed, conn = self.connection, trans = self.transaction)
	  
	  
        # dataset not registered
        if len(row) == 0:

           return None

        # get id, status

        dataset = row

        # check for closed status
        if dataset['status'] == 'closed':
            return None

	   
        return dataset['id']    #//End getDatasetId
	  
	  
	     
    ##########################################################################
    # get list of files of a dataset
    ##########################################################################
            
    def getFileListFromDataset(self, datasetName):
        """
        __getFileList__
        
        Get the list of files associated to a dataset
        
        Arguments:
        
          datasetName -- the name of the dataset
          
        Return:
            
          the list of files in the dataset

        """

        datasetId = self.getDatasetId(datasetName)
        
        if datasetId is None:
            return []
        
        return self.getFileList(datasetId)
	  

 
    ##########################################################################
    # get list of files of a dataset
    ##########################################################################
            
    def getFileList(self, datasetId):
        """
        __getFileList__
        
        Get the list of files associated to a dataset (specified by id)
        
        Arguments:
        
          datasetId -- the dataset id in database
        
        Return:
            
          the list of files in the dataset
        """
 
 
        getFileList = self.factory(classname = "File.GetFileList")	
        rows = getFileList.execute(datasetId, conn = self.connection, trans = self.transaction)
	  
        # return empty list
        if len(rows) == 0:
            
          # empty set
          return []

        # return it
        return rows


    def getDatasetFileMap(self, datasetId):
        """
        _getDatasetFileMap_

        Retrieve a mapping of input LFN to output LFN for completed
        merges.

        """
        getDatasetFileMap = self.factory(classname = "Dataset.GetDatasetFileMap")	
        rows = getDatasetFileMap.execute(datasetId, conn = self.connection, trans = self.transaction)
  
        
        # return empty list
        if len(rows) == 0:
    
           # empty set
           return {}
        

        #  //
        # // Reformat into a dictionary of input LFN: output LFN
        #//  NULL entries are converted to None
        result = {}
        [ result.__setitem__(x['name'], x['lfn']) for x in rows ]
        
        # return it
        return result   #// End getDatasetFileMap
	  
	  
	  
	  
	  
    def getFileBlocks(self, datasetId):
        """
        _getFileBlocks_
 
        Return a map of LFN:Block name for all unmerged LFNs
        in the dataset provided

        """
        
        getFileBlocks = self.factory(classname = "File.GetFileBlocks")	
        rows = getFileBlocks.execute(datasetId, conn = self.connection, trans = self.transaction)
  
       
        # return empty list
        if len(rows) == 0:
            
            # empty set
            return {}
        
        
        result = {}
        [ result.__setitem__(x['file'], x['block']) for x in rows ]
        
        # return it
        return result  #// End getFileBlocks
	
	
	
	
    def removalInfo(self, datasetId):
        """
        _removalInfo_
 
        Return a map of LFN: removal status

        """
        getRemovalInfo = self.factory(classname = "Status.RemovalInfo")	
        rows = getRemovalInfo.execute(datasetId, conn = self.connection, trans = self.transaction)
	  
        result = {}
        for entry in rows:
            result[entry[0]] = entry[1]

        return result

    def removingState(self, *files):
        """
        _removingState_

        Flag the files listed as removing
        """
	  
        removingState = self.factory(classname = "Status.RemovingState")	
        rowcount = removingState.execute(conn = self.connection, trans = self.transaction, *files)

        return   #// End removingState


    def removedState(self, *files):
        """
        _removingState_

        Flag the files listed as removing
        """
	  
        removedState = self.factory(classname = "Status.RemovedState")	
        rowcount = removedState.execute(conn = self.connection, trans = self.transaction, *files)

        return   #// End removingState


    def unremovedState(self, *files):
        """
        _removingState_
        Flag the files listed as removing

        """
	  
        unremovedState = self.factory(classname = "Status.UnRemovedState")	
        rowcount = unremovedState.execute(conn = self.connection, trans = self.transaction, *files)

        return   #// End removingState
	  
	  
	  
	  
    def getJobList(self, datasetName):
        """
        __getJobList__
        
        Get the list of merge jobs started on a dataset
        
        Arguments:
        
          datasetName -- the name of the dataset
          
        Return:
            
          the list of merge jobs

        """

        datasetId = self.getDatasetId(datasetName)
	
        getJobList = self.factory(classname = "Job.GetJobList")	
        rows = getJobList.execute(datasetId, conn = self.connection, trans = self.transaction)
        
        if len(rows) == 0:
            
            # empty set
           return []
        
        # build list
        mergeJobs = rows
        
        # remove extra level in lists
        mergeJobs = [job[0] for job in mergeJobs]
        

        # return it
        return mergeJobs
	  
	
    ##########################################################################
    # get list of files of a dataset
    ##########################################################################
            
    def getUnmergedFileListFromDataset(self, datasetName):
        """
        __getUnmergedFileListFromDataset__
        
        Get the list of unmerged files associated to a dataset
        
        Arguments:
        
          datasetName -- the name of the dataset
          
        Return:
            
          the list of files in the dataset

        """

        datasetId = self.getDatasetId(datasetName)
      
        if datasetId is None:
            return []
        
        return self.getUnmergedFileList(datasetId)
	  
	  
	  
    def getUnmergedFileList(self, datasetId):
        """
        __getUnmergedFileList__
        
        Get the list of unmerged files associated to a dataset organized by
        fileblock
        
        Arguments:
        
          datasetId -- the dataset id in database
          
        Return:
            
          the list of unmerged files in the dataset

        """
	
        getDatasetBlocks = self.factory(classname = "Dataset.GetDatasetBlocks")	
        rows = getDatasetBlocks.execute(datasetId, conn = self.connection, trans = self.transaction)
        
        
        # return empty list
        if len(rows) == 0:
            
            # empty set
            return []
        
        # build list
        blocks = rows



        # start building resulting list
        fileList = []
        
        for block in blocks:
            
            getUnmergedFiles = self.factory(classname = "File.GetUnmergedFiles")	
            rows = getUnmergedFiles.execute(datasetId, block, conn = self.connection, trans = self.transaction)

            # append to list
            fileList.append((block['name'], rows))
            

        # return it
        return fileList




    ##########################################################################
    # get list of files of a dataset
    ##########################################################################
            
    def getJobInfo(self, jobId):
        """
        __getFileList__
        
        Get the job information
        
        Arguments:
        
          jobId -- the job name
           
        Return:
            
         a dictionary with all job information

        """

        # start building result dictionary
        result = {'jobName' : jobId}
	
        getJobProperties = self.factory(classname = "Job.GetJobProperties")	
        rows = getJobProperties.execute(jobId, conn = self.connection, trans = self.transaction)
       
        if len(rows) == 0:

            # nothing
            return None
        
        # add information to result
        result['outputFile'] = rows['outputfile']
        result['datasetName'] = "/" + rows['prim'] + \
                                  "/" + rows['processed'] + \
                                  "/" + rows['tier']
        result['status'] = rows['status']
        
        # get output file id
        fileId = rows['fileid']
        result['fileId'] = fileId
        
        # get now all associated input files
        getAssociatedInputFiles = self.factory(classname = "Job.GetAssociatedInputFiles")	
        rows = getAssociatedInputFiles.execute(fileId, conn = self.connection, trans = self.transaction)
        
        # add to result
        if len(rows) != 0:
            

            # store input file information
            result['inputFiles'] = [aFile['filename'] for aFile in rows]
            result['fileBlock'] = rows[0]['blockname']
            
        else:
            
            # no information (failed job)
            result['inputFiles'] = []
            result['fileBlock'] = ''
        
        # return it
        return result




    ##########################################################################
    # add a file to a dataset
    ##########################################################################
            
    def addFile(self, datasetId, fileName, fileBlock, data):
        """
        __addFile__
        
        Add a file specification (and possibly a fileblock) to
        a dataset (specified by Id)
        
        Arguments:
        
          datasetId -- the dataset id in database
          fileName -- the file to add
          fileBlock -- the file block name
          data -- the parameters of the file
          
        Return:
            
          none

        """

        blockId = self.addFileBlock(fileBlock)

        fileSize = data['FileSize']
        events = data['NumberOfEvents']

	
        guid = "\"%s\"" % os.path.basename(fileName) 
	
      
		    
        fileId = 'last_insert_id()'
        addFile = self.factory(classname = "File.AddFile")	
        (fileId, rowcount) = addFile.execute(fileName, guid, blockId, datasetId, fileSize, events, conn = self.connection, trans = self.transaction)
	
         
        
        #// Add lumi info of each processed job to database
	

     
        
        # cannot be inserted
        if rowcount == 0:
            raise MergeSensorDBError, \
                   'Insertion of file %s failed' % fileName
		   


        return fileId  #// End AddFile
	  
	  
    ##########################################################################
    # get input file information
    ##########################################################################

    def getInputFileInfo(self, datasetId, fileName):
        """
        __getFileInfo__

        Get input file information.

        Arguments:

          datasetId -- the dataset id in database
          fileName -- the file name used to update

        Return:
           A dictionary with input file information
        """

        
        getInputFileInfo = self.factory(classname = "File.GetInputFileInfo")	
        row = getInputFileInfo.execute(datasetId, fileName, conn = self.connection, trans = self.transaction) 
	
        if len(row) == 0:

            # nothing
            return None


        # process it
        row['dataset'] = '/' + row['prim'] + '/' + row['tier'] + '/' + \
                       row['processed']
        del row['prim']
        del row['tier']
        del row['processed']


        # return it
        return row   





    ##########################################################################
    # update input file information
    #
    # no checking is performed since nothing prevent to update it to the same
    # status
    ##########################################################################
            
    def updateInputFile(self, datasetId, fileName, status = "merged", \
                        mergedFile = None, maxAttempts = None):
        """
        __updateInputFile__
        
        Update input file information, file should exist
        
        If the transition is from 'merged' to 'unmerged' status,
        increment instance counter.

        If the transition is from 'undermerge' to 'unmerged',
        increment failures counter.

        if maxAttempts is specified and number of failures is exceeded,
        set to 'invalid'

        No checking is performed on the update result, since nothing
        prevent to update it to the same status.

        Arguments:
        
            datasetId -- the dataset id in database
            fileName -- the file name used to update
            status -- new status (if any)
            mergedFile -- the merged output file id (if any)
            maxAttempts -- number of maximum failures allowed (if any)
          
        Return:
            
          the new status of the file 

        """
    
        
        getFileInfo = self.factory(classname = "File.GetFileInfo")	
        row = getFileInfo.execute(datasetId, fileName, conn = self.connection, trans = self.transaction) 
	

         

        # file does not exist
        if len(row) == 0:

            # generate exception
            raise MergeSensorDBError, \
             'Cannot update file %s, not registered in dataset.' % fileName

        # define mergedFileInformation
        if mergedFile is None:
            mergedFileUpdate = ''
        else:
              mergedFileUpdate = ", mergedfile='" + str(mergedFile) + "' "
            
        # increment instance counter for transitions from 'merged' to
        # 'unmerged'
     
        oldStatus = row['status']
        if oldStatus == 'merged' and status == 'unmerged':
            instanceUpdate = ', instance=instance+1'
        else:
            instanceUpdate = ''

        # increment failures counter for transitions from 'undermerge' to
        # 'unmerged'
        if oldStatus == 'undermerge' and status == 'unmerged':
            failuresUpdate = ', failures=failures+1'

            # verify exceeded failures condition
            failures = row['failures'] + 1

            if maxAttempts is not None:
                if failures >= maxAttempts:
                    status = "invalid"

        # no failures
        else:
            failuresUpdate = ''

        # insert dataset information
        updateInputDatasetInfo = self.factory(classname = "File.UpdateInputDatasetInfo")	
        rowcount = updateInputDatasetInfo.execute(status, mergedFileUpdate, instanceUpdate, failuresUpdate, datasetId, fileName, conn = self.connection, trans = self.transaction) 


        # return status
        return status    #// End updateInputFile



    ##########################################################################
    # update output file information
    ##########################################################################

    def updateOutputFile(self, datasetId, fileName = '', jobName = '', \
                         status = "merged", incrementFailures = False, \
                         lfn = ""):
        """
        __updateOutputFile__
  
          Update output file information. Assume that the file exists.

          No checking is performed on the update result, since nothing
          prevent to update it to the same status.

          Arguments:

            datasetId -- the dataset id in database
            fileName -- the file name used to update or...
            jobName -- ...equivalently, the job name can be used
                       (fileName has precedence)
            status -- new status (if any)
            incrementFailures -- indicates if failures counter must be
                                 incremented
            lfn -- output file LFN
          Return:

            none

        """

        # get fileName or jobName
        if fileName != '':
            checkCondition = "name='" + fileName + "'"
        else:
            checkCondition = "mergejob='" + jobName + "'"

        # increment failures if required
        if incrementFailures:
            failuresUpdate = ', failures=failures+1'
        else:
            failuresUpdate = ''

        # update LFN if required
        if lfn != '':
            lfnUpdate = ", lfn = '" + lfn + "'"
        else:
            lfnUpdate = ''


        # insert dataset information
        updateOutputDatasetInfo = self.factory(classname = "File.UpdateOutputDatasetInfo")	
        rowcount = updateOutputDatasetInfo.execute(status, failuresUpdate, lfnUpdate, datasetId, checkCondition, conn = self.connection, trans = self.transaction) 
 
        return    #// END updateOutputFile
	  
	   

    ##########################################################################
    # invalidate input file
    ##########################################################################
            
    def invalidateFile(self, datasetName, fileName):
        """
        __invalidateFile__
      
          Invalidate input file information, file should exist
        
          Arguments:
        
            datasetName -- the dataset name
            fileName -- the file name to invalidate
          
          Return:
            
            none
        """
    
        # get dataset id
        datasetId = self.getDatasetId(datasetName)
          
          
        getInputFileStatus = self.factory(classname = "File.GetInputFileStatus")	
        rowcount = getInputFileStatus.execute(datasetId, fileName, conn = self.connection, trans = self.transaction) 
                        
   
        # process results
        rows = rowcount
        
        # file does not exist
        if rows == 0:
            
            # generate exception
            raise MergeSensorDBError, \
             'Cannot invalidate file %s, not registered in dataset %s.' \
                 % (fileName, datasetName)
		 
        newStatus = 'invalid'     
        updateInputFileStatus = self.factory(classname = "File.UpdateInputFileStatus")	
        rowcount = updateInputFileStatus.execute(datasetId, fileName, newStatus, conn = self.connection, trans = self.transaction)     
	
        return   #// End invalidateFile
	
	
	
    ##########################################################################
    # add job
    ##########################################################################

    def addJob(self, datasetId, fileName, jobId, fileList):
        """
        __addJob__

          Add a new job.

          Arguments:

            datasetId -- the dataset id in database
            fileName -- the output merged file name
            jobId -- the job name
            fileList -- the list of input files

          Return:

            none

        """
        insertMergeJob = self.factory(classname = "Job.InsertMergeJob")	
        (fileId, rowcount) = insertMergeJob.execute(datasetId, fileName, jobId, conn = self.connection, trans = self.transaction)     
	

        rows = rowcount 
        # wrong insert?
        if rows == 0:


            # generate exception
            raise MergeSensorDBError, \
               'Insertion of outputfile %s failed' % fileName

        # update input file status
        for aFile in fileList:

            updateJobStatus = self.factory(classname = "Job.UpdateJobStatus")	
            rows = updateJobStatus.execute(datasetId, aFile, conn = self.connection, trans = self.transaction)   	     

            # cannot be updated
            if rows == 0:

                # generate exception

                raise MergeSensorDBError, \
                         'Update operation on file %s failed' % aFile
    
        return   #// END addJob
	
	
      ##########################################################################
      # resubmit job
      ##########################################################################

    def resubmitJob(self, datasetId, jobId, newJobId):
        """
        __resubmitJob__

          resubmit an existing job.

          Arguments:

            datasetId -- the dataset id in database
            fileName -- the output merged file name
            jobId -- the job name
            newJobId -- the new job id

          Return:

            the output file

        """

        getOutputFileInfo = self.factory(classname = "Job.GetOutputFileInfo")	
        row = getOutputFileInfo.execute(datasetId, jobId, conn = self.connection, trans = self.transaction)   	     
       

        # the job is not there!
        if len(row) == 0:

            # generate exception
            raise MergeSensorDBError, \
               'Merge job %s is not in database, cannot resubmit' % jobId


        mergeJobId = row['id']
        name = row['name']
        instance = row['instance']
        
        # define the new file name
        indexHyphen = name.find('_')
        if indexHyphen != -1:
            name = name[:indexHyphen]
        name = name + '_' + str(instance)


        updateOutputFileInfo = self.factory(classname = "Job.UpdateOutputFileInfo")	
        (fileId, rowcount) = updateOutputFileInfo.execute(name, newJobId, mergeJobId, conn = self.connection, trans = self.transaction)   	     
       

        # check result
        rows = rowcount

        # wrong update?
        if rows == 0:

            # generate exception
            raise MergeSensorDBError, \
               'Update of resubmitted job %s failed' % jobId

        updateInputFileStatus = self.factory(classname = "Job.UpdateInputFileStatus")	
        rowcount = updateInputFileStatus.execute(datasetId, mergeJobId, conn = self.connection, trans = self.transaction)   	     
       


        return name   #// END resubmitJob
	  
	  
	  
    ##########################################################################
    # get an output file to be remerged
    ##########################################################################

    def getMergeToBeDoneAgain(self, datasetId):
        """
        __getMergeToBeDoneAgain__
        
          Get an output file to be merged again
        
          Arguments:
        
            datasetId -- the dataset id in database
          
          Return:
            
            the name of the job, the file block id and the list of files

        """
        
        getOutputFileInfo = self.factory(classname = "File.GetOutputFileInfo")	
        row = getOutputFileInfo.execute(datasetId, conn = self.connection, trans = self.transaction)   	     
       

        # any job?
        if len(row) == 0:
            
            # no
            return None

        fileId = row['id']
        fileName = row['name']
        jobName = row['mergejob']
   
        getAssociatedInputFiles = self.factory(classname = "File.GetAssociatedInputFiles")	
        rows = getAssociatedInputFiles.execute(datasetId, fileId, conn = self.connection, trans = self.transaction)   	     
       
        # any file?
        if len(rows) == 0:

            # no input files, should not be the case, ignore
            return None

        fileList = [aFile['name'] for aFile in rows]
        fileBlock = rows[0]['block']
       
              
        getBlockName = self.factory(classname = "File.GetBlockName")	
        row = getBlockName.execute(fileBlock, conn = self.connection, trans = self.transaction)   	     
        

        # nothing, mmm, something wrong, just ignore
        if len(row) == 0:
            
     
            # nothing
            return None
        

        blockName =  row['name']
        

        # pack and return everything
        return (jobName, blockName, fileList)
	  
	  
	  

    ##########################################################################
    # redo job
    ##########################################################################
            
    def redoJob(self, jobName):
        """
        __redoJob__
        
          Flag job to be redone
        
          Only succesfully finished jobs can be re-executed or
          failed jobs with invalid input files.

          Note that even if a job has failed, but its files are
          still valid, next jobs will try to remerge them.

        
          Arguments:
        
            jobName -- the job name
                    
          Return:
            
            none
        """
    
        # get job info
        jobInfo = self.getJobInfo(jobName)
        
        # verify it exists
        if jobInfo is None:
            raise MergeSensorDBError, \
               "Cannot resubmit job %s, it does not exist" % jobName
               
        # verify its status
        if jobInfo['status'] == 'do_it_again':
            raise MergeSensorDBError, \
               "Job %s is already flagged for resubmission" % jobName
               
        # verify that is not runnig now
        if jobInfo['status'] == 'undermerge':
            raise MergeSensorDBError, \
               "Cannot resubmit job %s, it has not finished yet." % jobName
            
        # if it has failed, check input files. Should be all invalid.
        if jobInfo['status'] == 'failed':

            # get associated input file information

            getAssociatedFiles = self.factory(classname = "Job.GetAssociatedFiles")	
            rows = getAssociatedFiles.execute(jobInfo, conn = self.connection, trans = self.transaction)   	     

          # no files should be in a valid state
            if rows != 0:
  
                # generate exception
                raise MergeSensorDBError, \
                   "Cannot resubmit failed job %s, files not yet invalidated." \
                   % jobName

        # update output file informarion
	
        updateOutputFileStatus = self.factory(classname = "Job.UpdateOutputFileStatus")	
        rows = updateOutputFileStatus.execute(jobName, conn = self.connection, trans = self.transaction) 

        # cannot be uupdated
        if rows == 0:
            raise MergeSensorDBError, \
                   'Update operation for job %s failed' % jobName
		   
        return   #// End redoJob 		   
	
	
	
    ##########################################################################
    # update dataset information
    #
    # no checks are done since consecutive updates do not affect rows
    ##########################################################################
            
    def updateDataset(self, datasetName, sequenceNumber = None):
        """
        __updateDataset__
        
          Update 'last update' field and mark it as open
        
          Arguments:
        
            datasetName -- the dataset name
            sequenceNumber -- the sequence number
          
          Return:
            
            none

        """
        
        # get name components
        (prim, processed, tier) = Dataset.getNameComponents(datasetName)
        
        # build sequence update string
        if sequenceNumber is not None:
            sequenceString = ", sequence=" + str(sequenceNumber)
        else:
            sequenceString = ""
         
            
        # insert dataset information
	
        updateDataset = self.factory(classname = "Dataset.UpdateDataset")	
        rowcount = updateDataset.execute(sequenceString, prim, tier, processed, conn = self.connection, trans = self.transaction) 
	
        return   #// END updateDataset


    def insertDataset(self, data):
        """
        __insertDataset__
        
          insert dataset information
        
          Arguments:
        
            data -- the dataset associated dictionary
          
          Return:
            
            none

        """

        # insert dataset information

        insertDataset = self.factory(classname = "Dataset.InsertDataset")	
        (fileId, rowcount) = insertDataset.execute(data, conn = self.connection, trans = self.transaction) 
	
        # check for insertion status
        rows = rowcount

        # problems
        if rows == 0:        
            raise MergeSensorDBError, \
                   'Insertion of dataset %s failed' % data['name']
		   
        return  #// End insertDataset 


    ##########################################################################
    # close dataset
    ##########################################################################
                                                                                
    def closeDataset(self, datasetName): 
        """
        __closeDataset__
        
          close a dataset
        
          Arguments:
        
            datasetName -- the name of the dataset
          
          Return:
            
            none

        """
        
        # get name components
        (prim, processed, tier) = Dataset.getNameComponents(datasetName)
        
        closeDataset = self.factory(classname = "Dataset.CloseDataset")	
        rowcount = closeDataset.execute(prim, processed, tier, conn = self.connection, trans = self.transaction) 
	 
        # process results
        rows = rowcount

        # dataset not registered
        if rows == 0:
            raise MergeSensorDBError, \
                   'Cannot close dataset %s, not registered in database' \
                        % datasetName
			
			   
        return   #// END closeDataset
	  
	  
	  
    ##########################################################################
    # remove dataset
    ##########################################################################
                                                                                
    def removeDataset(self, datasetName): 
        """
        __removeDataset__
        
          Remove a dataset and all its associated files
        
          Be carefull: removes the dataset, plus all input files information
          and also all output file (jobs) information associated to it.
        
          Arguments:
        
            datasetName -- the name of the dataset
          
          Return:
            
            none

        """
        
        # get name components
        (prim, processed, tier) = Dataset.getNameComponents(datasetName)
        
        
        removeDataset = self.factory(classname = "Dataset.RemoveDataset")	
        rowcount = removeDataset.execute(prim, processed, tier, conn = self.connection, trans = self.transaction) 
	 
        # process results
        rows = rowcount
        
        # dataset not registered 
        if rows == 0:

            # generate exception
            raise MergeSensorDBError, \
                   'Cannot remove dataset %s, not registered in database' \
                        % datasetName    

        # remove now all orphan fileblocks
        removeOrphanBlocks = self.factory(classname = "Dataset.RemoveOrphanBlocks")	
        rowcount = removeOrphanBlocks.execute(prim, processed, tier, conn = self.connection, trans = self.transaction) 
	 
        return  #// removeDataset
	
	
	
	
    ##########################################################################
    # insert workflow
    ##########################################################################
            
    def insertWorkflow(self, dataset, workflow):
        """
        __insertWorkflow__
        
          insert workflow information
        
          Arguments:
        
            dataset -- the dataset name
            workflow -- the workflow name
          
          Return:
            
            none

        """

        # get dataset id
        datasetId = self.getDatasetId(dataset)
        
        getWorkflow = self.factory(classname = "Workflow.GetWorkflow")	
        rowcount = getWorkflow.execute(workflow, datasetId, conn = self.connection, trans = self.transaction) 
	 
        # check for number of workflows
        rows = rowcount
        
        # the workflow is not new
        if rows != 0:
            
            return False
        
        # insert the workflow
        insertWorkflow = self.factory(classname = "Workflow.InsertWorkflow")	
        rowcount = insertWorkflow.execute(workflow, datasetId, conn = self.connection, trans = self.transaction) 
	 
        # check for insertion status
        rows = rowcount

        # problems
        if rows == 0:        
            raise MergeSensorDBError, \
                   'Insertion of workflow %s failed' % workflow
                   
        # indicate a new worklow
        return True
	  
	  
	  
	  
	  
	  
	  
    ##########################################################################
    # get merge sensor status
    ##########################################################################
            
    def getStatus(self):
        """
        __getStatus__
        
          Get the Merge Sensor status
        
          Arguments:
          
            none
          
          Return:
            
            a dictionary with the status information

        """

        getMergeSensorStatus = self.factory(classname = "Status.GetMergeSensorStatus")	
        rows = getMergeSensorStatus.execute(conn = self.connection, trans = self.transaction) 
	 

        
        # no information, insert default
        if len(rows) == 0:
	
            insertDefaultStatus = self.factory(classname = "Status.InsertDefaultStatus")	
            rows = insertDefaultStatus.execute(conn = self.connection, trans = self.transaction) 
	 

            # problems
            if rows == 0:        
                
                # generate exception
                raise MergeSensorDBError, \
                       'Insertion of MergeSensor status into database failed'

 
            getMergeSensorStatus = self.factory(classname = "Status.GetMergeSensorStatus")	
            rows = getMergeSensorStatus.execute(conn = self.connection, trans = self.transaction) 
	 

        
        # remove id
        try:
            del rows['id']
        except KeyError:
            pass


        # return it
        return rows

    ##########################################################################
    # set status
    ##########################################################################
	
    def setStatus(self, statusInfo): 
        """
        __setStatus__
        
          set Merge Sensor status information into the database
        
          Arguments:
        
            statusInfo -- the status information
          
          Return:
            
            none
        """
        

        # build update command
        updates = ''
        separator = ''
        
        # for every key
        for (key, value) in statusInfo.items():

            updates = updates + separator + key + "='" + str(value) + "'"
            separator = ', \n'

        # no updates for empty argument
        if updates == '':
            return
        

 
        setMergeSensorStatus = self.factory(classname = "Status.SetMergeSensorStatus")	
        rows = setMergeSensorStatus.execute(updates, conn = self.connection, trans = self.transaction) 
	
        return    #//setStatus 
	
    ##########################################################################
    # close database connection
    ##########################################################################
                                                                                
    def closeDatabaseConnection(self): 
        """
        __closeDatabaseConnection__
         
         close the database connection
        
          Arguments:
        
            none
          
          Return:
            
            none

        """
     
        self.connection = None
        self.trans = None
        self.transaction = False
	   
        return   #//END   closeDatabaseConnection
	  
	  
	  
    ##########################################################################
    # get version information
    ##########################################################################


    def getSchema(self):
        """
        _getSchema_
        
          return schema information
        
          Creates its own new connection. Intended to be used
          from outside PA in order to get information on the
          current database schema.
        """
        
 
        getSchema = self.factory(classname = "Schema.GetSchema")	
        rows = getSchema.execute(conn = self.connection, trans = self.transaction) 
	
        # return it
        return rows       
	  
	  
    def addFileBlock (self, fileBlock):
        """
        _addFileBlock_
  	
        """

        getBlockId = self.factory(classname = "File.GetBlockId")	
        row = getBlockId.execute(fileBlock, conn = self.connection, trans = self.transaction)         

  
        # the block is new
        if len(row) == 0:

            blockId = 'last_insert_id()'
            insertFileBlock = self.factory(classname = "File.InsertFileBlock")	
            (blockId, rowcount) = insertFileBlock.execute(fileBlock, conn = self.connection, trans = self.transaction)         

            # check for file block
            rows = rowcount
            
            # wrong insert?
            if rows == 0:

                # generate exception
                raise MergeSensorDBError, \
                   'Insertion of file block %s failed' % fileBlock
       
        # block is not new 
        else:    

            blockId = "'" + str(row[0]) + "'"
	

            #// Return newly inserted file block id
        return blockId
	  
	  
	  
    def addLumiInfo (self, lumiSections, fileId):
        """
        _addLumiInfo_ 
 	
          Input: 
	
	      lumiSections: List of dictionaries where each dictionary contains each lumisection info
	
          return: None

        """
	
        #// Adding merge input lumi mappings.
        logging.info('Adding lumi Info...')
        temp = []
        temp = [{'LumiSectionNumber': x['LumiSectionNumber'], 'RunNumber': x['RunNumber'], 'fileId': fileId} for x in lumiSections] 

        if len(temp) != 0:  
           insertInputLumiInfo = self.factory(classname = "File.InsertOutputLumiInfo")	
           rowcount = insertInputLumiInfo.execute(temp, conn = self.connection, trans = self.transaction)         

        return  #// addLumiInfo
	
	
	
    def addInputLumiInfo (self, lumiSections, fileId):
        """
        _addLumiInfo_

          Input:

              lumiSections: List of dictionaries where each dictionary contains each lumisection info

          return: None

        """

        #// Adding merge input lumi mappings.
        logging.info('Adding input lumi Info...')
        temp = []
        temp = [{'LumiSectionNumber': x['LumiSectionNumber'], 'RunNumber': x['RunNumber'], 'fileId': fileId} for x in lumiSections]

        if len(temp) != 0:
           insertInputLumiInfo = self.factory(classname = "File.InsertInputLumiInfo")
           rowcount = insertInputLumiInfo.execute(temp, conn = self.connection, trans = self.transaction)

        return  #// addLumiInfo	   
