"""
_MergeSensorDB_

This module implements the database administration functions required
by the MergeSensor component.

"""

__revision__ = "$Id$"
__version__ = "$Revision$"
__author__ = "Carlos.Kavka@ts.infn.it"

import time
import MySQLdb

from ProdAgentDB.Connect import connect
from MergeSensor.Dataset import Dataset
from MergeSensor.MergeSensorError import MergeSensorDBError, \
                                         DatasetNotInDatabase

##############################################################################
# MergeSensorDB class
##############################################################################

class MergeSensorDB:
    """
    _MergeSensorDB_
    
    Merge Sensor database administration instance

    """
    
    ##########################################################################
    # MergeSensorDB class initialization
    ##########################################################################

    def __init__(self):
        """
        __connect__
                                                                                
        Initialize a MergeSensorDB instance.
        
        Arguments:
        
          none
          
        Return:
            
          none
 
        """

        # parameters
        self.refreshPeriod = 60 * 60 * 12 # 12 hours connections

        # force open connection
        self.connectionTime = 0
        self.conn = self.connect(invalidate = True)

        # database connection
        self.database = None
        
        # current transaction
        self.transaction = []

    ##########################################################################
    # get an open connection to the database
    ##########################################################################
                                                                                
    def connect(self, invalidate = False): 
        """
        __connect__
                                                                                
        return a DB connection, reusing old one if still valid. Create a new
        one if requested so or if old one expired.

        Arguments:
        
          invalidate -- used to force reconnection
          
        Return:
            
          none
 
        """

        # is it necessary to refresh the connection?
        
        if (time.time() - self.connectionTime > self.refreshPeriod 
            or invalidate):
            
            #  close current connection (if any)
            try:
                self.conn.close()

            # cannot close, just ignore and get a fresh one
            except (MySQLdb.Error, AttributeError):
                pass
            
            # create a new one    
            conn = connect(False)
            self.connectionTime = time.time()
                
            # set transaction properties
            cursor = conn.cursor()
            cursor.execute(\
                 "SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
            cursor.execute("SET AUTOCOMMIT=0")
            cursor.close()
            
            # return connection handler
            return conn
        
        # return old one
        return self.conn

    ##########################################################################
    # commit method 
    ##########################################################################

    def commit(self):
        """
        __commit__
        
        The operation commit closes the current transaction, making all
        operations to take place as a single atomic operation.

        Arguments:
        
          none
          
        Return:
            
          none

        """

        # commit
        try:
            self.conn.commit()
        except MySQLdb.Error:

            # lost connection with database, reopen it
            self.conn = self.connect(invalidate = True)

            # redo operations in interrupted transaction
            self.redo()

            # try to commit
            self.conn.commit()

        # erase redo list
        self.transaction = []

        # refresh connection
        self.conn = self.connect()

    ##########################################################################
    # start transaction
    ##########################################################################

    def startTransaction(self):
        """
        __startTransaciton__
        
        Start a transaction, performing an implicit commit if necessary.

        Arguments:
        
          none
          
        Return:
            
          none

        """

        if self.transaction != []:

            # commit
            try:
                self.conn.commit()
            except MySQLdb.Error:

                # lost connection with database, reopen it
                self.conn = self.connect(invalidate = True)

                # redo operations in interrupted transaction
                self.redo()

                # try to commit
                self.conn.commit()

        # erase redo list
        self.transaction = []

        # refresh connection
        self.conn = self.connect()

    ##########################################################################
    # rollback method 
    ##########################################################################

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

        # roll back
        try:
            self.conn.rollback()
        except MySQLdb.Error:
            # lost connection con database, just get a new connection
            # the effect of rollback is then automatic

            pass

        # erase redo list
        self.transaction = []

        # refresh connection
        self.conn = self.connect()

    ##########################################################################
    # redo method
    ##########################################################################
                                                                                
    def redo(self):
        """
        __redo__
        
        Tries to redo all operations pending (uncomitted) performed during
        an interrupted transaction.

        Only called with a fresh valid connection

        Arguments:
        
          none
          
        Return:
            
          none

        """

        # get cursor
        cursor = self.conn.cursor()

        # perform all operations in current newly created transaction
        for sqlOperation in self.transaction:
            cursor.execute(sqlOperation)

        # close cursor
        cursor.close()

    ##########################################################################
    # wipe out DB contents
    ##########################################################################
                                                                                
    def eraseDB(self): 
        """
        __eraseDB__
        
        Erase all contents of MergeSensor DB in a single transaction (all 
        tables are deleted or none at all)
        
        Arguments:
        
          none
          
        Return:
            
          none

        """
        
        # start transaction
        self.startTransaction()

        # get cursor
        try:
            self.conn = self.connect()
            cursor = self.conn.cursor()
        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect(invalidate = True)
            cursor = self.conn.cursor()
            
        # for all databases (order is important due to foreign keys)
        for dbname in ["merge_inputfile",
                       "merge_fileblock",
                       "merge_outputfile",
                       "merge_dataset"]:

            # delete all contents
            sqlCommand = """
                         DELETE
                           FROM """ + dbname
                           
            # execute command
            try:

                cursor.execute(sqlCommand)
                self.transaction.append(sqlCommand)
            except MySQLdb.Error:

                # if it does not work, we lost connection to database.
                self.conn = self.connect(invalidate = True)

                # redo operations in interrupted transaction
                self.redo()
            
                # get cursor
                cursor = self.conn.cursor()

                # retry
                cursor.execute(sqlCommand)

        # commit changes
        self.commit()
            
    ##########################################################################
    # get list of datasets
    ##########################################################################
                                                                                
    def getDatasetList(self): 
        """
        __getDatasetList__
        
        Get the list of all datasets.
        
        Arguments:
        
          none
          
        Return:
            
          the list of all dataset names which are currently in open status

        """
        
        # get cursor
        try:
            self.conn = self.connect()
            cursor = self.conn.cursor()
        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect(invalidate = True)
            cursor = self.conn.cursor()
            
        # delete all contents
        sqlCommand = """
                     SELECT prim, tier, processed
                       FROM merge_dataset
                       WHERE status="open"
                     """
                       
        # execute command
        try:

            cursor.execute(sqlCommand)
        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect(invalidate = True)

            # get cursor
            cursor = self.conn.cursor()

            # retry
            cursor.execute(sqlCommand)

        # process results
        rows = cursor.rowcount
        
        # return empty list if no watched datasets
        if rows == 0:
            return []
        
        # build list
        rows = cursor.fetchall()
        datasetList = ["/%s/%s/%s" % elem for elem in rows]
        
        # return it
        return datasetList
        
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
        (prim, tier, processed) = Dataset.getNameComponents(datasetName)
        
        # get dictionary based cursor
        try:
            self.conn = self.connect()
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)
        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect(invalidate = True)
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)
            
        # get information
        sqlCommand = """
                     SELECT prim as primaryDataset,
                            tier as dataTier,
                            processed as processedDataset,
                            polltier as pollTier,
                            psethash as PSetHash,
                            status,
                            started,
                            updated as lastUpdated,
                            version,
                            workflow as workflowName,
                            mergedlfnbase as mergedLFNBase,
                            category,
                            timeStamp,
                            sequence as outSeqNumber
                       FROM merge_dataset
                       WHERE prim='""" + prim + """'
                         AND tier='""" + tier + """'
                         AND processed='""" + processed + """'
                     """
                       
        # execute command
        try:

            cursor.execute(sqlCommand)
        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect(invalidate = True)

            # get cursor
            cursor = self.conn.cursor()

            # retry
            cursor.execute(sqlCommand)

        # process results
        rows = cursor.rowcount
        
        # dataset not registered
        if rows == 0:
            raise DatasetNotInDatabase, \
                   'Dataset %s is not registered in database' % datasetName    

        # store information        
        datasetInfo = cursor.fetchall()[0]

        # add extra fields
        datasetInfo['name'] = datasetName
        datasetInfo['secondaryOutputTiers'] = \
               datasetInfo['dataTier'].split("-")[1:]

        # return it
        return datasetInfo
        
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
        (prim, tier, processed) = Dataset.getNameComponents(datasetName)
        
        # get cursor
        try:
            self.conn = self.connect()
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)
        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect(invalidate = True)
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)
            
        # get dataset id
        sqlCommand = """
                     SELECT id, status
                       FROM merge_dataset
                       WHERE prim='""" + prim + """'
                         AND tier='""" + tier + """'
                         AND processed='""" + processed + "'"
                       
        # execute command
        try:

            cursor.execute(sqlCommand)
        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect(invalidate = True)

            # get cursor
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

            # retry
            cursor.execute(sqlCommand)

        # process results
        rows = cursor.rowcount
        
        # it does not exist!
        if rows == 0:
            return None
        
        # get id, status
        dataset = cursor.fetchone()
        
        # check for closed status
        if dataset['status'] == 'closed':
            return None
            
        return dataset['id']
    
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

       # get cursor
        try:
            self.conn = self.connect()
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)
        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect(invalidate = True)
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

        # get all files associated to dataset
        sqlCommand = """
                     SELECT merge_inputfile.name as name,
                            merge_fileblock.name as blockname,
                            merge_inputfile.filesize as filesize
                       FROM merge_inputfile,
                            merge_fileblock
                      WHERE merge_inputfile.dataset='""" + str(datasetId) + """'
                        AND merge_fileblock.id=merge_inputfile.block
                     """
                       
        # execute command
        try:

            cursor.execute(sqlCommand)
        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect(invalidate = True)

            # get cursor
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

            # retry
            cursor.execute(sqlCommand)

        # process results
        rows = cursor.rowcount
        
        # return empty list
        if rows == 0:
            return ()
        
        # build list
        rows = cursor.fetchall()
        
        # return it
        return rows
    
    ##########################################################################
    # get list of mergejobs of a dataset
    ##########################################################################
            
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
        
       # get cursor
        try:
            self.conn = self.connect()
            cursor = self.conn.cursor()
        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect(invalidate = True)
            cursor = self.conn.cursor()

        # get merge jobs in dataset
        sqlCommand = """
                     SELECT mergejob
                       FROM merge_outputfile
                      WHERE dataset='""" + str(datasetId) + "'"
                       
        # execute command
        try:

            cursor.execute(sqlCommand)
        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect(invalidate = True)

            # get cursor
            cursor = self.conn.cursor()

            # retry
            cursor.execute(sqlCommand)

        # process results
        rows = cursor.rowcount
        
        # return empty list
        if rows == 0:
            return ()
        
        # build list
        mergeJobs = cursor.fetchall()
        
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

    ##########################################################################
    # get list of unmerged files of a dataset
    ##########################################################################
            
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

       # get cursor
        try:
            self.conn = self.connect()
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)
        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect(invalidate = True)
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

        # get file blocks in dataset
        sqlCommand = """
                     SELECT DISTINCT merge_inputfile.block as block,
                                     merge_fileblock.name as name
                       FROM merge_inputfile,
                            merge_fileblock
                      WHERE merge_inputfile.dataset='""" + str(datasetId) + """'
                        AND merge_inputfile.status='unmerged'
                        AND merge_inputfile.block=merge_fileblock.id
                     """
                       
        # execute command
        try:

            cursor.execute(sqlCommand)
        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect(invalidate = True)

            # get cursor
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

            # retry
            cursor.execute(sqlCommand)

        # process results
        rows = cursor.rowcount
        
        # return empty list
        if rows == 0:
            return ()
        
        # build list
        blocks = cursor.fetchall()

        # start building resulting list
        fileList = []
        
        for block in blocks:
            
            # get cursor
            try:
                self.conn = self.connect()
                cursor = self.conn.cursor()
            except MySQLdb.Error:

                # if it does not work, we lost connection to database.
                self.conn = self.connect(invalidate = True)
                cursor = self.conn.cursor()

            # get all unmerged files in a particular fileblock
            sqlCommand = """
                     SELECT name,
                            filesize
                       FROM merge_inputfile
                      WHERE dataset='""" + str(datasetId) + """'
                        AND block='""" + str(block['block'])  + """'
                        AND status='unmerged'
                   ORDER BY filesize
                     """
                       
            # execute command
            try:

                cursor.execute(sqlCommand)
            except MySQLdb.Error:

                # if it does not work, we lost connection to database.
                self.conn = self.connect(invalidate = True)

                # get cursor
                cursor = self.conn.cursor()
   
                # retry
                cursor.execute(sqlCommand)

            # get result
            rows = cursor.fetchall()

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
       
        # get cursor
        try:
            self.conn = self.connect()
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)
        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect(invalidate = True)
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

        # get main properties
        sqlCommand = """
                     SELECT merge_outputfile.name as outputfile,
                            merge_outputfile.id as fileid,
                            merge_outputfile.status as status,
                            merge_dataset.prim as prim,
                            merge_dataset.tier as tier,
                            merge_dataset.processed as processed
                       FROM merge_outputfile, merge_dataset
                      WHERE mergejob='""" + str(jobId) + """'
                        AND merge_dataset.id = merge_outputfile.dataset
                     """
                       
        # execute command
        try:

            cursor.execute(sqlCommand)
        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect(invalidate = True)

            # get cursor
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

            # retry
            cursor.execute(sqlCommand)

        # process results
        rows = cursor.rowcount
        
        # return no data
        if rows == 0:
            return None
        
        # get information
        rows = cursor.fetchone()
        
        # add information to result
        result['outputFile'] = rows['outputfile']
        result['datasetName'] = "/" + rows['prim'] + \
                                "/" + rows['tier'] + \
                                "/" + rows['processed']
        result['status'] = rows['status']
        
        # get output file id
        fileId = rows['fileid']
        
        # get now all associated input files
        sqlCommand = """
                     SELECT merge_inputfile.name as filename,
                            merge_fileblock.name as blockname
                       FROM merge_inputfile,
                            merge_fileblock
                      WHERE merge_inputfile.mergedfile='""" \
                             + str(fileId) + """'
                        AND merge_inputfile.block=merge_fileblock.id
                     """
                       
        # execute command
        try:

            cursor.execute(sqlCommand)
        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect(invalidate = True)

            # get cursor
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

            # retry
            cursor.execute(sqlCommand)

        # process results
        rows = cursor.rowcount

        # something wrong if not a single input file...
        if rows == 0:
            return None
        
        # get information
        rows = cursor.fetchall()

        # add to result
        result['inputFiles'] = [aFile['filename'] for aFile in rows]
        result['fileBlock'] = rows[0]['blockname']
        
        # return it
        return result
    
    ##########################################################################
    # add a file to a dataset
    ##########################################################################
            
    def addFile(self, datasetId, fileName, fileSize, fileBlock):
        """
        __addFile__
        
        Add a file specification (and possibly a fileblock) to
        a dataset (specified by Id)
        
        Arguments:
        
          datasetId -- the dataset id in database
          fileName -- the file to add
          fileSize -- its file size
          fileBlock -- the file block name
          
        Return:
            
          none

        """
        
        # get cursor
        try:
            self.conn = self.connect()
            cursor = self.conn.cursor()
        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect(invalidate = True)
            cursor = self.conn.cursor()
            
        # get file block
        sqlCommand = """
                     SELECT id
                       FROM merge_fileblock
                      WHERE name='""" + fileBlock + """'
                     """
                                                    
        # execute command
        try:

            cursor.execute(sqlCommand)
        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect(invalidate = True)

            # get cursor
            cursor = self.conn.cursor()

            # retry
            cursor.execute(sqlCommand)

        # check for file block
        rows = cursor.rowcount
        
        # the block is new
        if rows == 0:

            # insert it
            sqlCommand = """
                     INSERT INTO merge_fileblock
                            (name)
                     VALUES ('""" + fileBlock + """')
                     """
            # execute command
            try:

                cursor.execute(sqlCommand)
                self.transaction.append(sqlCommand)
            except MySQLdb.Error:

                # if it does not work, we lost connection to database.
                self.conn = self.connect(invalidate = True)

                # get cursor
                cursor = self.conn.cursor()

                # retry
                cursor.execute(sqlCommand)
                self.transaction.append(sqlCommand)
                
            # check for file block
            rows = cursor.rowcount
            
            # wrong insert?
            if rows == 0:
                raise MergeSensorDBError, \
                   'Insertion of file block %s failed' % fileBlock
            
            # get id
            sqlCommand = "SELECT LAST_INSERT_ID()"
            
            # execute command
            try:

                cursor.execute(sqlCommand)
            except MySQLdb.Error:

                # if it does not work, we lost connection to database.
                self.conn = self.connect(invalidate = True)

                # get cursor
                cursor = self.conn.cursor()

                # retry
                cursor.execute(sqlCommand)

        # get block id
        block = cursor.fetchone()[0]
        
        # insert input file
        sqlCommand = """
                     INSERT
                       INTO merge_inputfile
                            (name, block, dataset, filesize)
                     VALUES ('""" + fileName + """', 
                             '""" + str(block) +"""',
                             '""" + str(datasetId) + """',
                             '""" + str(fileSize) + """')
                     """
                                                                         
        # execute command
        try:

            cursor.execute(sqlCommand)
            self.transaction.append(sqlCommand)
        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect(invalidate = True)

            # get cursor
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

            # retry
            cursor.execute(sqlCommand)
            self.transaction.append(sqlCommand)
             
        # process results
        rows = cursor.rowcount
        
        # cannot be inserted
        if rows == 0:
            raise MergeSensorDBError, \
                   'Insertion of file %s failed' % fileName

    ##########################################################################
    # update input file information
    #
    # no checking is performed since nothing prevent to update it to the same
    # status
    ##########################################################################
            
    def updateInputFile(self, datasetId, fileName, status = "merged", \
                        mergedFile = None):
        """
        __updateInputFile__
        
        Update input file information, file should exist
        
        Arguments:
        
          datasetId -- the dataset id in database
          fileName -- the file name used to update
          status -- new status (if any)
          mergedFile -- the merged output file id (if any)
          
        Return:
            
          none

        """
    
        # get cursor
        try:
            self.conn = self.connect()
            cursor = self.conn.cursor()
        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect(invalidate = True)
            cursor = self.conn.cursor()
    
        # define mergedFileInformation
        if mergedFile is None:
            mergedFileUpdate = ''
        else:
            mergedFileUpdate = ", mergedfile='" + str(mergedFile) + "' "
            
        # insert dataset information
        sqlCommand = """
                     UPDATE merge_inputfile
                        SET status='""" + status + "'" + \
                        mergedFileUpdate + """
                      WHERE dataset='""" + str(datasetId) + """'
                        AND name='""" + fileName + """'
                     """
        
        # execute command
        try:

            cursor.execute(sqlCommand)
            self.transaction.append(sqlCommand)
        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect(invalidate = True)

            # get cursor
            cursor = self.conn.cursor()

            # retry
            cursor.execute(sqlCommand)
            self.transaction.append(sqlCommand)
            
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
        
       # get cursor
        try:
            self.conn = self.connect()
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)
        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect(invalidate = True)
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

        # get input file information
        sqlCommand = """
                     SELECT id, status
                       FROM merge_inputfile
                      WHERE dataset='""" + str(datasetId) + """'
                        AND name='""" + fileName + """'
                     """
                       
        # execute command
        try:

            cursor.execute(sqlCommand)
        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect(invalidate = True)

            # get cursor
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

            # retry
            cursor.execute(sqlCommand)

        # process results
        rows = cursor.rowcount
        
        # file does not exist
        if rows == 0:
            raise MergeSensorDBError, \
             'Cannot invalidate file %s, not registered in dataset %s.' \
                 % (fileName, datasetName)
            
        # mark it as invalid
        sqlCommand = """
                     UPDATE merge_inputfile
                        SET status='invalid'
                      WHERE dataset='""" + str(datasetId) + """'
                        AND name='""" + fileName + """'
                     """
                     
        # execute command
        try:

            cursor.execute(sqlCommand)
            self.transaction.append(sqlCommand)
        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect(invalidate = True)

            # get cursor
            cursor = self.conn.cursor()

            # retry
            cursor.execute(sqlCommand)
            self.transaction.append(sqlCommand)

    ##########################################################################
    # add output file
    ##########################################################################
            
    def addOutputFile(self, datasetId, fileName, jobId):
        """
        __addOutputFile__
        
        Add an output file to a file block. If it exists, increment instance
        counter.
        
        Arguments:
        
          datasetId -- the dataset id in database
          fileName -- the output merged file name
          jobId -- the job name
          
        Return:
            
          instance -- instance number of the file (0 = new)
          fileId -- id of the output file

        """
        
        # get cursor
        try:
            self.conn = self.connect()
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)
        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect(invalidate = True)
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)
            
        # get file block
        sqlCommand = """
                     SELECT *
                       FROM merge_outputfile
                      WHERE name='""" + fileName + """'
                        AND dataset='""" + str(datasetId) + """'
                     """
                                                    
        # execute command
        try:

            cursor.execute(sqlCommand)
        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect(invalidate = True)

            # get cursor
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

            # retry
            cursor.execute(sqlCommand)

        # check for file block
        rows = cursor.rowcount
        
        # the file is new, create it
        if rows == 0:

            # insert it
            sqlCommand = """
                     INSERT INTO merge_outputfile
                            (name, dataset, mergejob)
                     VALUES ('""" + fileName + """',
                            '""" + str(datasetId) + """',
                            '""" + jobId + """')
                     """
            # execute command
            try:

                cursor.execute(sqlCommand)
                self.transaction.append(sqlCommand)
            except MySQLdb.Error:

                # if it does not work, we lost connection to database.
                self.conn = self.connect(invalidate = True)

                # get cursor
                cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

                # retry
                cursor.execute(sqlCommand)
                self.transaction.append(sqlCommand)
                
            # check result
            rows = cursor.rowcount
            
            # wrong insert?
            if rows == 0:
                raise MergeSensorDBError, \
                   'Insertion of outputfile %s failed' % fileName
            
            # get id
            sqlCommand = "SELECT LAST_INSERT_ID()"
            
            # execute command
            try:

                cursor.execute(sqlCommand)
            except MySQLdb.Error:

                # if it does not work, we lost connection to database.
                self.conn = self.connect(invalidate = True)

                # get cursor
                cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

                # retry
                cursor.execute(sqlCommand)

            # get file id
            fileId = (cursor.fetchone()).values()[0]
            
            # return file id of a newly created file, 0 = first instance
            return (0, fileId)

        # get file id
        row = cursor.fetchone()
        fileId = row['id']
        instance = row['instance']
        
        # update output file informarion
        sqlCommand = """
                     UPDATE merge_outputfile
                        SET status='merged',
                            instance=instance+1,
                            mergejob='""" + jobId + """'
                      WHERE name='""" + fileName + """'
                        AND dataset='""" + str(datasetId) + """'
                     """
                                                                         
        # execute command
        try:

            cursor.execute(sqlCommand)
            self.transaction.append(sqlCommand)
        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect(invalidate = True)

            # get cursor
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

            # retry
            cursor.execute(sqlCommand)
            self.transaction.append(sqlCommand)
            
        # process results
        rows = cursor.rowcount
        
        # cannot be uupdated
        if rows == 0:
            raise MergeSensorDBError, \
                   'Update operaion on file %s failed' % fileName

        # return file id of updated file and instance number
        return (instance, fileId)
    
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
            
          the name of the output file, the lists of input files
          and the name of the output file

        """
        
        # get cursor
        try:
            self.conn = self.connect()
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)
        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect(invalidate = True)
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)
            
        # get file block
        sqlCommand = """
                     SELECT id,
                            name
                       FROM merge_outputfile
                      WHERE dataset='""" + str(datasetId) + """'
                        AND status='do_it_again'
                      LIMIT 1
                     """
        
        # execute command
        try:

            cursor.execute(sqlCommand)
        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect(invalidate = True)

            # get cursor
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

            # retry
            cursor.execute(sqlCommand)

        # check for file block
        rows = cursor.rowcount
        
        # any file?
        if rows == 0:
            
            # no
            return None
        
        # get file information
        row = cursor.fetchone()
        
        fileId = row['id']
        fileName = row['name']
        
        # get associated input files
        sqlCommand = """
                     SELECT name,
                            block
                       FROM merge_inputfile
                      WHERE dataset='""" + str(datasetId) + """'
                        AND mergedfile='""" + str(fileId) + """'
                        AND status!='invalid'
                     """
        
        # execute command
        try:

            cursor.execute(sqlCommand)
        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect(invalidate = True)

            # get cursor
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

            # retry
            cursor.execute(sqlCommand)

        # check for file block
        rows = cursor.rowcount
        
        # any file?
        if rows == 0:
            
            # no input files, should not be the case, ignore
            return None

        # get list of files and fileblock
        rows = cursor.fetchall()

        fileList = [aFile['name'] for aFile in rows]
        fileBlock = rows[0]['block']
        
        # get file block name
        sqlCommand = """
                     SELECT name
                       FROM merge_fileblock
                      WHERE id='""" + str(fileBlock) + """'
                     """
        
        # execute command
        try:

            cursor.execute(sqlCommand)
        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect(invalidate = True)

            # get cursor
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

            # retry
            cursor.execute(sqlCommand)

        # check for file block
        rows = cursor.rowcount

        # nothing, mmm, something wrong, just ignore
        if rows == 0:
            return None
        
        # get block name
        row = cursor.fetchone()
        blockName =  row['name']
        
        # pack and return everything
        return (fileList, blockName, fileName)
    
    ##########################################################################
    # redo job
    ##########################################################################
            
    def redoJob(self, jobName):
        """
        __redoJob__
        
        Flag job to be redone
        
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
               
        # get cursor
        try:
            self.conn = self.connect()
            cursor = self.conn.cursor()
        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect(invalidate = True)
            cursor = self.conn.cursor()
            
        # update output file informarion
        sqlCommand = """
                     UPDATE merge_outputfile
                        SET status='do_it_again'
                      WHERE mergejob='""" + jobName + """'
                     """
                                                                         
        # execute command
        try:

            cursor.execute(sqlCommand)
            self.transaction.append(sqlCommand)
        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect(invalidate = True)

            # get cursor
            cursor = self.conn.cursor()

            # retry
            cursor.execute(sqlCommand)
            self.transaction.append(sqlCommand)
            
        # process results
        rows = cursor.rowcount
        
        # cannot be uupdated
        if rows == 0:
            raise MergeSensorDBError, \
                   'Update operation for job %s failed' % jobName

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
        (prim, tier, processed) = Dataset.getNameComponents(datasetName)
        
        # build sequence update string
        if sequenceNumber is not None:
            sequenceString = ", sequence=" + str(sequenceNumber)
        else:
            sequenceString = ""
            
        # get cursor
        try:
            self.conn = self.connect()
            cursor = self.conn.cursor()
        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect(invalidate = True)
            cursor = self.conn.cursor()
            
        # insert dataset information
        sqlCommand = """
                     UPDATE merge_dataset
                        SET status='open',
                            updated=current_timestamp
                     """ + sequenceString + """
                      WHERE prim='""" + prim + """'
                        AND tier='""" + tier + """'
                        AND processed='""" + processed + """'
                     """
        
        # execute command
        try:

            cursor.execute(sqlCommand)
            self.transaction.append(sqlCommand)
        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect(invalidate = True)

            # get cursor
            cursor = self.conn.cursor()

            # retry
            cursor.execute(sqlCommand)
            self.transaction.append(sqlCommand)
            
    ##########################################################################
    # update dataset information
    ##########################################################################
            
    def insertDataset(self, data):
        """
        __insertDataset__
        
        insert dataset information
        
        Arguments:
        
          data -- the dataset associated dictionary
          
        Return:
            
          none

        """
        # get cursor
        try:
            self.conn = self.connect()
            cursor = self.conn.cursor()
        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect(invalidate = True)
            cursor = self.conn.cursor()
            
        # insert dataset information
        sqlCommand = """
                     INSERT
                       INTO merge_dataset
                            (prim,tier,processed,polltier,psethash,
                             started,updated,version,workflow,mergedlfnbase,
                             category,timestamp,sequence)
                      VALUES ('""" + data['primaryDataset'] + """',
                              '""" + data['dataTier'] + """',
                              '""" + data['processedDataset'] + """',
                              '""" + data['pollTier'] + """',
                              '""" + str(data['PSetHash']) + """',
                              '""" + data['started'] + """',
                              '""" + data['lastUpdated'] + """',
                              '""" + data['version'] + """',
                              '""" + data['workflowName'] + """',
                              '""" + data['mergedLFNBase'] + """',
                              '""" + data['category'] + """',
                              '""" + str(data['timeStamp']) + """',
                              '""" + str(data['outSeqNumber']) + """')
                     """
        
        # execute command
        try:

            cursor.execute(sqlCommand)
            self.transaction.append(sqlCommand)
        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect(invalidate = True)

            # get cursor
            cursor = self.conn.cursor()

            # retry
            try:
                cursor.execute(sqlCommand)
                self.transaction.append(sqlCommand)

            except MySQLdb.IntegrityError, msg:

                # duplicate or wrong data
                raise MergeSensorDBError, msg

        except MySQLdb.IntegrityError, msg:

            # duplicate or wrong data
            raise MergeSensorDBError, msg

        # check for insertion status
        rows = cursor.rowcount

        # problems
        if rows == 0:        
            raise MergeSensorDBError, \
                   'Insertion of dataset %s failed' % data['name']


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
        (prim, tier, processed) = Dataset.getNameComponents(datasetName)
        
        # get dictionary based cursor
        try:
            self.conn = self.connect()
            cursor = self.conn.cursor()
        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect(invalidate = True)
            cursor = self.conn.cursor()
            
        # get information
        sqlCommand = """
                       UPDATE merge_dataset
                          SET status='closed'
                       WHERE prim='""" + prim + """'
                         AND tier='""" + tier + """'
                         AND processed='""" + processed + """'
                     """
                       
        # execute command
        try:

            cursor.execute(sqlCommand)
            self.transaction.append(sqlCommand)
        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect(invalidate = True)

            # get cursor
            cursor = self.conn.cursor()

            # retry
            cursor.execute(sqlCommand)
            self.transaction.append(sqlCommand)
            
        # process results
        rows = cursor.rowcount
        
        # dataset not registered
        if rows == 0:
            raise MergeSensorDBError, \
                   'Cannot close dataset %s, not registered in database' \
                        % datasetName    

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
        (prim, tier, processed) = Dataset.getNameComponents(datasetName)
        
        # get cursor
        try:
            self.conn = self.connect()
            cursor = self.conn.cursor()
        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect(invalidate = True)
            cursor = self.conn.cursor()
        
        # remove dataset + all input files associated to the
        # dataset + all output files (merge jobs) associated
        # to the dataset
        sqlCommand = """
                       DELETE FROM merge_dataset
                       WHERE prim='""" + prim + """'
                         AND tier='""" + tier + """'
                         AND processed='""" + processed + """'
                     """
        
        # execute command
        try:

            cursor.execute(sqlCommand)
            self.transaction.append(sqlCommand)
        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect(invalidate = True)

            # get cursor
            cursor = self.conn.cursor()

            # retry
            cursor.execute(sqlCommand)
            self.transaction.append(sqlCommand)
            
        # process results
        rows = cursor.rowcount
        
        # dataset not registered 
        if rows == 0:
            raise MergeSensorDBError, \
                   'Cannot remove dataset %s, not registered in database' \
                        % datasetName    

        # remove now all orphan fileblocks
        sqlCommand = """
                     DELETE FROM merge_fileblock
                      WHERE NOT EXISTS
                            (SELECT NULL
                               FROM merge_inputfile
                              WHERE merge_fileblock.id=merge_inputfile.block);
                     """
                       
        # execute command
        try:

            cursor.execute(sqlCommand)
            self.transaction.append(sqlCommand)
        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect(invalidate = True)

            # get cursor
            cursor = self.conn.cursor()

            # retry
            cursor.execute(sqlCommand)
            self.transaction.append(sqlCommand)
        
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

       # get cursor
        try:
            self.conn = self.connect()
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)
        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect(invalidate = True)
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

        # get status information
        sqlCommand = """
                     SELECT *
                       FROM merge_control
                     """
                       
        # execute command
        try:

            cursor.execute(sqlCommand)
        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect(invalidate = True)

            # get cursor
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

            # retry
            cursor.execute(sqlCommand)

        # process results
        rows = cursor.rowcount
        
        # no information, insert default
        if rows == 0:
            
            sqlCommand = """
                         INSERT
                           INTO merge_control
                                ()
                         VALUES ()
                         """
        
            # execute command
            try:

                cursor.execute(sqlCommand)
            except MySQLdb.Error:

                # if it does not work, we lost connection to database.
                self.conn = self.connect(invalidate = True)

                # get cursor
                cursor = self.conn.cursor()

                # retry
                cursor.execute(sqlCommand)

            # check for insertion status
            rows = cursor.rowcount

            # problems
            if rows == 0:        
                raise MergeSensorDBError, \
                       'Insertion of MergeSensor status into database failed'

            # commit changes
            self.commit()
            
            # get default status information
            sqlCommand = """
                         SELECT *
                           FROM merge_control
                         """
                       
            # execute command
            try:

                cursor.execute(sqlCommand)
            except MySQLdb.Error:

                # if it does not work, we lost connection to database.
                self.conn = self.connect(invalidate = True)

                # get cursor
                cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

                # retry
                cursor.execute(sqlCommand)

        # build status dictionary
        rows = cursor.fetchone()
        
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
        
        # start transaction
        self.startTransaction()
        
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
        
        # get dictionary based cursor
        try:
            self.conn = self.connect()
            cursor = self.conn.cursor()
        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect(invalidate = True)
            cursor = self.conn.cursor()

        # get information
        sqlCommand = """
                       UPDATE merge_control
                          SET """ + updates

        # execute command
        try:

            cursor.execute(sqlCommand)
        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect(invalidate = True)

            # get cursor
            cursor = self.conn.cursor()

            # retry
            cursor.execute(sqlCommand)
            
        # commit changes
        self.commit()
            
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
    
        # close connection (ignoring errors if any)
        try:
            self.database.close()
        except:
            pass
