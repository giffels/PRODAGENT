"""
_MergeSensorDB_

This module implements the database administration functions required
by the MergeSensor component.

"""

__revision__ = "$Id: MergeSensorDB.py,v 1.25 2007/09/27 20:12:41 evansde Exp $"
__version__ = "$Revision: 1.25 $"
__author__ = "Carlos.Kavka@ts.infn.it"

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

        # force open connection
        self.conn = self.connect()
        
        # current transaction
        self.transaction = []

    ##########################################################################
    # get an open connection to the database
    ##########################################################################
                                                                                
    def connect(self): 
        """
        __connect__
                                                                                
        return a DB connection

        Arguments:
        
          none
          
        Return:
            
          none
 
        """
        
        # create a new one    
        conn = connect(False)
                
        # set transaction properties
        cursor = conn.cursor()
        cursor.execute(\
             "SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
        cursor.execute("SET AUTOCOMMIT=0")
        cursor.close()
            
        # return connection handler
        return conn
        
    ##########################################################################
    # commit method 
    ##########################################################################

    def commit(self):
        """
        __commit__
        
        The operation commit closes the current transaction,
        making all operations to take place as a single atomic
        operation.

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
            self.conn = self.connect()
            self.redo()

            # try to commit
            self.conn.commit()

        # erase redo list
        self.transaction = []

    ##########################################################################
    # start transaction
    ##########################################################################

    def startTransaction(self):
        """
        __startTransaciton__
        
        Start a transaction, performing an implicit commit if
        necessary.

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
                self.conn = self.connect()
                self.redo()

                # try to commit
                self.conn.commit()

        # erase redo list
        self.transaction = []

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

            # refresh connection
            self.conn = self.connect()
            
        # erase redo list
        self.transaction = []


    ##########################################################################
    # redo method
    ##########################################################################
                                                                                
    def redo(self):
        """
        __redo__
        
        Tries to redo all operations pending (uncomitted)
        performed during an interrupted transaction.

        Arguments:
        
          none
          
        Return:
            
          none

        """

        # get cursor
        try:
            cursor = self.conn.cursor()

        except MySQLdb.Error:

            # lost connection with database, reopen it
            self.conn = self.connect()
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
            cursor = self.conn.cursor()
            
        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect()
            cursor = self.conn.cursor()
            
        # for all databases (order is important due to foreign keys)
        for dbname in ["merge_workflow",
                       "merge_inputfile",
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
                self.conn = self.connect()
                self.redo()
                cursor = self.conn.cursor()

                # retry
                cursor.execute(sqlCommand)
                self.transaction.append(sqlCommand)

        # close cursor
        cursor.close()

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
            cursor = self.conn.cursor()

        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect()
            self.redo()
            cursor = self.conn.cursor()
            
        # select open datasets
        sqlCommand = """
                     SELECT prim, processed, tier
                       FROM merge_dataset
                       WHERE status="open"
                     """
                       
        # execute command
        try:
            cursor.execute(sqlCommand)
            
        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect()
            self.redo()
            cursor = self.conn.cursor()

            # retry
            cursor.execute(sqlCommand)

        # process results
        rows = cursor.rowcount
        
        # return empty list if no watched datasets
        if rows == 0:
            
            # close cursor
            cursor.close()

            # emtpy list
            return []
        
        # build list
        rows = cursor.fetchall()
        datasetList = ["/%s/%s/%s" % elem for elem in rows]
        
        # close cursor
        cursor.close()

        # return it
        return datasetList
        
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
        # get cursor
        try:
            cursor = self.conn.cursor()
                                                                                
        except MySQLdb.Error:
                                                                                
            # if it does not work, we lost connection to database.
            self.conn = self.connect()
            self.redo()
            cursor = self.conn.cursor()
                                                                                
        # select open datasets
        sqlCommand = """
                     SELECT prim, processed, tier
                       FROM merge_dataset
                       WHERE status="open" 
                       AND workflow='""" + workflowName + """'
                     """
        # execute command
        try:
            cursor.execute(sqlCommand)
                                                                                
        except MySQLdb.Error:
                                                                                
            # if it does not work, we lost connection to database.
            self.conn = self.connect()
            self.redo()
            cursor = self.conn.cursor()
                                                                                
            # retry
            cursor.execute(sqlCommand)
                                                                                
        # process results
        rows = cursor.rowcount
        # return empty list if no watched datasets
        if rows == 0:
                                                                                
            # close cursor
            cursor.close()
                                                                                
            # emtpy list
            return []
                                                                                
        # build list
        rows = cursor.fetchall()
        datasetList = ["/%s/%s/%s" % elem for elem in rows]
                                                                                
        # close cursor
        cursor.close()
                                                                                
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
        (prim, processed, tier) = Dataset.getNameComponents(datasetName)
        
        # get dictionary based cursor
        try:
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect()
            self.redo()
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)
            
        # get information
        sqlCommand = """
                     SELECT prim as primaryDataset,
                            tier as dataTier,
                            processed as processedDataset,
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
            self.conn = self.connect()
            self.redo()
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

            # retry
            cursor.execute(sqlCommand)

        # process results
        rows = cursor.rowcount
        
        # dataset not registered
        if rows == 0:
            
            # close cursor
            cursor.close()

            # generate exception
            raise DatasetNotInDatabase, \
                   'Dataset %s is not registered in database' % datasetName    

        # store information        
        datasetInfo = cursor.fetchall()[0]

        # add extra fields
        datasetInfo['name'] = datasetName

        # close cursor
        cursor.close()

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
        (prim, processed, tier) = Dataset.getNameComponents(datasetName)
        
        # get cursor
        try:
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect()
            self.redo()
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
            self.conn = self.connect()
            self.redo()
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

            # retry
            cursor.execute(sqlCommand)

        # process results
        rows = cursor.rowcount
        
        # it does not exist!
        if rows == 0:

            # close cursor
            cursor.close()

            # nothing
            return None
        
        # get id, status
        dataset = cursor.fetchone()
        
        # check for closed status
        if dataset['status'] == 'closed':
            return None
            
        # close cursor
        cursor.close()

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
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect()
            self.redo()
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
            self.conn = self.connect()
            self.redo()
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

            # retry
            cursor.execute(sqlCommand)

        # process results
        rows = cursor.rowcount
        
        # return empty list
        if rows == 0:
            
            # close cursor
            cursor.close()

            # empty set
            return []
        
        # build list
        rows = cursor.fetchall()
        
        # close cursor
        cursor.close()

        # return it
        return rows

    def getDatasetFileMap(self, datasetId):
        """
        _getDatasetFileMap_

        Retrieve a mapping of input LFN to output LFN for completed
        merges.

        """

        # get cursor
        try:
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect()
            self.redo()
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)
        
        sqlStr = \
        """
        SELECT MI.name, MO.lfn FROM merge_inputfile MI
          JOIN merge_outputfile MO  ON MI.mergedfile = MO.id
            JOIN  merge_dataset DS  ON DS.id = MO.dataset
              WHERE DS.id = %s;
        """  % datasetId
        
        
        # execute command
        try:
            cursor.execute(sqlStr)
            
        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect()
            self.redo()
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

            # retry
            cursor.execute(sqlStr)

        # process results
        rows = cursor.rowcount
        
        # return empty list
        if rows == 0:
            
            # close cursor
            cursor.close()

            # empty set
            return {}
        
        # build list
        rows = cursor.fetchall()
        
        # close cursor
        cursor.close()

        #  //
        # // Reformat into a dictionary of input LFN: output LFN
        #//  NULL entries are converted to None
        result = {}
        [ result.__setitem__(x['name'], x['lfn']) for x in rows ]
        
        # return it
        return result


    def getFileBlocks(self, datasetId):
        """
        _getFileBlocks_

        Return a map of LFN:Block name for all unmerged LFNs
        in the dataset provided

        """
        
        sqlStr = \
        """
        select MB.name AS block, MI.name AS file from merge_inputfile MI
            JOIN merge_fileblock MB ON MB.id = MI.block
               JOIN merge_dataset DS ON DS.id = MI.dataset
                  WHERE DS.id = %s;

        """ % datasetId
        # get cursor
        try:
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect()
            self.redo()
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)
        
                
        # execute command
        try:
            cursor.execute(sqlStr)
            
        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect()
            self.redo()
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

            # retry
            cursor.execute(sqlStr)

        # process results
        rows = cursor.rowcount
        
        # return empty list
        if rows == 0:
            
            # close cursor
            cursor.close()

            # empty set
            return {}
        
        # build list
        rows = cursor.fetchall()
        
        # close cursor
        cursor.close()

        
        result = {}
        [ result.__setitem__(x['file'], x['block']) for x in rows ]
        
        # return it
        return result
    
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
            cursor = self.conn.cursor()

        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect()
            self.redo()
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
            self.conn = self.connect()
            self.redo()
            cursor = self.conn.cursor()

            # retry
            cursor.execute(sqlCommand)

        # process results
        rows = cursor.rowcount
        
        # return empty list
        if rows == 0:
            
            # close cursor
            cursor.close()

            # empty set
            return []
        
        # build list
        mergeJobs = cursor.fetchall()
        
        # remove extra level in lists
        mergeJobs = [job[0] for job in mergeJobs]
        
        # close cursor
        cursor.close()

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
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect()
            self.redo()
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
            self.conn = self.connect()
            self.redo()
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

            # retry
            cursor.execute(sqlCommand)

        # process results
        rows = cursor.rowcount
        
        # return empty list
        if rows == 0:
            
            # close cursor
            cursor.close()
            
            # empty set
            return []
        
        # build list
        blocks = cursor.fetchall()

        # close cursor
        cursor.close()

       # get cursor
        try:
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect()
            self.redo()
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

        # start building resulting list
        fileList = []
        
        for block in blocks:
            
            # get all unmerged files in a particular fileblock
            sqlCommand = """
                     SELECT *
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
                self.conn = self.connect()
                self.redo()
                cursor = self.conn.cursor()
   
                # retry
                cursor.execute(sqlCommand)

            # get result
            rows = cursor.fetchall()

            # append to list
            fileList.append((block['name'], rows))
            
        # close cursor
        cursor.close()

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
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect()
            self.redo()
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
            self.conn = self.connect()
            self.redo()
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

            # retry
            cursor.execute(sqlCommand)

        # process results
        rows = cursor.rowcount
        
        # return no data
        if rows == 0:
            
            # close cursor
            cursor.close()

            # nothing
            return None
        
        # get information
        rows = cursor.fetchone()
        
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
            self.conn = self.connect()
            self.redo()
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

            # retry
            cursor.execute(sqlCommand)

        # process results
        rowscount = cursor.rowcount
        
        # add to result
        if rowscount != 0:
            
            # get information
            rows = cursor.fetchall()

            # store input file information
            result['inputFiles'] = [aFile['filename'] for aFile in rows]
            result['fileBlock'] = rows[0]['blockname']
            
        else:
            
            # no information (failed job)
            result['inputFiles'] = []
            result['fileBlock'] = ''
        
        # close cursor
        cursor.close()
        
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
       
        # get parameters
        fileSize = data['FileSize']
        events = data['NumberOfEvents']

        # get run numbers
        listOfRuns = data['RunsList']
        run = str([x['RunNumber'] for x in listOfRuns])
 
        # get cursor
        try:
            cursor = self.conn.cursor()
            
        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect()
            self.redo()
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
            self.conn = self.connect()
            self.redo()
            cursor = self.conn.cursor()

            # retry
            cursor.execute(sqlCommand)

        # check for file block
        rows = cursor.rowcount
 
        # the block is new
        if rows == 0:

            blockId = 'last_insert_id()'

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
                self.conn = self.connect()
                self.redo()
                cursor = self.conn.cursor()

                # retry
                cursor.execute(sqlCommand)
                self.transaction.append(sqlCommand)
                
            # check for file block
            rows = cursor.rowcount
            
            # wrong insert?
            if rows == 0:

                # close cursor
                cursor.close()

                # generate exception
                raise MergeSensorDBError, \
                   'Insertion of file block %s failed' % fileBlock
       
        # block is not new 
        else:    

            blockId = "'" + str(cursor.fetchone()[0]) + "'"

        # hack to support databases with no run field
        # TO BE REMOVED WHEN PRODUCTION TEAMS WILL UPDATE DB SCHEMA
        if run != '[]':
            runField = ", run"
            runValue = ", '" + str(run) + "'"
        else:
            runField = ""
            runValue = ""

       # insert input file
        sqlCommand = """
                     INSERT
                       INTO merge_inputfile
                            (name, block, dataset, filesize, eventcount""" + \
                             runField + """)
                     VALUES ('""" + fileName + """', 
                             """ + str(blockId) + """,
                             '""" + str(datasetId) + """',
                             '""" + str(fileSize) + """',
                             '""" + str(events) + """'
                             """ + str(runValue) + """)
                     """
        
        # execute command
        try:
            cursor.execute(sqlCommand)
            self.transaction.append(sqlCommand)

        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect()
            self.redo()
            cursor = self.conn.cursor()

            # retry
            cursor.execute(sqlCommand)
            self.transaction.append(sqlCommand)
             
        # process results
        rows = cursor.rowcount

        # close cursor
        cursor.close()
        
        # cannot be inserted
        if rows == 0:
            raise MergeSensorDBError, \
                   'Insertion of file %s failed' % fileName

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

        try:
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect()
            self.redo()
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

        # get input file information
        sqlCommand = """
                     SELECT merge_inputfile.id as id,
                            merge_inputfile.eventcount as eventcount,
                            merge_inputfile.status as status,
                            merge_inputfile.failures as failures,
                            merge_inputfile.instance as instance,
                            merge_inputfile.mergedfile as mergedfile,
                            merge_fileblock.name as block,
                            merge_dataset.prim as prim,
                            merge_dataset.tier as tier,
                            merge_dataset.processed as processed
                       FROM merge_inputfile,
                            merge_dataset,
                            merge_fileblock
                      WHERE merge_inputfile.dataset='""" + str(datasetId) + """'
                        AND merge_inputfile.name='""" + fileName + """'
                        AND merge_inputfile.dataset=merge_dataset.id
                        AND merge_inputfile.block=merge_fileblock.id
                     """

        # execute command
        try:
            cursor.execute(sqlCommand)
            
        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect()
            self.redo()
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

            # retry
            cursor.execute(sqlCommand)

        # process results
        rows = cursor.rowcount

        # file does not exist
        if rows == 0:

            # close cursor
            cursor.close()
            
            # nothing
            return None

        # get information
        row = cursor.fetchone()

        # process it
        row['dataset'] = '/' + row['prim'] + '/' + row['tier'] + '/' + \
                         row['processed']
        del row['prim']
        del row['tier']
        del row['processed']

        # close cursor
        cursor.close()

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
    
        # get cursor
        try:
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)
            
        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect()
            self.redo()
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)
    
         # get input file information
        sqlCommand = """
                     SELECT status, failures
                       FROM merge_inputfile
                      WHERE dataset='""" + str(datasetId) + """'
                        AND name='""" + fileName + """'
                     """

        # execute command
        try:
            cursor.execute(sqlCommand)
            
        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect()
            self.redo()
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

            # retry
            cursor.execute(sqlCommand)

        # process results
        rows = cursor.rowcount

        # file does not exist
        if rows == 0:
            
            # close cursor
            cursor.close()

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
        row = cursor.fetchone()
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
        sqlCommand = """
                     UPDATE merge_inputfile
                        SET status='""" + status + "'" + \
                          mergedFileUpdate + \
                          instanceUpdate + \
                          failuresUpdate + """
                      WHERE dataset='""" + str(datasetId) + """'
                        AND name='""" + fileName + """'
                     """
        
        # execute command
        try:

            cursor.execute(sqlCommand)
            self.transaction.append(sqlCommand)
            
        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect()
            self.redo()
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

            # retry
            cursor.execute(sqlCommand)
            self.transaction.append(sqlCommand)

        # close cursor
        cursor.close()

        # return status
        return status

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

        # get cursor
        try:
            cursor = self.conn.cursor()

        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect()
            self.redo()
            cursor = self.conn.cursor()

        # insert dataset information
        sqlCommand = """
                     UPDATE merge_outputfile
                        SET status='""" + status + "'" + \
                        failuresUpdate + lfnUpdate + """
                      WHERE dataset='""" + str(datasetId) + """'
                        AND """ + checkCondition

        # execute command
        try:
            cursor.execute(sqlCommand)
            self.transaction.append(sqlCommand)
            
        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect()
            self.redo()
            cursor = self.conn.cursor()

            # retry
            cursor.execute(sqlCommand)
            self.transaction.append(sqlCommand)

        # close cursor
        cursor.close()

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
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect()
            self.redo()
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
            self.conn = self.connect()
            self.redo()
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

            # retry
            cursor.execute(sqlCommand)

        # process results
        rows = cursor.rowcount
        
        # file does not exist
        if rows == 0:
            
            # close cursor
            cursor.close()

            # generate exception
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
            self.conn = self.connect()
            self.redo()
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

            # retry
            cursor.execute(sqlCommand)
            self.transaction.append(sqlCommand)

        # close cursor
        cursor.close()

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

        # get cursor
        try:
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect()
            self.redo()
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

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
            self.conn = self.connect()
            self.redo()
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

            # retry
            cursor.execute(sqlCommand)
            self.transaction.append(sqlCommand)

        # check result
        rows = cursor.rowcount

        # wrong insert?
        if rows == 0:

            # close cursor
            cursor.close()

            # generate exception
            raise MergeSensorDBError, \
               'Insertion of outputfile %s failed' % fileName

        # update input file status
        for aFile in fileList:

            # update input file information
            sqlCommand = """
                     UPDATE merge_inputfile
                        SET status='undermerge',
                            mergedfile=last_insert_id()
                      WHERE dataset='""" + str(datasetId) + """'
                        AND name='""" + aFile + """'
                     """

            # execute command
            try:
                cursor.execute(sqlCommand)
                self.transaction.append(sqlCommand)

            except MySQLdb.Error:

                # if it does not work, we lost connection to database.
                self.conn = self.connect()
                self.redo()
                cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

                # retry
                cursor.execute(sqlCommand)
                self.transaction.append(sqlCommand)

            # process results
            rows = cursor.rowcount

            # cannot be updated
            if rows == 0:

                # generate exception
                cursor.close()
                raise MergeSensorDBError, \
                       'Update operation on file %s failed' % aFile
    
        # close cursor
        cursor.close()

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

        # get cursor
        try:
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect()
            self.redo()
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

        # get output file information
        sqlCommand = """
                     SELECT *
                       FROM merge_outputfile
                      WHERE mergeJob='""" + jobId + """'
                        AND dataset='""" + str(datasetId) + """'
                     """

        # execute command
        try:
            cursor.execute(sqlCommand)

        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect()
            self.redo()
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

            # retry
            cursor.execute(sqlCommand)

        # check for job
        rows = cursor.rowcount

        # the job is not there!
        if rows == 0:

            # generate exception
            raise MergeSensorDBError, \
               'Merge job %s is not in database, cannot resubmit' % jobId

        # get file name information
        row = cursor.fetchone()

        mergeJobId = row['id']
        name = row['name']
        instance = row['instance']
        
        # define the new file name
        indexHyphen = name.find('_')
        if indexHyphen != -1:
            name = name[:indexHyphen]
        name = name + '_' + str(instance)

        # insert it
        sqlCommand = """
                 UPDATE merge_outputfile
                    SET name='""" + str(name) + """',
                        instance=instance+1,
                        status='undermerge',
                        mergejob='""" + str(newJobId) + """'
                  WHERE id='""" + str(mergeJobId) +"""'
                 """

        # execute command
        try:
            cursor.execute(sqlCommand)
            self.transaction.append(sqlCommand)

        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect()
            self.redo()
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

            # retry
            cursor.execute(sqlCommand)
            self.transaction.append(sqlCommand)

        # check result
        rows = cursor.rowcount

        # wrong update?
        if rows == 0:

            # close cursor
            cursor.close()

            # generate exception
            raise MergeSensorDBError, \
               'Update of resubmitted job %s failed' % jobId

        # update input file information
        sqlCommand = """
                 UPDATE merge_inputfile
                    SET status='undermerge'
                  WHERE dataset='""" + str(datasetId) + """'
                    AND mergedfile='""" + str(mergeJobId) + """'
                     """

        # execute command
        try:
            cursor.execute(sqlCommand)
            self.transaction.append(sqlCommand)

        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect()
            self.redo()
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

            # retry
            cursor.execute(sqlCommand)
            self.transaction.append(sqlCommand)

        # close cursor
        cursor.close()

        return name

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
        
        # get cursor
        try:
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)
            
        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect()
            self.redo()
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)
            
        # get output file information
        sqlCommand = """
                     SELECT id,
                            name,
                            mergejob
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
            self.conn = self.connect()
            self.redo()
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

            # retry
            cursor.execute(sqlCommand)

        # check for file block
        rows = cursor.rowcount
        
        # any job?
        if rows == 0:
            
            # close cursor
            cursor.close()

            # no
            return None
        
        # get file information
        row = cursor.fetchone()
        
        fileId = row['id']
        fileName = row['name']
        jobName = row['mergejob']
        
        # get associated input files
        sqlCommand = """
                     SELECT name,
                            block
                       FROM merge_inputfile
                      WHERE dataset='""" + str(datasetId) + """'
                        AND mergedfile='""" + str(fileId) + """'
                     """
        
        # execute command
        try:
            cursor.execute(sqlCommand)
            
        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect()
            self.redo()
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

            # retry
            cursor.execute(sqlCommand)

        # check for file block
        rows = cursor.rowcount
        
        # any file?
        if rows == 0:
            
            # close cursor
            cursor.close()

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
            self.conn = self.connect()
            self.redo()
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

            # retry
            cursor.execute(sqlCommand)

        # check for file block
        rows = cursor.rowcount

        # nothing, mmm, something wrong, just ignore
        if rows == 0:
            
            # close cursor
            cursor.close()
            
            # nothing
            return None
        
        # get block name
        row = cursor.fetchone()
        blockName =  row['name']
        
        # close cursor
        cursor.close()

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

        # get cursor
        try:
            cursor = self.conn.cursor()

        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect()
            self.redo()
            cursor = self.conn.cursor()
            
        # if it has failed, check input files. Should be all invalid.
        if jobInfo['status'] == 'failed':

            # get associated input file information
            sqlCommand = """
                         SELECT merge_inputfile.id
                           FROM merge_inputfile, merge_outputfile
                          WHERE merge_inputfile.mergedfile=""" + \
                                                str(jobInfo['fileId']) + """
                            AND merge_inputfile.status!='invalid'
                         """

            # execute command
            try:
                cursor.execute(sqlCommand)
                
            except MySQLdb.Error:

                # if it does not work, we lost connection to database.
                self.conn = self.connect()
                self.redo()
                cursor = self.conn.cursor()

                # retry
                cursor.execute(sqlCommand)

            # process results
            rows = cursor.rowcount

            # no files should be in a valid state
            if rows != 0:

                # close cursor
                cursor.close()

                # generate exception
                raise MergeSensorDBError, \
                   "Cannot resubmit failed job %s, files not yet invalidated." \
                   % jobName

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
            self.conn = self.connect()
            self.redo()
            cursor = self.conn.cursor()

            # retry
            cursor.execute(sqlCommand)
            self.transaction.append(sqlCommand)
            
        # process results
        rows = cursor.rowcount
        
        # close cursor
        cursor.close()

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
        (prim, processed, tier) = Dataset.getNameComponents(datasetName)
        
        # build sequence update string
        if sequenceNumber is not None:
            sequenceString = ", sequence=" + str(sequenceNumber)
        else:
            sequenceString = ""
            
        # get cursor
        try:
            cursor = self.conn.cursor()

        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect()
            self.redo()
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
            self.conn = self.connect()
            self.redo()
            cursor = self.conn.cursor()

            # retry
            cursor.execute(sqlCommand)
            self.transaction.append(sqlCommand)
            
        # close cursor
        cursor.close()

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
            cursor = self.conn.cursor()
            
        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect()
            self.redo()
            cursor = self.conn.cursor()
           
        # insert dataset information
        sqlCommand = """
                     INSERT
                       INTO merge_dataset
                            (prim,tier,processed,
                             psethash,started,updated,version,workflow,
                             mergedlfnbase,category,timestamp,sequence)
                      VALUES ('""" + data['primaryDataset'] + """',
                              '""" + data['dataTier'] + """',
                              '""" + data['processedDataset'] + """',
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
            self.conn = self.connect()
            self.redo()
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

        # close cursor
        cursor.close()

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
        (prim, processed, tier) = Dataset.getNameComponents(datasetName)
        
        # get dictionary based cursor
        try:
            cursor = self.conn.cursor()

        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect()
            self.redo()
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
            self.conn = self.connect()
            self.redo()
            cursor = self.conn.cursor()

            # retry
            cursor.execute(sqlCommand)
            self.transaction.append(sqlCommand)
            
        # process results
        rows = cursor.rowcount
        
        # close cursor
        cursor.close()

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
        (prim, processed, tier) = Dataset.getNameComponents(datasetName)
        
        # get cursor
        try:
            cursor = self.conn.cursor()

        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect()
            self.redo()
            cursor = self.conn.cursor()
        
        # remove dataset + all input files associated to the
        # dataset + all output files (merge jobs) associated
        # to the dataset + all workflows
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
            self.conn = self.connect()
            self.redo()
            cursor = self.conn.cursor()

            # retry
            cursor.execute(sqlCommand)
            self.transaction.append(sqlCommand)
            
        # process results
        rows = cursor.rowcount
        
        # dataset not registered 
        if rows == 0:

            # close cursor
            cursor.close()

            # generate exception
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
            self.conn = self.connect()
            self.redo()
            cursor = self.conn.cursor()

            # retry
            cursor.execute(sqlCommand)
            self.transaction.append(sqlCommand)
        
        # close cursor
        cursor.close()

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
        
        # get cursor
        try:
            cursor = self.conn.cursor()
            
        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect()
            self.redo()
            cursor = self.conn.cursor()
           
        sqlCommand = """
                     SELECT id
                       FROM merge_workflow
                      WHERE name='""" + workflow + """'
                        AND dataset='""" + str(datasetId) + """'
                     """
  
        # execute command
        try:
            cursor.execute(sqlCommand)
            
        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect()
            self.redo()
            cursor = self.conn.cursor()

            # retry
            cursor.execute(sqlCommand)

        # check for number of workflows
        rows = cursor.rowcount
        
        # the workflow is not new
        if rows != 0:
            
            # close cursor and return false
            cursor.close()
            return False
        
        # insert the workflow
        sqlCommand = """
                     INSERT 
                       INTO merge_workflow
                            (name, dataset)
                     VALUES ('""" + workflow + """',
                             '""" + str(datasetId) + """')
                     """
        # execute command
        try:
            cursor.execute(sqlCommand)
            self.transaction.append(sqlCommand)
            
        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect()
            self.redo()
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

        # close cursor
        cursor.close()

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

       # get cursor
        try:
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect()
            self.redo()
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
            self.conn = self.connect()
            self.redo()
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
                self.transaction.append(sqlCommand)
                
            except MySQLdb.Error:

                # if it does not work, we lost connection to database.
                self.conn = self.connect()
                self.redo()
                cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

                # retry
                cursor.execute(sqlCommand)
                self.transaction.append(sqlCommand)

            # check for insertion status
            rows = cursor.rowcount

            # problems
            if rows == 0:        
                
                # close cursor
                cursor.close()

                # generate exception
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
                self.conn = self.connect()
                self.redo()
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

        # close cursor
        cursor.close()

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
        
        # get cursor
        try:
            cursor = self.conn.cursor()
            
        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect()
            self.redo()
            cursor = self.conn.cursor()

        # get information
        sqlCommand = """
                       UPDATE merge_control
                          SET """ + updates

        # execute command
        try:
            cursor.execute(sqlCommand)
            self.transaction.append(sqlCommand)
            
        except MySQLdb.Error:

            # if it does not work, we lost connection to database.
            self.conn = self.connect()
            self.redo()
            cursor = self.conn.cursor()

            # retry
            cursor.execute(sqlCommand)
            self.transaction.append(sqlCommand)

        # close cursor
        cursor.close()
            
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
            self.conn.close()
        except MySQLdb.Error:
            pass

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

    ##########################################################################
    # get version information
    ##########################################################################

    @classmethod
    def getSchema(cls):
        """
        _getSchema_
        
        return schema information
        
        Creates its own new connection. Intended to be used
        from outside PA in order to get information on the
        current database schema.
        """
        
       # get cursor
        try:

            conn = connect(False)
            cursor = conn.cursor()

        except MySQLdb.Error, msg:

            return "Cannot connect to DB: " + str(msg)

        # get schema information
        sqlCommand = """
                    SELECT *
                      FROM information_schema.columns
                     WHERE table_name like 'merge_%'
                  ORDER BY table_name;
                 """
                       
        # execute command
        try:
            cursor.execute(sqlCommand)
            
        except MySQLdb.Error, msg:

            return "Cannot execute query: " + str(msg)

        # get all rows
        rows = cursor.fetchall()
        
        # close cursor
        cursor.close()

        # close connection
        conn.close()
        
        # return it
        return rows

