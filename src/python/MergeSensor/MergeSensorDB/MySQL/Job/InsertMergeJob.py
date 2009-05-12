#!/usr/bin/env python

"""
        __InsertMergeJob__

        Add a new job.

        Arguments:

          datasetId -- the dataset id in database
          fileName -- the output merged file name
          jobId -- the job name
    

        Return:

          none
          
"""


from MergeSensor.MergeSensorDB.MySQL.Base import MySQLBase
from MergeSensor.MergeSensorError import MergeSensorDBError

class InsertMergeJob(MySQLBase):
      """
        __InsertMergeJob__

        Add a new job.

        Arguments:

          datasetId -- the dataset id in database
          fileName -- the output merged file name
          jobId -- the job name
    

        Return:

          none
      """
      def execute (self, datasetId, fileName, jobId, conn = None, trans = False):
          """
          _execute_
          """

          # insert it
          self.sqlCommand = """
                 INSERT INTO merge_outputfile
                        (name, dataset, mergejob)
                 VALUES ('""" + fileName + """',
                         '""" + str(datasetId) + """',
                         '""" + jobId + """')
                 """

	  try:
	  		                    
             result = self.dbi.processData(self.sqlCommand, conn = conn, transaction = trans)
	     
          except Exception, ex:
      
              raise MergeSensorDBError, str(ex)
	  
        

          rowcount = result[0].rowcount
	  fileId = result[0].lastrowid
     
          return (fileId, rowcount)  #//END

 
