#!/usr/bin/env python

"""
        __UpdateJobStatus__

"""


from MergeSensor.MergeSensorDB.MySQL.Base import MySQLBase
from MergeSensor.MergeSensorError import MergeSensorDBError

class UpdateJobStatus(MySQLBase):
      """
        __UpdateJobStatus__

      """
      def execute (self, datasetId, aFile, conn = None, trans = False):
          """
          _execute_
          """

          # update input file information
          self.sqlCommand = """
                     UPDATE merge_inputfile
                        SET status='undermerge',
                            mergedfile=last_insert_id()
                      WHERE dataset='""" + str(datasetId) + """'
                        AND name='""" + aFile + """'
                     """

          result = None
	  try:
	  		                    
             result = self.dbi.processData(self.sqlCommand, conn = conn, transaction = trans)
	     
          except Exception, ex:
      
              raise MergeSensorDBError, str(ex)
	  
        

          rows = result[0].cursor.rowcount
	 

         
          return rows #//END

 
