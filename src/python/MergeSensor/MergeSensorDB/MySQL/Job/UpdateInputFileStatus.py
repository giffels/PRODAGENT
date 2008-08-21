#!/usr/bin/env python

"""
        __UpdateInputFileStatus__
        
                 
          
          
"""


from MergeSensor.MergeSensorDB.MySQL.Base import MySQLBase
from MergeSensor.MergeSensorError import MergeSensorDBError

class UpdateInputFileStatus(MySQLBase):
      """
        __UpdateInputFileStatus__
        
          
      """
      def execute (self, datasetId, mergeJobId,  conn = None, trans = False):
          """
          _execute_
          """

          self.sqlCommand = """
                 UPDATE merge_inputfile
                    SET status='undermerge'
                  WHERE dataset='""" + str(datasetId) + """'
                    AND mergedfile='""" + str(mergeJobId) + """'
                     """

			
	  try:
	  		                    
             result = self.dbi.processData(self.sqlCommand, conn = conn, transaction = trans)
	     
	  except Exception, msg:

             # duplicate or wrong data
             raise MergeSensorDBError, msg   
	  
          rowcount = result[0].cursor.rowcount
	   
          return rowcount  #//END

 
