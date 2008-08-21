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
      def execute (self, datasetId, fileName, status,  conn = None, trans = False):
          """
          _execute_
          """

          # mark it as invalid
          self.sqlCommand = """
                     UPDATE merge_inputfile
                        SET status='"""+status+"""'   WHERE dataset='""" + str(datasetId) + """'
                        AND name='""" + fileName + """'
                     """
          result = None
			
	  try:
	  		                    
             result = self.dbi.processData(self.sqlCommand, conn = conn, transaction = trans)
	     
	  except Exception, msg:

             # duplicate or wrong data
             raise MergeSensorDBError, msg   
	  

	   
          return result[0].cursor.rowcount #//END

 
