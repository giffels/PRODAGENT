#!/usr/bin/env python

"""
        __UpdateOutputDatasetInfo__
        
          insert dataset information

        Arguments:
        

          
        Return:
            
          
          
"""


from MergeSensor.MergeSensorDB.MySQL.Base import MySQLBase
from MergeSensor.MergeSensorError import MergeSensorDBError

class UpdateOutputDatasetInfo(MySQLBase):
      """
        __UpdateInputFile__
        
        insert dataset information
          
        Return:
            
          
      """
      def execute (self, status, failuresUpdate, lfnUpdate, datasetId, checkCondition,  conn = None, trans = False):
          """
          _execute_
          """

          result = None
          try:
           #insert dataset information
           self.sqlCommand = """UPDATE merge_outputfile  SET status='""" + status + "'" +  failuresUpdate + lfnUpdate + \
                            """  WHERE dataset='""" + str(datasetId) + """'  AND """ + checkCondition

	  		                    
           result = self.dbi.processData(str(self.sqlCommand), conn = conn, transaction = trans)
	     
	  except Exception, msg:

             # duplicate or wrong data
             raise MergeSensorDBError, str(msg)   
	  

	   
          return result[0].cursor.rowcount #//END

  
