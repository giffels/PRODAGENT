#!/usr/bin/env python

"""
        __UpdateInputDatasetInfo__
        
          insert dataset information

        Arguments:
        

          
        Return:
            
          
          
"""

import os
from MergeSensor.MergeSensorDB.MySQL.Base import MySQLBase
from MergeSensor.MergeSensorError import MergeSensorDBError

class UpdateInputDatasetInfo(MySQLBase):
      """
        __UpdateInputFile__
        
        insert dataset information
          
        Return:
            
          
      """
      def execute (self, status, mergedFileUpdate, instanceUpdate, failuresUpdate, datasetId, fileName,  conn = None, trans = False):
          """
          _execute_
          """


          # insert dataset information
          self.sqlCommand = """
                     UPDATE merge_inputfile
                        SET status='""" + status + "'" + \
                          mergedFileUpdate + \
                          instanceUpdate + \
                          failuresUpdate + """
                      WHERE dataset='""" + str(datasetId) + """'
                        AND guid='""" + os.path.basename(fileName) + """'
                     """
			
	  try:
	  		                    
             result = self.dbi.processData(str(self.sqlCommand), conn = conn, transaction = trans)
	     
	  except Exception, msg:

             # duplicate or wrong data
             raise MergeSensorDBError, msg   
	  

	   
          return  result[0].cursor.rowcount  #//END

 
