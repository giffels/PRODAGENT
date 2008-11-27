#!/usr/bin/env python

"""
        __GetInputFileStatus__
        
       
          
"""

import os
from MergeSensor.MergeSensorDB.MySQL.Base import MySQLBase
from MergeSensor.MergeSensorError import MergeSensorDBError

class GetInputFileStatus(MySQLBase):
      """
        __GetInputFileStatus__
        

      """
      def execute (self,  datasetId, fileName,  conn = None, trans = False):
          """
          _execute_
          """

	  
	  # get input file information
          self.sqlCommand = """
                     SELECT id, status
                       FROM merge_inputfile
                      WHERE dataset='""" + str(datasetId) + """'
                        AND guid='""" + os.path.basename(fileName) + """'
                     """
        
          result = None			
	  try:
	  		                    
             result = self.dbi.processData(self.sqlCommand, conn = conn, transaction = trans)
	     
	  except Exception, msg:

             raise MergeSensorDBError, str(msg)   
	  
		 
	
		      
          return result[0].cursor.rowcount  #//END

 
