#!/usr/bin/env python

"""
        __GetOutputFileInfo__

        get output file information
          
"""


from MergeSensor.MergeSensorDB.MySQL.Base import MySQLBase
from MergeSensor.MergeSensorError import MergeSensorDBError

class GetOutputFileInfo(MySQLBase):
      """
        __GetOutputFileInfo__
        get output file information

      """
      def execute (self, datasetId, jobId,  conn = None, trans = False):
          """
          _execute_
          """


          # get output file information
          self.sqlCommand = """
                     SELECT *
                       FROM merge_outputfile
                      WHERE mergeJob='""" + jobId + """'
                        AND dataset='""" + str(datasetId) + """'
                     """
			
	  try:
	  		                    
             result = self.dbi.processData(self.sqlCommand, conn = conn, transaction = trans)
	     
	  except Exception, msg:

             raise MergeSensorDBError, str(msg)
	  

	     
          row = self.formatOne (result, dictionary= True)

	   
          return row #//END

 
