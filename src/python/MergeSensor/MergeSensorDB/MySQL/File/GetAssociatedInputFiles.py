#!/usr/bin/env python

"""
        __GetAssociatedInputFiles__

       
          
"""


from MergeSensor.MergeSensorDB.MySQL.Base import MySQLBase
from MergeSensor.MergeSensorError import MergeSensorDBError

class GetAssociatedInputFiles(MySQLBase):
      """
        __GetAssociatedInputFiles__
      

      """
      def execute (self, datasetId, fileId,  conn = None, trans = False):
          """
          _execute_
          """


          # get associated input files
          self.sqlCommand = """
                     SELECT name,
                            block
                       FROM merge_inputfile
                      WHERE dataset='""" + str(datasetId) + """'
                        AND mergedfile='""" + str(fileId) + """'
                     """
	  try:
	  		                    
             result = self.dbi.processData(str(self.sqlCommand), conn = conn, transaction = trans)
	     
	  except Exception, msg:
             raise MergeSensorDBError, str(msg)     
	     
          rows = self.format(result, dictionary= True)
	  
      
          return rows #//END

 
