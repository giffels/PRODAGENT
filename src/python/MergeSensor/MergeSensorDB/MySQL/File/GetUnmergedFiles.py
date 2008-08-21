#!/usr/bin/env python

"""
        __GetUnmergedFiles__

          
"""


from MergeSensor.MergeSensorDB.MySQL.Base import MySQLBase
from MergeSensor.MergeSensorError import MergeSensorDBError

class GetUnmergedFiles(MySQLBase):
      """
        __GetUnmergedFiles__

      """
      def execute (self, datasetId, block,  conn = None, trans = False):
          """
          __execute__
	  
	  """


            
          # get all unmerged files in a particular fileblock
          self.sqlCommand = """
                     SELECT *
                       FROM merge_inputfile
                      WHERE dataset='""" + str(datasetId) + """'
                        AND block='""" + str(block['block'])  + """'
                        AND status='unmerged'
                   ORDER BY filesize
                     """
	  try:
	  		                    
             result = self.dbi.processData(self.sqlCommand, conn = conn, transaction = trans)
	     
	  except Exception, msg:

             # duplicate or wrong data
             raise MergeSensorDBError, msg   
	  
	     
          rows = self.format (result, dictionary= True)
     
	   
          return rows #//END

 
