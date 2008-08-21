#!/usr/bin/env python

"""
        __GetBlockName__

          
"""


from MergeSensor.MergeSensorDB.MySQL.Base import MySQLBase
from MergeSensor.MergeSensorError import MergeSensorDBError

class GetBlockName(MySQLBase):
      """
       __GetBlockName__

      """
      def execute (self, fileBlock,  conn = None, trans = False):
          """
          _execute_
          """


          # get file block name
          self.sqlCommand = """
                     SELECT name
                       FROM merge_fileblock
                      WHERE id='""" + str(fileBlock) + """'
                     """
          result = None
			
	  try:
	  		                    
             result = self.dbi.processData(self.sqlCommand, conn = conn, transaction = trans)
	     
	  except Exception, msg:
             raise MergeSensorDBError, str(msg)   
	  
	     
          row = self.formatOne (result, dictionary= True)
          
    
   
	   
          return row #//END

 
