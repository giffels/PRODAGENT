#!/usr/bin/env python

"""
        __GetBlockId__

          
"""


from MergeSensor.MergeSensorDB.MySQL.Base import MySQLBase
from MergeSensor.MergeSensorError import MergeSensorDBError

class GetBlockId(MySQLBase):
      """
       __GetBlockName__

      """
      def execute (self, fileBlock,  conn = None, trans = False):
          """
          _execute_
          """


          # get file block
          self.sqlCommand = """
                     SELECT id
                       FROM merge_fileblock
                      WHERE name='""" + fileBlock + """'
                     """
                                                    
			
	  try:
	  		                    
             result = self.dbi.processData(self.sqlCommand, conn = conn, transaction = trans)
	     
	  except Exception, msg:

             # duplicate or wrong data
             raise MergeSensorDBError, msg   
	  
     
          row = self.formatOne (result, dictionary= False)
          
	
        
   
	   
          return row #//END

 
