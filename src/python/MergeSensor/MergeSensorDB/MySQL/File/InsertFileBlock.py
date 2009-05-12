#!/usr/bin/env python

"""
        __InsertFileBlock__

          
"""


from MergeSensor.MergeSensorDB.MySQL.Base import MySQLBase
import MySQLdb
from MergeSensor.MergeSensorError import MergeSensorDBError, \
                                         DatasetNotInDatabase, \
                                         DuplicateLFNError

class InsertFileBlock(MySQLBase):
      """
       __InsertFileBlock__
      """
      
      def execute (self, fileBlock, conn = None, trans = False):
          """
          _execute_
          """
	  
          blockId = 'last_insert_id()'

          # insert it
          self.sqlCommand = """
                     INSERT INTO merge_fileblock
                            (name)
                     VALUES ('""" + fileBlock + """')
                     """
        
	  try:
	  		                    
             result = self.dbi.processData(self.sqlCommand, conn = conn, transaction = trans)
	     
          except Exception, ex:
              msg = 'Failed to insert File Block'
	      msg += str(ex) 
              raise MergeSensorDBError, msg
	  
        
      
          rows = result[0].rowcount
          blockId =  result[0].lastrowid
        

         
          return (blockId, rows) #//END

 
