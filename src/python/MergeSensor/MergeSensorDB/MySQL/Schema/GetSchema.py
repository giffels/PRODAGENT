#!/usr/bin/env python

"""
        __GetSchema__
        
          
"""


from MergeSensor.MergeSensorDB.MySQL.Base import MySQLBase
from MergeSensor.MergeSensorError import MergeSensorDBError

class GetSchema(MySQLBase):
      """
        __GetSchema__
      """
      
      def execute (self, conn = None, trans = False):
          """
          _execute_
          """
        
          # get schema information
          self.sqlCommand = """
                    SELECT *
                      FROM information_schema.columns
                     WHERE table_name like 'merge_%'
                  ORDER BY table_name;
                 """      
			
	  try:
	  		                    
             result = self.dbi.processData(self.sqlCommand, conn = conn, transaction = trans)
	     
	  except Exception, msg:

             raise MergeSensorDBError, msg   
	  
          rows = self.format(result)
      	 

          return rows #//END

 
