#!/usr/bin/env python

"""
       _SetMergeSensorStatus_
        Set the Merge Sensor status
        
          
"""


from MergeSensor.MergeSensorDB.MySQL.Base import MySQLBase
from MergeSensor.MergeSensorError import MergeSensorDBError

class SetMergeSensorStatus(MySQLBase):
      """
       _SetMergeSensorStatus_
        Set the Merge Sensor status
        
      """
      
      def execute (self, updates, conn = None, trans = False):
          """
          _execute_
          """  
      
          # get information
          self.sqlCommand = """
                       UPDATE merge_control
                          SET """ + updates
        
	  try:
	  		                    
             result = self.dbi.processData(self.sqlCommand, conn = conn, transaction = trans)
	     
	  except Exception, msg:

             raise MergeSensorDBError, msg   
	  

          return result[0].cursor.rowcount #//END

 
