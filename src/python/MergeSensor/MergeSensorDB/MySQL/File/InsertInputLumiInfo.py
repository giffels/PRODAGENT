#!/usr/bin/env python

"""
        __InsertInputLumiInfo__

          
"""


from MergeSensor.MergeSensorDB.MySQL.Base import MySQLBase
import MySQLdb
from MergeSensor.MergeSensorError import MergeSensorDBError

class InsertInputLumiInfo(MySQLBase):
      """
        __InsertInputLumiInfo__
      """
      
      def execute (self, binds, conn = None, trans = False):
          """
          _execute_
          """
	  

	  self.sqlCommand = """
	         Insert into merge_input_lumi(run, lumi, file_id)
		 values(:RunNumber, :LumiSectionNumber, :fileId)""" 
        
	  try:
	  		                    
             result = self.dbi.processData(self.sqlCommand, binds, conn = conn, transaction = trans)
	     
          except Exception, ex:
              msg = 'Failed to insert input lumi info'
	      msg += str(ex) 
              raise MergeSensorDBError, msg
	  

         
          return result[0].cursor.rowcount #//END

 
