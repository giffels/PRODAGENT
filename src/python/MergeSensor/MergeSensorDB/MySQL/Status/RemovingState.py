#!/usr/bin/env python

"""
        _RemovingState_

        Flag the files listed as removing

          
"""


from MergeSensor.MergeSensorDB.MySQL.Base import MySQLBase
from MergeSensor.MergeSensorError import MergeSensorDBError

class RemovingState(MySQLBase):
      """
        _RemovingState_

        Flag the files listed as removing

      """
      def execute (self, *files, conn = None, trans = False):
          """
          _execute_
          """


          self.sqlCommand = """ UPDATE merge_inputfile set status='removing'
          WHERE name IN ("""

          for fname in files:
            sqlStr += "\'%s\'\n," % fname
          sqlStr = sqlStr.rstrip(',')
          sqlStr += ");"
	
	  try:
	  		                    
             result = self.dbi.processData(self.sqlCommand, conn = conn, transaction = trans)
	     
	  except Exception, msg:

             raise MergeSensorDBError, msg   
         
          return result[0].cursor.rowcount  #//END

 
