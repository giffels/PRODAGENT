#!/usr/bin/env python

"""
        _RemovedState_

        Flag the files listed as removed
          
"""


from MergeSensor.MergeSensorDB.MySQL.Base import MySQLBase
from MergeSensor.MergeSensorError import MergeSensorDBError

class RemovedState(MySQLBase):
      """
        _RemovedState_

        Flag the files listed as removed
          
      """
      def execute (self, *files, conn = None, trans = False):
          """
          _execute_
          """


          self.sqlCommand = """ UPDATE merge_inputfile set status='removed'
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

 
