#!/usr/bin/env python

"""
        _RemovingState_

        Flag the files listed as removing

          
"""

import os
from MergeSensor.MergeSensorDB.MySQL.Base import MySQLBase
from MergeSensor.MergeSensorError import MergeSensorDBError

class RemovingState(MySQLBase):
      """
        _RemovingState_

        Flag the files listed as removing

      """
      def execute (self, *files, **kwargs):
          """
          _execute_
          """
          conn = kwargs.get('conn', None)
          trans = kwargs.get('trans', False)

          self.sqlCommand = """ UPDATE merge_inputfile set status='removing'
          WHERE guid IN ("""

          for fname in files:
            self.sqlCommand += "\'%s\'\n," % os.path.basename(fname)
          self.sqlCommand = self.sqlCommand.rstrip(',')
          self.sqlCommand += ");"
	
	  try:
	  		                    
             result = self.dbi.processData(self.sqlCommand, conn = conn, transaction = trans)
	     
	  except Exception, msg:

             raise MergeSensorDBError, msg   
         
          return result[0].cursor.rowcount  #//END

 
