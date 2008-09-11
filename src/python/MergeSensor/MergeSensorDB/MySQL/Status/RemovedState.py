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
      def execute (self, *files, **kwargs):
          """
          _execute_
          """
          conn = kwargs.get('conn', None)
          trans = kwargs.get('trans', False)

          self.sqlCommand = """ UPDATE merge_inputfile set status='removed'
            WHERE name IN ("""

          for fname in files:
              self.sqlCommand += "\'%s\'\n," % fname
          self.sqlCommand = self.sqlCommand.rstrip(',')
          self.sqlCommand += ");"

	  try:
	  		                    
             result = self.dbi.processData(self.sqlCommand, conn = conn, transaction = trans)
	     
	  except Exception, msg:

             raise MergeSensorDBError, msg   
	  

	     
          
          return result[0].cursor.rowcount  #//END

 
