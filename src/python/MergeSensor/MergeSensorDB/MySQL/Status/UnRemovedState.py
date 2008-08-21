#!/usr/bin/env python

"""
       _UnRemovedState_

        Flag the files listed as unremoved, or as removefailed if the
        above a certain threshold.

        That threshold is set to 3 as that seems to be a decent default
        to get started
"""


from MergeSensor.MergeSensorDB.MySQL.Base import MySQLBase
from MergeSensor.MergeSensorError import MergeSensorDBError

class UnRemovedState(MySQLBase):
      """
       _UnRemovedState_

        Flag the files listed as unremoved, or as removefailed if the
        above a certain threshold.

        That threshold is set to 3 as that seems to be a decent default
        to get started

      """
      def execute (self, *files, conn = None, trans = False):
          """
          _execute_
          """


          numberOfFailuresMax = 3
        
          fileList = "("
          for fname in files:
              fileList += "\'%s\'\n," % fname 
          fileList = fileList.rstrip(',')
          fileList += ")"
        
          self.sqlCommand = """ update merge_inputfile set status='unremoved', remove_failures = remove_failures+1
            WHERE name IN %s;""" % fileList
          self.sqlCommand += """update merge_inputfile set status="removefailed"
                       where remove_failures >= %s
                        and name in %s""" % (
              numberOfFailuresMax, fileList)
        
          self.sqlCommand = sqlStr.split(';')
  
	  result = None
	  try:
	     for sql in self.sqlCommand: 		                    
                result = self.dbi.processData(sql, conn = conn, transaction = trans)
	     
	  except Exception, msg:

             raise MergeSensorDBError, msg   
	  

	     
          
          return result[0].cursor.rowcount #//END

 
