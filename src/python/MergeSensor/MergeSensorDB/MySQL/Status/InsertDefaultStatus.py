#!/usr/bin/env python

"""
       _InsertDefaultStatus_

          
"""


from MergeSensor.MergeSensorDB.MySQL.Base import MySQLBase
from MergeSensor.MergeSensorError import MergeSensorDBError

class InsertDefaultStatus(MySQLBase):
      """
       _InsertDefaultStatus_

      """
      
      def execute (self, conn = None, trans = False):
          """
          _execute_
          """


          self.sqlCommand = """
                         INSERT
                           INTO merge_control
                                ()
                         VALUES ()
                         """
        

			
	  try:
	  		                    
             result = self.dbi.processData(self.sqlCommand, conn = conn, transaction = trans)
	     
	  except Exception, msg:

             raise MergeSensorDBError, msg   
	  


          return  result[0].rowcount #//END

 
