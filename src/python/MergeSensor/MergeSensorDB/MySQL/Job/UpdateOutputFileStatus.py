#!/usr/bin/env python

"""
        __UpdateOutputFileStatus__

"""


from MergeSensor.MergeSensorDB.MySQL.Base import MySQLBase
from MergeSensor.MergeSensorError import MergeSensorDBError, \
                                         DatasetNotInDatabase, \
                                         DuplicateLFNError

class UpdateOutputFileStatus(MySQLBase):
      """
        __UpdateOutputFileStatus__

      """
      def execute (self, jobName, conn = None, trans = False):
          """
          _execute_
          """

          # update output file informarion
          self.sqlCommand = """
                     UPDATE merge_outputfile
                        SET status='do_it_again'
                      WHERE mergejob='""" + jobName + """'
                     """
                           

	  try:
	  		                    
             result = self.dbi.processData(self.sqlCommand, conn = conn, transaction = trans)
	     
          except Exception, ex:
      
              raise MergeSensorDBError, str(ex)
	  
        

          rows = result[0].rowcount
	 

         
          return rows #//END

 
