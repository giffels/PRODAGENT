#!/usr/bin/env python

"""
        __GetOutputFileInfo__

        get output file information
          
"""


from MergeSensor.MergeSensorDB.MySQL.Base import MySQLBase
from MergeSensor.MergeSensorError import MergeSensorDBError

class GetOutputFileInfo(MySQLBase):
      """
        __GetOutputFileInfo__
        get output file information

      """
      def execute (self, datasetId,  conn = None, trans = False):
          """
          _execute_
          """


          # get output file information
          self.sqlCommand = """
                     SELECT id,
                            name,
                            mergejob
                       FROM merge_outputfile
                      WHERE dataset='""" + str(datasetId) + """'
                        AND status='do_it_again'
                      LIMIT 1
                     """
                       
			
	  try:
	  		                    
             result = self.dbi.processData(self.sqlCommand, conn = conn, transaction = trans)
	     
	  except Exception, msg:

             raise MergeSensorDBError, str(msg)  
          	     
          row = self.formatOne (result, dictionary= True)

	   
          return row #//END

 
