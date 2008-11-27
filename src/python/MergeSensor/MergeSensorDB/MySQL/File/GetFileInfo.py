#!/usr/bin/env python

"""
        __GetFileInfo__

        Get merge input file information.

        Arguments:

          datasetId -- the dataset id in database
          fileName -- the file name used to update

        Return:

          A dictionary with input file information
          
"""

import os
from MergeSensor.MergeSensorDB.MySQL.Base import MySQLBase
from MergeSensor.MergeSensorError import MergeSensorDBError

class GetFileInfo(MySQLBase):
      """
        __GetFileInfo__

        Get merge input file information.

        Arguments:

          datasetId -- the dataset id in database
          fileName -- the file name used to update

        Return:

          A dictionary with input file information
      """
      def execute (self, datasetId, fileName,  conn = None, trans = False):
          """
          _execute_
          """


          # get input file information
          self.sqlCommand = """
                     SELECT status, failures
                       FROM merge_inputfile
                      WHERE dataset='""" + str(datasetId) + """'
                        AND guid='""" + os.path.basename(fileName) + """'
                     """

			
	  try:
	  		                    
             result = self.dbi.processData(self.sqlCommand, conn = conn, transaction = trans)
	     
	  except Exception, msg:

             # duplicate or wrong data
             raise MergeSensorDBError, msg   
	  
          if result[0].cursor.rowcount == 0:
	  

            # generate exception
            raise MergeSensorDBError, \
             'Cannot update file %s, not registered in dataset.' % fileName
	     
          row = self.formatOne (result, dictionary= True)
	 
	   
          return row #//END

 
