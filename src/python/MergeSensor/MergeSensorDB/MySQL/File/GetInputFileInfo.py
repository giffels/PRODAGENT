#!/usr/bin/env python

"""
        __GetInputFileInfo__

        get output file information
          
"""

import os
from MergeSensor.MergeSensorDB.MySQL.Base import MySQLBase
from MergeSensor.MergeSensorError import MergeSensorDBError

class GetInputFileInfo(MySQLBase):
      """
        __GetInputFileInfo__
        get output file information

      """
      def execute (self, datasetId, fileName,  conn = None, trans = False):
          """
          _execute_
          """

          # get input file information
          self.sqlCommand = """
                     SELECT merge_inputfile.id as id,
                            merge_inputfile.eventcount as eventcount,
                            merge_inputfile.status as status,
                            merge_inputfile.failures as failures,
                            merge_inputfile.instance as instance,
                            merge_inputfile.mergedfile as mergedfile,
                            merge_fileblock.name as block,
                            merge_dataset.prim as prim,
                            merge_dataset.tier as tier,
                            merge_dataset.processed as processed
                       FROM merge_inputfile,
                            merge_dataset,
                            merge_fileblock
                      WHERE merge_inputfile.dataset='""" + str(datasetId) + """'
                        AND merge_inputfile.guid='""" + os.path.basename(fileName) + """'
                        AND merge_inputfile.dataset=merge_dataset.id
                        AND merge_inputfile.block=merge_fileblock.id
                     """

                       
			
	  try:
	  		                    
             result = self.dbi.processData(self.sqlCommand, conn = conn, transaction = trans)
	     
	  except Exception, msg:

             # duplicate or wrong data
             raise MergeSensorDBError, msg   
	  
	     
          row = self.formatOne (result, dictionary= True)
	   
          return row #//END

 
