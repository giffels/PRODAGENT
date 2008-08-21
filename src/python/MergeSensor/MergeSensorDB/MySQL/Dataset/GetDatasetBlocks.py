#!/usr/bin/env python

"""
        __GetDatasetBlocks__

"""


from MergeSensor.MergeSensorDB.MySQL.Base import MySQLBase


class GetDatasetBlocks(MySQLBase):
      """
         __GetDatasetBlocks__


      """
      def execute (self, datasetId, conn = None, trans = False):
          """
          _execute_
          """
	  
          # get file blocks in dataset
          self.sqlCommand = """
                       SELECT DISTINCT merge_inputfile.block as block,
                                     merge_fileblock.name as name
                       FROM merge_inputfile,
                            merge_fileblock
                      WHERE merge_inputfile.dataset='""" + str(datasetId) + """'
                        AND merge_inputfile.status='unmerged'
                        AND merge_inputfile.block=merge_fileblock.id
                     """
			                    
          result = self.dbi.processData(self.sqlCommand, conn = conn, transaction = trans)

          rows = self.format (result, dictionary= True)

          return rows #//END GetDatasetInfo
	  
 
