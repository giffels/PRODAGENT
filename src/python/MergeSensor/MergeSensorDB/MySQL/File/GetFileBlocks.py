#!/usr/bin/env python

"""
        _GetFileBlocks_

        Return a map of LFN:Block name for all unmerged LFNs
        in the dataset provided
          
"""


from MergeSensor.MergeSensorDB.MySQL.Base import MySQLBase


class GetFileBlocks(MySQLBase):
      """
        _GetFileBlocks_

        Return a map of LFN:Block name for all unmerged LFNs
        in the dataset provided
      """
      def execute (self, datasetId,  conn = None, trans = False):
          """
          _execute_
          """

          self.sqlCommand = \
          """
            select MB.name AS block, MI.name AS file from merge_inputfile MI
             JOIN merge_fileblock MB ON MB.id = MI.block
               JOIN merge_dataset DS ON DS.id = MI.dataset
                  WHERE DS.id = %s;

          """ % datasetId
      
      
          result = self.dbi.processData(self.sqlCommand, conn = conn, transaction = trans)
	     
    
	  rows = self.format(result, dictionary = True)   
	  
          return rows #//END

 
