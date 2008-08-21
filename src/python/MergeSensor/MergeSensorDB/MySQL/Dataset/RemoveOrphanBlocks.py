#!/usr/bin/env python

"""
        __RemoveOrphanBlocks__
     
      
"""


from MergeSensor.MergeSensorDB.MySQL.Base import MySQLBase
from MergeSensor.MergeSensorError import MergeSensorDBError


class RemoveOrphanBlocks (MySQLBase):
      """
        __RemoveOrphanBlocks__
        
      """
      def execute (self, conn = None, trans = False):
          """
          _execute_
          """

	  
          # remove now all orphan fileblocks
          self.sqlCommand = """
                     DELETE FROM merge_fileblock
                      WHERE NOT EXISTS
                            (SELECT NULL
                               FROM merge_inputfile
                              WHERE merge_fileblock.id=merge_inputfile.block);
                     """


          result = self.dbi.processData(self.sqlCommand, conn = conn, transaction = trans)

                

  
          return  result[0].cursor.rowcount  #//END RemoveDataset

 
