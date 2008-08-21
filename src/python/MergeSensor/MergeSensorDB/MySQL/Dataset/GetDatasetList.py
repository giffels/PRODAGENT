#!/usr/bin/env python

"""
_GetDatasetList_

        Get the list of all datasets.
        
        Arguments:
        
          none
          
        Return:
            
          the list of all dataset names which are currently in open status
"""


from MergeSensor.MergeSensorDB.MySQL.Base import MySQLBase

class GetDatasetList(MySQLBase):
      """
      _GetDatasetList_

        Get the list of all datasets.

        Arguments:

          none

        Return:

          the list of all dataset names which are currently in open status

      """
      def execute (self, conn = None, trans = False):
          """
          _execute_
          """
          self.sqlCommand = """
                     SELECT prim, processed, tier
                       FROM merge_dataset
                       WHERE status="open"
                     """  
          result = self.dbi.processData(self.sqlCommand, conn = conn, transaction = trans)

          rows = self.format (result, dictionary = False)
      
         
          return rows #//END GetDatasetList

 
