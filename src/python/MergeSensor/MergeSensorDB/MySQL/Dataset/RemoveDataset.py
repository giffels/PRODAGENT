#!/usr/bin/env python

"""
        __RemoveDataset__
        
        Remove a dataset        
      
"""


from MergeSensor.MergeSensorDB.MySQL.Base import MySQLBase
from MergeSensor.MergeSensorError import MergeSensorDBError


class RemoveDataset(MySQLBase):
      """
        __RemoveDataset__
        
        Remove a dataset      

      """
      def execute (self,  prim, processed, tier,  conn = None, trans = False):
          """
          _execute_
          """

	  
	  self.sqlCommand = """
                       DELETE FROM merge_dataset
                       WHERE prim='""" + prim + """'
                         AND tier='""" + tier + """'
                         AND processed='""" + processed + """'
                     """
        
			                    
          result = self.dbi.processData(self.sqlCommand, conn = conn, transaction = trans)
	  
  
          return  result[0].cursor.rowcount  #//END RemoveDataset

 
