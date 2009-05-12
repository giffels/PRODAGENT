#!/usr/bin/env python

"""
        __CloseDataset__
        
        close a dataset
        
        Arguments:
        
          datasetName -- the name of the dataset
          
        Return:
            
          none

          
"""


from MergeSensor.MergeSensorDB.MySQL.Base import MySQLBase
from MergeSensor.MergeSensorError import MergeSensorDBError


class CloseDataset(MySQLBase):
      """
        __CloseDataset__
        
        close a dataset
        
        Arguments:
        
          datasetName -- the name of the dataset
          
        Return:
            
          none


      """
      def execute (self, prim, processed, tier,  conn = None, trans = False):
          """
          _execute_
          """


	  # get information
          self.sqlCommand = """
                       UPDATE merge_dataset
                          SET status='closed'
                       WHERE prim='""" + prim + """'
                         AND tier='""" + tier + """'
                         AND processed='""" + processed + """'
                     """
                       
			
			                    
          result = self.dbi.processData(self.sqlCommand, conn = conn, transaction = trans)
	  
         
          return  result[0].rowcount #//END CloseDataset

 
