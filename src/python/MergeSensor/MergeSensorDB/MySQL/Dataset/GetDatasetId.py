#!/usr/bin/env python

"""
        __GetDatasetId__
        
        Get the id of the dataset
        
        Arguments:
        
          datasetName -- the name of the dataset
          
        Return:
            
          the dataset id identification in the database
          
"""


from MergeSensor.MergeSensorDB.MySQL.Base import MySQLBase


class GetDatasetId(MySQLBase):
      """
        __GetDatasetId__
        
        Get the id of the dataset
        
        Arguments:
        
          datasetName -- the name of the dataset
          
        Return:
            
          the dataset id identification in the database
          

      """
      def execute (self, prim, tier, processed, conn = None, trans = False):
          """
          _execute_
          """
	  
	  # get dataset id
          self.sqlCommand = """
                     SELECT id, status
                       FROM merge_dataset
                       WHERE prim='""" + prim + """'
                         AND tier='""" + tier + """'
                         AND processed='""" + processed + "'"
			 
			
			                    
          result = self.dbi.processData(self.sqlCommand, conn = conn, transaction = trans)

          row = self.formatOne (result, dictionary= True)
	  

          return row #//END GetDatasetId

 
