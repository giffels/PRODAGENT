#!/usr/bin/env python

"""
        __GetDatasetInfo__
        
        Get information on dataset (on any status)
        
        Arguments:
        
          datasetName -- the name of the dataset
          
        Return:
            
          a dictionary with all dataset information from database
"""


from MergeSensor.MergeSensorDB.MySQL.Base import MySQLBase


class GetDatasetInfo(MySQLBase):
      """
        __GetDatasetInfo__
        
        Get information on dataset (on any status)
        
        Arguments:
        
          datasetName -- the name of the dataset
          
        Return:
            
          a dictionary with all dataset information from database

      """
      def execute (self, prim, tier, processed, conn = None, trans = False):
          """
          _execute_
          """
	  
          # get information
          self.sqlCommand = """
                     SELECT prim as primaryDataset,
                            tier as dataTier,
                            processed as processedDataset,
                            psethash as PSetHash,
                            status,
                            started,
                            updated as lastUpdated,
                            version,
                            workflow as workflowName,
                            mergedlfnbase as mergedLFNBase,
                            category,
                            timeStamp,
                            sequence as outSeqNumber
                       FROM merge_dataset
                       WHERE prim='""" + prim + """'
                         AND tier='""" + tier + """'
                         AND processed='""" + processed + """'   
			 """
			                    
          result = self.dbi.processData(self.sqlCommand, conn = conn, transaction = trans)

          rows = self.format (result, dictionary= True)

          return rows #//END GetDatasetInfo
	  
 
