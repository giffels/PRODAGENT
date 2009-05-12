#!/usr/bin/env python

"""
        __UpdateDataset__
        
        Update 'last update' field and mark it as open
        
        Arguments:
        
          datasetName -- the dataset name
          sequenceNumber -- the sequence number
          
        Return:
            
          none
          
"""


from MergeSensor.MergeSensorDB.MySQL.Base import MySQLBase

class UpdateDataset(MySQLBase):
      """
        __UpdateDataset__
        
        Update 'last update' field and mark it as open
        
        Arguments:
        
          datasetName -- the dataset name
          sequenceNumber -- the sequence number
          
        Return:
            
          none
          

      """
      def execute (self,  sequenceString, prim, tier, processed,  conn = None, trans = False):
          """
          _execute_
          """       
	
	              
          # insert dataset information
          self.sqlCommand = """
                     UPDATE merge_dataset
                        SET status='open',
                            updated=current_timestamp
                     """ + sequenceString + """
                      WHERE prim='""" + prim + """'
                        AND tier='""" + tier + """'
                        AND processed='""" + processed + """'
                     """    

			
			                    
          result = self.dbi.processData(self.sqlCommand, conn = conn, transaction = trans)

         
          return result[0].rowcount  #//END UpdateDataset

 
