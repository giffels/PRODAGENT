#!/usr/bin/env python

"""
        __InsertDataset__
        
        insert dataset information
        
        Arguments:
        
          data -- the dataset associated dictionary
          
        Return:
            
          none
          
"""


from MergeSensor.MergeSensorDB.MySQL.Base import MySQLBase
from MergeSensor.MergeSensorError import MergeSensorDBError
import MySQLdb

class InsertDataset(MySQLBase):
      """
        __InsertDataset__
        
        insert dataset information
        
        Arguments:
        
          data -- the dataset associated dictionary
          
        Return:
            
          none
          

      """
      def execute (self, data,  conn = None, trans = False):
          """
          _execute_
          """

          # insert dataset information
          self.sqlCommand = """
                     INSERT
                       INTO merge_dataset
                            (prim,tier,processed,
                             psethash,started,updated,version,workflow,
                             mergedlfnbase,category,timestamp,sequence)
                      VALUES ('""" + data['primaryDataset'] + """',
                              '""" + data['dataTier'] + """',
                              '""" + data['processedDataset'] + """',
                              '""" + str(data['PSetHash']) + """',
                              '""" + data['started'] + """',
                              '""" + data['lastUpdated'] + """',
                              '""" + data['version'] + """',
                              '""" + data['workflowName'] + """',
                              '""" + data['mergedLFNBase'] + """',
                              '""" + data['category'] + """',
                              '""" + str(data['timeStamp']) + """',
                              '""" + str(data['outSeqNumber']) + """')
                     """
        
			
	  try:
	  		                    
             result = self.dbi.processData(self.sqlCommand, conn = conn, transaction = trans)
	     
	  except MySQLdb.IntegrityError, msg:

             # duplicate or wrong data
             raise MergeSensorDBError, msg   
	  
          rowcount = result[0].cursor.rowcount
          fileId =  result[0].cursor.lastrowid

        
          return (fileId, rowcount)   #//END InsertDataset

 
