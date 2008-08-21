#!/usr/bin/env python

"""
        __GetJobList__
        
        Get the list of merge jobs started on a dataset
        
        Arguments:
        
          datasetName -- the name of the dataset
          
        Return:
            
          the list of merge jobs
          
"""


from MergeSensor.MergeSensorDB.MySQL.Base import MySQLBase
from MergeSensor.MergeSensorError import MergeSensorDBError

class GetJobList(MySQLBase):
      """
        __GetJobList__
        
        Get the list of merge jobs started on a dataset
        
        Arguments:
        
          datasetName -- the name of the dataset
          
        Return:
            
          the list of merge jobs
      """
      
      def execute (self, datasetId, conn = None, trans = False):
          """
          _execute_
          """
        
	  # get merge jobs in dataset
          self.sqlCommand = """
                     SELECT mergejob
                       FROM merge_outputfile
                      WHERE dataset='""" + str(datasetId) + "'"
                       
			
	  try:
	  		                    
             result = self.dbi.processData(self.sqlCommand, conn = conn, transaction = trans)
	     
	  except Exception, msg:

             raise MergeSensorDBError, msg   
	  
           
          rows = self.format (result, dictionary= False)          	 

          return rows #//END

 
