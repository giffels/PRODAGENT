#!/usr/bin/env python

"""
        __GetJobProperties__
        
        Get the job information
        
        Arguments:
        
          jobId -- the job name
          
        Return:
            
          a dictionary with all job information
          
"""


from MergeSensor.MergeSensorDB.MySQL.Base import MySQLBase
from MergeSensor.MergeSensorError import MergeSensorDBError

class GetJobProperties(MySQLBase):
      """
        __getJobInfo__
        
        Get the job information
        
        Arguments:
        
          jobId -- the job name
          
        Return:
            
          a dictionary with all job information
      """
      
      def execute (self, jobId, conn = None, trans = False):
          """
          _execute_
          """
        
          # get main properties
          self.sqlCommand = """
                     SELECT merge_outputfile.name as outputfile,
                            merge_outputfile.id as fileid,
                            merge_outputfile.status as status,
                            merge_dataset.prim as prim,
                            merge_dataset.tier as tier,
                            merge_dataset.processed as processed
                       FROM merge_outputfile, merge_dataset
                      WHERE mergejob='""" + str(jobId) + """'
                        AND merge_dataset.id = merge_outputfile.dataset

                     """
          result = None
			
	  try:
	  		                    
             result = self.dbi.processData(self.sqlCommand, conn = conn, transaction = trans)
	     
	  except Exception, msg:

             raise MergeSensorDBError, msg   
	  
           
          rows = self.formatOne (result, dictionary= True)
	
   

          return rows #//END

 
