#!/usr/bin/env python

"""
        __GetWorkflow__
        
          
"""


from MergeSensor.MergeSensorDB.MySQL.Base import MySQLBase
from MergeSensor.MergeSensorError import MergeSensorDBError

class GetWorkflow(MySQLBase):
      """
      __GetWorkflow__
      """
      
      def execute (self, workflow, datasetId, conn = None, trans = False):
          """
          _execute_
          """
        
          self.sqlCommand = """
                     SELECT id
                       FROM merge_workflow
                      WHERE name='""" + workflow + """'
                        AND dataset='""" + str(datasetId) + """'
                     """
                       
	  result = None
		
	  try:
	  		                    
             result = self.dbi.processData(self.sqlCommand, conn = conn, transaction = trans)
	     
	  except Exception, msg:

             raise MergeSensorDBError, str(msg)   
	  
          rowcount = result[0].rowcount

      	 

          return rowcount #//END

 
