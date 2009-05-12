#!/usr/bin/env python

"""
        __InsertWorkflow__
        
          
"""


from MergeSensor.MergeSensorDB.MySQL.Base import MySQLBase
from MergeSensor.MergeSensorError import MergeSensorDBError

class InsertWorkflow(MySQLBase):
      """
      __InsertWorkflow__
      """
      
      def execute (self, workflow, datasetId, conn = None, trans = False):
          """
          _execute_
          """
        
          # insert the workflow
          self.sqlCommand = """
                     INSERT 
                       INTO merge_workflow
                            (name, dataset)
                     VALUES ('""" + workflow + """',
                             '""" + str(datasetId) + """')
                     """
			
	  try:
	  		                    
             result = self.dbi.processData(self.sqlCommand, conn = conn, transaction = trans)
	     
	  except Exception, msg:

             raise MergeSensorDBError, str(msg)   
	  
          rowcount = result[0].rowcount

      	 

          return rowcount #//END

 
