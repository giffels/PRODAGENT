#!/usr/bin/env python

"""
        __GetDatasetListFromWorkflow__

        Get the list of datasets belonging to a workflow
                                                                                
        Arguments:
                                                                                
          workflow name
                                                                                
        Return:
                                                                                
          the list of all dataset names which are currently in open status
          and belong to the workflow
"""


from MergeSensor.MergeSensorDB.MySQL.Base import MySQLBase

class GetDatasetListFromWorkflow(MySQLBase):
      """
        __GetDatasetListFromWorkflow__

        Get the list of datasets belonging to a workflow
                                                                                
        Arguments:
                                                                                
          workflow name
                                                                                
        Return:
                                                                                
          the list of all dataset names which are currently in open status
          and belong to the workflow

      """
      def execute (self, workflowName, conn = None, trans = False):
          """
          _execute_
          """
          self.sqlCommand = """
                     SELECT prim, processed, tier
                       FROM merge_dataset
                       WHERE status="open" 
                       AND workflow='""" + workflowName + """'
		       """ 
		       
          result = self.dbi.processData(self.sqlCommand, conn = conn, transaction = trans)

          rows = self.format (result)     
 
          return rows #//END GetDatasetList

 
