#!/usr/bin/env python

"""
       _RemovalInfo_

        Return a map of LFN: removal status
          
"""


from MergeSensor.MergeSensorDB.MySQL.Base import MySQLBase

class RemovalInfo(MySQLBase):
      """
       _RemovalInfo_

        Return a map of LFN: removal status
      """
      def execute (self, datasetId, conn = None, trans = False):
          """
          _execute_
          """


          self.sqlCommand = """ SELECT name, status FROM merge_inputfile
           WHERE dataset=%s AND status in ('removed', 'removing', 'unremoved', removefailed');""" % (
            datasetId,)
  		                    
          result = self.dbi.processData(self.sqlCommand, conn = conn, transaction = trans)
	     
          rows = self.format (result, dictionary= False)
	 
          return rows  #//END

 
