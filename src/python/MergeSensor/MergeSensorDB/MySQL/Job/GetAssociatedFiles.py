#!/usr/bin/env python

"""
        __GetAssociatedFiles__
        
        get now all associated input files for redo job
"""


from MergeSensor.MergeSensorDB.MySQL.Base import MySQLBase
from MergeSensor.MergeSensorError import MergeSensorDBError

class GetAssociatedFiles(MySQLBase):
      """
        __GetAssociatedInputFiles__
        
        get now all associated input files for redo job
      """
      
      def execute (self, jobInfo, conn = None, trans = False):
          """
          _execute_
          """
        
          # get associated input file information
          self.sqlCommand = """
                         SELECT merge_inputfile.id
                           FROM merge_inputfile, merge_outputfile
                          WHERE merge_inputfile.mergedfile=""" + \
                                                str(jobInfo['fileId']) + """
                            AND merge_inputfile.status!='invalid'
                         """
          result = None			
	  try:
	  		                    
             result = self.dbi.processData(self.sqlCommand, conn = conn, transaction = trans)
	     
	  except Exception, msg:

             raise MergeSensorDBError, msg   
	  
          rowcount = result[0].rowcount
	
	
   

          return rowcount #//END

 
