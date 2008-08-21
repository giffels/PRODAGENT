#!/usr/bin/env python

"""
        __GetAssociatedInputFiles__
        
        get now all associated input files
"""


from MergeSensor.MergeSensorDB.MySQL.Base import MySQLBase
from MergeSensor.MergeSensorError import MergeSensorDBError

class GetAssociatedInputFiles(MySQLBase):
      """
        __GetAssociatedInputFiles__
        
        get now all associated input files
      """
      
      def execute (self, fileId, conn = None, trans = False):
          """
          _execute_
          """
        
          # get now all associated input files
          self.sqlCommand = """
                     SELECT merge_inputfile.name as filename,
                            merge_fileblock.name as blockname
                       FROM merge_inputfile,
                            merge_fileblock
                      WHERE merge_inputfile.mergedfile='""" \
                             + str(fileId) + """'
                        AND merge_inputfile.block=merge_fileblock.id
                     """
                       
	  result = None
		
	  try:
	  		                    
             result = self.dbi.processData(self.sqlCommand, conn = conn, transaction = trans)
	     
	  except Exception, msg:

             raise MergeSensorDBError, msg   
	  
	     
          rows = self.format (result, dictionary= True)
	
   

          return rows #//END

 
