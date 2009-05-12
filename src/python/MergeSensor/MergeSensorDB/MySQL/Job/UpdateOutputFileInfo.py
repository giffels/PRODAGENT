#!/usr/bin/env python

"""
        __UpdateOutputFileInfo__
        
          insert dataset information

            
          
          
"""


from MergeSensor.MergeSensorDB.MySQL.Base import MySQLBase
from MergeSensor.MergeSensorError import MergeSensorDBError

class UpdateOutputFileInfo(MySQLBase):
      """
        __UpdateOutputFileInfo__
        

          
      """
      def execute (self, name, newJobId, mergeJobId,  conn = None, trans = False):
          """
          _execute_
          """


          # insert it
          self.sqlCommand = """
                 UPDATE merge_outputfile
                    SET name='""" + str(name) + """',
                        instance=instance+1,
                        status='undermerge',
                        mergejob='""" + str(newJobId) + """'
                  WHERE id='""" + str(mergeJobId) +"""'
                 """
          result = None

	  try:
	  		                    
             result = self.dbi.processData(self.sqlCommand, conn = conn, transaction = trans)
	     
	  except Exception, msg:

             # duplicate or wrong data
             raise MergeSensorDBError, msg   
	  

	  rowcount = result[0].rowcount 
          fileId = result[0].lastrowid	
	       
	   
          return  (fileId, rowcount)  #//END

 
