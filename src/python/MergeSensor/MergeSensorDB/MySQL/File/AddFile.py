#!/usr/bin/env python

"""
        __AddFile__
        
        Add a file specification (and possibly a fileblock) to
        a dataset (specified by Id)
        

          
"""


from MergeSensor.MergeSensorDB.MySQL.Base import MySQLBase
import MySQLdb
from MergeSensor.MergeSensorError import MergeSensorDBError, \
                                         DatasetNotInDatabase, \
                                         DuplicateLFNError

class AddFile(MySQLBase):
      """
        __AddFile__
        
        Add a file specification (and possibly a fileblock) to
        a dataset (specified by Id)

      """
      def execute (self, fileName, guid, blockId, datasetId, fileSize, events, conn = None, trans = False):
          """
          _execute_
          """

	
          # insert input file
          self.sqlCommand = """
                     INSERT
                       INTO merge_inputfile
                            (name, guid, block, dataset, filesize, eventcount )
                     VALUES ('""" + fileName + """',
                             """ + str(guid) + """,
                             """ + str(blockId) + """,
                             '""" + str(datasetId) + """',
                             '""" + str(fileSize) + """',
                             '""" + str(events) + """'
                            )
                     """

	  try:
	  		                    
             result = self.dbi.processData(str(self.sqlCommand), conn = conn, transaction = trans)
	     
          except MySQLdb.IntegrityError, ex:

              msg = "Duplicate LFN: %s" % str(ex)
              raise DuplicateLFNError, msg
	  
        

          fileId = result[0].lastrowid      
          rowcount = result[0].rowcount 
         
          return (fileId, rowcount) #//END

 
