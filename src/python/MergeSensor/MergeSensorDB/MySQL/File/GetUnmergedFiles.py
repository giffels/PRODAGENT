#!/usr/bin/env python

"""
        __GetUnmergedFiles__

          
"""


from MergeSensor.MergeSensorDB.MySQL.Base import MySQLBase
from MergeSensor.MergeSensorError import MergeSensorDBError

class GetUnmergedFiles(MySQLBase):
      """
        __GetUnmergedFiles__

      """
      def execute (self, datasetId, block,  conn = None, trans = False):
          """
          __execute__
	  
	  """
            
          # get all unmerged files in a particular fileblock
          # actually pulls one result per lumi section - need to remove dups
          # Can we do this in the db?
          self.sqlCommand = """
                     SELECT id,name,guid,eventcount,block,status,dataset,
                         mergedfile,filesize,merge_inputfile.run,failures,
                         instance
                       FROM merge_inputfile, merge_lumi as lumi
                      WHERE merge_inputfile.id = lumi.file_id AND
                        dataset='""" + str(datasetId) + """'
                        AND block='""" + str(block['block'])  + """'
                        AND status='unmerged'
                   ORDER BY lumi.run, lumi.lumi
                     """
	  try:
	  		                    
             result = self.dbi.processData(self.sqlCommand, conn = conn, transaction = trans)
	     
	  except Exception, msg:

             # duplicate or wrong data
             raise MergeSensorDBError, msg   
	  
	     
          rows = self.format (result, dictionary= True)
          
          ids = {}
          output = []
          for row in rows:
              if not ids.has_key(row['id']):
                  output.append(row)
                  ids[row['id']] = None
          rows = output
          return rows #//END
