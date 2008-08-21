#!/usr/bin/env python

"""
        __GetFileList__
        
        Get the list of files associated to a dataset (specified by id)
        
        Arguments:
        
          datasetId -- the dataset id in database
          
        Return:
            
          the list of files in the dataset
          
"""


from MergeSensor.MergeSensorDB.MySQL.Base import MySQLBase

class GetFileList(MySQLBase):
      """
        __GetFileList__
        
        Get the list of files associated to a dataset (specified by id)
        
        Arguments:
        
          datasetId -- the dataset id in database
          
        Return:
            
          the list of files in the dataset
      """
      def execute (self, datasetId,  conn = None, trans = False):
          """
          _execute_
          """

          # get all files associated to dataset
          self.sqlCommand = """
                     SELECT merge_inputfile.name as name,
                            merge_fileblock.name as blockname,
                            merge_inputfile.filesize as filesize
                       FROM merge_inputfile,
                            merge_fileblock
                      WHERE merge_inputfile.dataset='""" + str(datasetId) + """'
                        AND merge_fileblock.id=merge_inputfile.block
                     """
                       
			
	 
	  		                    
          result = self.dbi.processData(self.sqlCommand, conn = conn, transaction = trans)    
         
          return self.format(result, dictionary = True) #//END

 
