#!/usr/bin/env python

"""
        _GetDatasetFileMap_

        Retrieve a mapping of input LFN to output LFN for completed
        merges.
          
"""


from MergeSensor.MergeSensorDB.MySQL.Base import MySQLBase


class GetDatasetFileMap(MySQLBase):
      """
        _GetDatasetFileMap_

        Retrieve a mapping of input LFN to output LFN for completed
        merges.
          

      """
      def execute (self,  datasetId,  conn = None, trans = False):
          """
          _execute_
          """
	  

          self.sqlCommand = \
           """
           SELECT MI.name, MO.lfn FROM merge_inputfile MI
             JOIN merge_outputfile MO  ON MI.mergedfile = MO.id
               JOIN  merge_dataset DS  ON DS.id = MO.dataset
                 WHERE DS.id = %s AND MI.status NOT IN ('removing', 'removed', 'removefailed');
           """  % datasetId
			
			                    
          result = self.dbi.processData(self.sqlCommand, conn = conn, transaction = trans)

          rows = self.format (result, dictionary= True)
	  
        
          return rows #//END GetDatasetFileMap

 
