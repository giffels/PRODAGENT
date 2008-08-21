#!/usr/bin/env python

"""
       _GetMergeSensorStatus_
        Get the Merge Sensor status
        
        Arguments:
        
          none
          
        Return:
            
          a dictionary with the status information
          
"""


from MergeSensor.MergeSensorDB.MySQL.Base import MySQLBase
from MergeSensor.MergeSensorError import MergeSensorDBError

class GetMergeSensorStatus(MySQLBase):
      """
       _GetMergeSensorStatus_
        Get the Merge Sensor status
        
        Arguments:
        
          none
          
        Return:
            
          a dictionary with the status information
      """
      
      def execute (self, conn = None, trans = False):
          """
          _execute_
          """


          # get status information
          self.sqlCommand = """
                     SELECT *
                       FROM merge_control
                     """

			
	  try:
	  		                    
             result = self.dbi.processData(self.sqlCommand, conn = conn, transaction = trans)
	     
	  except Exception, msg:

             raise MergeSensorDBError, msg   
	  
          result = self.formatOne (result, dictionary =  True)         
          return result  #//END

 
