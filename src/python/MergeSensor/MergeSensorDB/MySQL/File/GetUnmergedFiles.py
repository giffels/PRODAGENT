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
                         mergedfile,filesize,lumi.run,failures,
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

          return __arrangeRuns(rows)


def __arrangeRuns(file_list):
    """
    
    Takes a list of files with duplicates for each run/lumi combination,
    returns a list of unique files with run fields set to a list
    
    >>> __arrangeRuns([{'run': 1, 'id':1}, \
                          {'run': 2, 'id':2}, {'run': 3, 'id':3}])
    [{'run': [1], 'id': 1}, {'run': [2], 'id': 2}, {'run': [3], 'id': 3}]
    
    >>> __arrangeRuns([{'run': 1, 'id':1}, {'run': 2, 'id':2}, \
                              {'run': 3, 'id':2}, {'run': 4, 'id':2}])
    [{'run': [1], 'id': 1}, {'run': [2, 3, 4], 'id': 2}]
    """
    output = []
    index = 0

    # insert each run/lumi section once
    for this_file in file_list:
        subindex = index
        this_file['run'] = [this_file['run']]
        output.append(this_file)

        # find all runs for this file
        for other_file in file_list[index+1:]:
            if other_file['id'] == this_file['id']:
                this_file['run'].append(other_file['run'])
                # once the run is recorded remove the duplicate file
                file_list.pop(subindex)
                # dont increase subindex as next element will take this place
            else:
                subindex += 1
        index += 1
    return output #//END


if __name__ == "__main__":
    import doctest
    doctest.testmod()

