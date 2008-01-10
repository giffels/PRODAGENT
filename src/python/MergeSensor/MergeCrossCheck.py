#!/usr/bin/env python
"""
_FileCrossCheck_


Tools for cross checking file lists in merge sensor, DBS etc

"""
__version__ = "$Revision$"
__revision__ = "$id$"

import logging


from MergeSensor.MergeSensorDB import MergeSensorDB


def listAllMergeDatasets():
    """
    _listAllMergeDatasets_

    Get a list of all merge datasets currently watched by the merge sensor

    """
    mergeDB = MergeSensorDB()
    datasets = mergeDB.getDatasetList()
    return datasets



class MergeSensorCrossCheck:
    """
    _MergeSensorCrossCheck_

    Object representing a cross check for the data in
    the merge sensor for a particular workflow

    Provide an API for extracting the dataset information
    from the merge sensor
    
    """
    def __init__(self, datasetName):
        self.dataset = datasetName
        self.mergeDB = MergeSensorDB()
        self.datasetInfo = self.mergeDB.getDatasetInfo(self.dataset)
        self.datasetId = self.mergeDB.getDatasetId(self.dataset)


    def __del__(self):
        self.mergeDB.closeDatabaseConnection()
        

    def getFiles(self):
        """
        _getFiles_

        Extract a list of files

        """
        filelist = self.mergeDB.getFileList(self.datasetId)
        return [ x['name'] for x in filelist]
        
        

    def getPendingUnmergedFiles(self):
        """
        _getPendingUnmergedFiles_

        Get a list of unmerged files waiting to be merged

        """
        filelist = self.mergeDB.getUnmergedFileList(self.datasetId)
        result = []
        for block in filelist:
            for filedata in block[1]:
                result.append(filedata['name'])
                
                
        
        return result
        
    def getFileMap(self):
        """
        _getFileMap_

        Get mapping of unmerged LFN to merged LFN or None

        """
        return self.mergeDB.getDatasetFileMap(self.datasetId)
    
    def getBlocksMap(self):
        """
        _getBlocksMap_

        Retrieve a map of LFN : block name for unmerged files

        """
        return self.mergeDB.getFileBlocks(self.datasetId)
        

    def removedFiles(self):
        """
        _removedFiles_

        Files flagged as removed

        """
        return [ x for x, v in self.mergeDB.removalInfo(self.datasetId).items()
                 if v == "removed" ]
        
    def removingFiles(self):
        """
        _removingFiles_

        Files flagged as removing state (IE removal should be in progress

        """
        return [ x for x, v in self.mergeDB.removalInfo(self.datasetId).items()
                 if v == "removing" ]

    def removalFiles(self):
        """
        _removalFiles_

        All files in process of being removed or that have been removed

        """
        return self.mergeDB.removalInfo(self.datasetId).keys()


    def flagRemoving(self, *files):
        """
        _flagRemoving_

        Flag the list of LFNs as removing status

        """
        self.mergeDB.removingState(*files)
        self.mergeDB.commit()

        

        
