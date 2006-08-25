#!/usr/bin/env python

"""
_SplitterMaker_

For a given dataset, create and return a JobSplitter instance


"""



from PhEDExInterface.DBSDLSToolkit import DBSDLSToolkit
from DatasetInjector.JobSplitter import JobSplitter





def createJobSplitter(dataset):
    """
    _createJobSplitter_

    Instantiate a JobSplitter instance for the dataset provided
    and populate it with details from DBS/DLS via the DBSDLSToolkit


    """
    toolkit = DBSDLSToolkit()

    result = JobSplitter(dataset)


    blocks =  toolkit.listFileBlocksForDataset(dataset)

    events =  toolkit.getDatasetFiles(dataset)

    for block in blocks:
        blockName = block['blockName']
        locations = toolkit.getFileBlockLocation(blockName)
        newBlock = result.newFileblock(blockName, * locations)
        for fileEntry in block['fileList']:
            lfn = fileEntry['logicalFileName']
            numEvents = events.get(lfn, None)
            if numEvents == None:
                continue
            newBlock.addFile(lfn, numEvents)

    del blocks, events

    return result
            
    






