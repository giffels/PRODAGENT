#!/usr/bin/env python

"""
_SplitterMaker_

For a given dataset, create and return a JobSplitter instance


"""



from PhEDExInterface.DBSDLSToolkit import DBSDLSToolkit, RemoteDBSDLSToolkit
from DatasetInjector.JobSplitter import JobSplitter




def createJobSplitter(dataset, **dbsdlsContacts):
    """
    _createJobSplitter_

    Instantiate a JobSplitter instance for the dataset provided
    and populate it with details from DBS/DLS via the DBSDLSToolkit

    If dbsdlsContacts are provided, then the DBS and DLS that they
    point to is used, otherwise the ProdAgent local DBS and DLS are extracted
    from the cfg file and used.

    """

    if len(dbsdlsContacts) == 0:
        toolkit = DBSDLSToolkit()
    else:
        toolkit = RemoteDBSDLSToolkit(*dbsdlsContacts)
    
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
            
    






