#!/usr/bin/env python

"""
_SplitterMaker_

For a given dataset, create and return a JobSplitter instance


"""

import logging

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
        logging.info("Using Local DBS/DLS")
        toolkit = DBSDLSToolkit()
    else:
        logging.info("Using Remote DBS/DLS: %s" % dbsdlsContacts)
        toolkit = RemoteDBSDLSToolkit(**dbsdlsContacts)
    
    result = JobSplitter(dataset)


    blocks =  toolkit.listFileBlocksForDataset(dataset)
    events =  toolkit.getDatasetFiles(dataset)

    
    
    for block in blocks:
        blockName = block['blockName']
        try:
            locations = toolkit.getFileBlockLocation(blockName)
        except Exception, ex:
            msg = "Unable to find DLS Locations for Block: %s\n" %  blockName
            msg += str(ex)
            msg += "\nSkipping import of this block..."
            logging.warning(msg)
            continue
            
        newBlock = result.newFileblock(blockName, * locations)
        for fileEntry in block['fileList']:
            lfn = fileEntry['logicalFileName']
            numEvents = events.get(lfn, None)
            if numEvents == None:
                continue
            newBlock.addFile(lfn, numEvents)

    del blocks, events

    return result
            
    






