#!/usr/bin/env python
"""
_InjectionSpec_

Interface for generating a PhEDEx injection XML file listing the
datasets, fileblocks and LFNs to be inserted

"""

from IMProv.IMProvNode import IMProvNode
from IMProv.IMProvDoc import IMProvDoc



class Fileblock(list):
    """
    _Fileblock_

    Object representing a fileblock

    """
    def __init__(self, fileblockName, isOpen = "y"):
        list.__init__(self)
        self.fileblockName = fileblockName
        self.isOpen = isOpen

    def addFile(self, lfn, checksum, size):
        """
        _addFile_

        Add a file to this fileblock

        """
        self.append(
            ( lfn, checksum, size, )
            )
        return
    

    def save(self):
        """
        _save_

        Serialise this to XML compatible with PhEDEx injection
        
        """
        result = IMProvNode("block")
        result.attrs['name'] = self.fileblockName
        result.attrs['is-open'] = self.isOpen
        for entry in self:
            result.addNode(
                IMProvNode("file", None,
                           lfn = entry[0],
                           checksum = entry[1],
                           size = entry[2])
                )
        return result

    



class InjectionSpec:
    """
    _InjectionSpec_

    <dbs name='DBSNameHere'>
    <dataset name='/primary/datatier/processed' is-open='boolean' is-transient='boolean'>
    <block name='fileblockname' is-open='boolean'>
    <file lfn='lfn1Here' checksum='cksum:0' size ='fileSize1Here'/>
    <file lfn='lfn2Here' checksum='cksum:0' size ='fileSize2Here'/> </block>
    </dataset>
    </dbs> 
    """
    def __init__(self, dbs,
                 datasetName,
                 datasetOpen = "y",
                 datasetTransient = "n" ):
        self.dbs = dbs
        #  //
        # // dataset attributes
        #//
        self.datasetName = datasetName
        self.datasetIsOpen = datasetOpen
        self.datasetIsTransient = datasetTransient

        #  //
        # // Fileblocks
        #//
        self.fileblocks = {}


    def getFileblock(self, fileblockName, isOpen = "y"):
        """
        _getFileblock_

        Add a new fileblock with name provided if not present, if it exists,
        return it

        """
        if self.fileblocks.has_key(fileblockName):
            return self.fileblocks[fileblockName]
        
        newFileblock = Fileblock(fileblockName, isOpen)
        self.fileblocks[fileblockName] = newFileblock
        return newFileblock

    def save(self):
        """
        _save_

        serialise object into PhEDEx injection XML format

        """
        result = IMProvNode("dbs")
        result.attrs['name'] = self.dbs
        dataset = IMProvNode("dataset")
        dataset.attrs['name'] = self.datasetName
        dataset.attrs['is-open'] = self.datasetIsOpen
        dataset.attrs['is-transient'] = self.datasetIsTransient

        
        result.addNode(dataset)

        for block in self.fileblocks.values():
            dataset.addNode(block.save())

        return result

    def write(self, filename):
        """
        _write_

        Write to file using name provided

        """
        handle = open(filename, 'w')
        improv = self.save()
        handle.write(improv.makeDOMElement().toprettyxml())
        handle.close()
        return
        

        
        
