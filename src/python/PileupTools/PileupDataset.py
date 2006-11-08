#!/usr/bin/env python
"""

_PileupDataset_

Object that retrieves and contains a list of lfns for some pileup dataset.

Provides randomisation of access to the files, with two modes:

- No Overlap:  List of LFNs diminishes, and no pileup file is used twice per job
- Overlap:  Random selection of files from the list.


"""

import random

from IMProv.IMProvNode import IMProvNode
from IMProv.IMProvDoc import IMProvDoc
from IMProv.IMProvQuery import IMProvQuery
from IMProv.IMProvLoader import loadIMProvFile

from PhEDExInterface.DBSDLSToolkit import RemoteDBSDLSToolkit, DBSDLSToolkit



class PileupDataset(list):
    """
    _PileupDataset_

    List of files in a pileup dataset.

    Includes random access with and without overlap, and also a persistency
    mechanism to save state to
    a file if required
    
    """
    def __init__(self, dataset, filesPerJob, overlap = True):
        list.__init__(self)
        self.dataset = dataset
        self.filesPerJob = filesPerJob
        self.overlap = overlap


        

    def getPileupFiles(self):
        """
        _getPileupFiles_

        Get next randomised set of files. Returns list of filesPerJob lfns.
        If overlap is true, then the files are not removed from the list.
        If overlap is false, then the files are pruned from the list. When the list
        runs out of files, it will throw an exception

        """
        #  //
        # // Saftey Net
        #//
        if self.filesPerJob >= len(self):
            msg = "Not enough pileup files in dataset: \n  %s\n" % self.dataset
            msg += "Current Dataset Size: %s\n" % len(self)
            msg += "Files Requested: %s\n" % self.filesPerJob
            raise RuntimeError, msg
        
        files = []
        indices = []
        while len(indices) < self.filesPerJob:
            shot = random.randint(0, len(self)-1)
            if shot not in indices:
                indices.append(shot)
        for index in indices:
            files.append(self[index])

        if not self.overlap:
            for filename in files:
                self.remove(filename)
        return files
    
                
        
                           
        
        
        
        


    def loadLFNs(self, **dbsContacts):
        """
        Get the list of LFNs from the DBS

        """
        for i in self:
            self.remove(i)
    
        if len(dbsContacts) > 0:
            toolkit = RemoteDBSDLSToolkit(**dbsContacts)
        else:
            toolkit = DBSDLSToolkit()
            

            
        self.extend(toolkit.getDatasetFiles(self.dataset))
        return

    
    

    def save(self):
        """
        this object -> improvNode

        """
        result = IMProvDoc("PileupDataset")
        node = IMProvNode("Dataset", None,
                          Name = self.dataset,
                          FilesPerJob = str(self.filesPerJob))
        if self.overlap:
            node.attrs['Overlap'] = "True"
        else:
            node.attrs['Overlap'] = "False"
        
        result.addNode(node)
        for lfn in self:
            node.addNode(IMProvNode("LFN", lfn))
        return result
                           
            

    def load(self, improvNode):
        """
        node -> this Object
        
        """
        datasetQ = IMProvQuery("/PileupDataset/Dataset[attribute(\"Name\")]")
        fpjQ = IMProvQuery("/PileupDataset/Dataset[attribute(\"FilesPerJob\")]")
        lfnQ = IMProvQuery("/PileupDataset/Dataset/LFN[text()]")
        overlapQ = IMProvQuery("/PileupDataset/Dataset[attribute(\"Overlap\")]")
        datasetName = str(datasetQ(improvNode)[-1])
        fpj = int(fpjQ(improvNode)[-1])
        lfns = lfnQ(improvNode)
        olap = str(overlapQ(improvNode)[-1])
        print lfns

        self.dataset = datasetName
        self.filesPerJob = fpj
        if olap == "True":
            self.overlap = True
        else:
            self.overlap = False

        self.extend(lfns)
        return
    

    def read(self, filename):
        """
        _read_

        load file -> this object
        
        """
        node = loadIMProvFile(filename)
        self.load(node)
        return
    


    def write(self, filename):
        """
        _write_

        this object -> file

        """
        handle = open(filename, 'w')
        handle.write(self.save().makeDOMDocument().toprettyxml())
        handle.close()
        return

if __name__ == '__main__':
    
    dataset = "/MC-110-os-minbias/SIM/CMSSW_1_1_0-GEN-SIM-1161611489"
    dbsInfo = {
        "DBSAddress":"MCLocal_1/Writer",
        "DBSURL" :"http://cmsdbs.cern.ch/cms/prod/comp/DBS/CGIServer/prodquery",
        "DBSType" :"CGI",
        "DLSType":"DLS_TYPE_MYSQL",

        "DLSAddress": "lxgate10.cern.ch:18081",

        }
    
    
    pd = PileupDataset(dataset, 2, False)
    pd.loadLFNs(**dbsInfo)
    pd.write("TestPU.xml")
    
    
    for i in range(0,100):
        pud = PileupDataset(dataset, 2)
        pud.read("TestPU.xml")
        print pud
        print pud.getPileupFiles()
        pud.write("TestPU.xml")
