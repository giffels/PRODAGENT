#!/usr/bin/env python
# pylint: disable-msg=W0613
"""
_DMBBuilder_

Utility to build a directory structure on disk from a
DMB instance


"""
__version__ = '$Version$'
__revision__ = "$Id: DMBBuilder.py,v 1.1 2005/12/30 18:51:39 evansde Exp $"
__author__ = "evansde@fnal.gov"


import os
import popen2
from MB.Energise import energise
from MB.MBException import MBException

class DMBBuilder:
    """
    Operator to recursively traverse a DMB structure
    and build the directory structure in the
    target location
    """
    def __init__(self, targetPath = '.', defaultTransport = 'cp'):
        self.targetPath = targetPath
        self.defaultTransport = defaultTransport
        self.dirsBuilt = []
        if self.targetPath == None:
            msg = "DMBBuilder instantiated with None as target Path\n"
            msg += "A Non-None value must be provided to create dirs\n"
            raise MBException(msg, ClassInstance = self)
        
    def __call__(self, dmb, *args, **keywords):
        if dmb.__class__.__name__ != "DirMetaBroker":
            return
   
        absname = dmb['AbsName']
        if absname != None:
            pathToBuild = os.path.join(
                self.targetPath,
                dmb['AbsName'],
                )
        else:
            pathToBuild = self.targetPath
        
        self.dirsBuilt.append(pathToBuild)
        if not os.path.exists(pathToBuild):
            os.makedirs(pathToBuild)


            
        
class DMBPopulator:
    """
    Operator to recursively traverse a DMB structure
    and build the directory structure in the
    target location.
    All FMBs found in a directory are staged in using the
    MB transport mechanisms.
    """
    def __init__(self, targetPath = '.', defaultTransport = 'cp'):
        self.targetPath = targetPath
        self.defaultTransport = defaultTransport
        self.dirsBuilt = []
        self.filesAdded = []
        self.filesFailed = []
        self.failures = {}
        if self.targetPath == None:
            msg = "DMBPopulator instantiated with None as target Path\n"
            msg += "A Non-None value must be provided to create dirs\n"
            raise MBException(msg, ClassInstance = self)
            

    def __call__(self, dmb, *args, **keywords):
        """
        _operator()_

        Define the action of this operator on a MetaBroker.
        This operator only acts on DMB instances to create directories
        and then it traverses the list of FMBs attached to that directory
        and stages in files that have source information
        """
        if dmb.__class__.__name__ != "DirMetaBroker":
            #  //
            # // Only handle DMB Instances
            #//
            return
        #  //
        # // calculate the path to create
        #//
        absname = dmb['AbsName']
        if absname != None:
            pathToBuild = os.path.join(
                self.targetPath,
                dmb['AbsName'],
                )
        else:
            pathToBuild = self.targetPath
            
        #  //
        # // Build the path
        #//
        self.dirsBuilt.append(pathToBuild)
        

        if dmb['Source'] != None:
            #  //
            # // DMB has a Source so copy that in
            #//
            self.stageInDir(dmb['SourceAbsName'], pathToBuild)

        if not os.path.exists(pathToBuild):
            os.makedirs(pathToBuild)
        
        #  //
        # // Process FMBs attached as children to this DMB
        #//
        for item in dmb.files():
            self.populate(item, pathToBuild)
            
            

    def populate(self, fmb, targetPath):
        """
        _populate_

        Add the file pointed at by an FMB to the new directory structure.
        This requires that the FMB have a Source attribute.
        """
        if fmb.__class__.__name__ != "FileMetaBroker":
            return

       
        src =  fmb['SourceAbsName']
        if src == None:
            return
        
        fmb['PathName'] = targetPath
        tgtName = fmb['BaseName']
        if tgtName == None:
            fmb['BaseName'] = fmb['SourceBaseName']
            
        
        tgtMethod =  fmb['TransportMethod']
        if tgtMethod == None:
            fmb['TransportMethod'] = self.defaultTransport
            
        try:
            energise(fmb)
            self.filesAdded.append(fmb['AbsName'])
        except StandardError, excep:
            self.filesFailed.append(fmb['AbsName'])
            self.failures[fmb['AbsName']] = excep 
        return
        

    def stageInDir(self, dirPath, targetPath):
        """
        recursively stage in local DMB using cp -rf

        """
        command = "/bin/cp -rf %s %s" % (
            dirPath, targetPath
            )
        pop = popen2.Popen4(command)
        while pop.poll() == -1:
            exitCode = pop.poll()
        exitCode = pop.poll()
        if exitCode:
            self.filesFailed.append(dirPath)
        return
