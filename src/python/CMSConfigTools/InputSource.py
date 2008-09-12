#!/usr/bin/env python
"""
_InputSource_

Object to assist with manipulating the input source provided in a PSet

"""

from CMSConfigTools.Utilities import isQuoted

class InputSource:
    """
    _InputSource_

    Util for manipulating the InputSource within a cmsconfig object

    """
    def __init__(self, sourceDictRef):
        self.data = sourceDictRef
        self.sourceType = sourceDictRef['@classname'][2]

    def maxevents(self):
        """get value of MaxEvents, None if not set"""
        tpl = self.data.get("maxEvents", None)
        if tpl != None:
            return int(tpl[2])
        return None
    
    def setMaxEvents(self, maxEv):
        """setMaxEvents value"""
        self.data['maxEvents'] = ('int32', 'untracked', maxEv)

    def firstRun(self):
        """get firstRun value of None if not set"""
        tpl = self.data.get("firstRun", None)
        if tpl != None:
            return int(tpl[2])
        return None
        
    def setFirstRun(self, firstRun):
        """set first run number"""
        self.data['firstRun'] = ('uint32', 'untracked', firstRun)
        
    def fileNames(self):
        """ return value of fileNames, None if not provided """
        tpl = self.data.get("fileNames", None)
        if tpl != None:
            return tpl[2]
        return None

    def setFileNames(self, *fileNames):
        """set fileNames vector"""
        fnames = []
        for fname in fileNames:
            if not isQuoted(fname):
                fname = "\'%s\'" % fname
            fnames.append(fname)
        self.data['fileNames'] = ('vstring', 'untracked', fnames)
        
            
        


