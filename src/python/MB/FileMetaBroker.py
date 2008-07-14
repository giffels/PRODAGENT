#!/usr/bin/env python
"""
_FileMetaBroker_

FileMetaBroker module containing specialisation of MetaBroker interface
for dealing with files.
"""
__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: FileMetaBroker.py,v 1.1 2005/12/30 18:51:37 evansde Exp $"

from MB.MetaBroker import MetaBroker
from MB.MBException import MBException


class FileMetaBroker(MetaBroker):
    """
    _FileMetaBroker_

    Specialisation of MetaBroker for a file
    Adds synonym for BaseName: FileName
    """
    _FMBInitFields = [ 'FileName',
                       'TargetFileName',
                       'SourceFileName']
    
    def __init__(self, **args):
        MetaBroker.__init__(self)
        for item in self._FMBInitFields:
            self.setdefault(item, None)

      
            
        self._SetHandlers['FileName'] = self._MBSetBaseName
        self._GetHandlers['FileName'] = self._FMBGetFileName
        self._SetHandlers['TargetFileName'] = self._MBSetTargetBaseName
        self._GetHandlers['TargetFileName'] = self._FMBGetTargetFileName
        self._SetHandlers['SourceFileName'] = self._MBSetSourceBaseName
        self._GetHandlers['SourceFileName'] = self._FMBGetSourceFileName
        
        
        for key, val in args.items():
            self[key] = val


    def addChild(self, fmbInstance):
        """
        Add a child File associated with this file. This
        means that the file will be moved around along with the parent
        file
        """
        if not isinstance(fmbInstance, FileMetaBroker):
            msg = "Non FileMetaBroker Object added as Child"
            raise MBException(
                msg,
                ModuleName = "MetaBroker.FileMetaBroker",
                ClassName = "FileMetaBroker",
                MethodName = "AddChild",
                BadObject = fmbInstance,
                )

        self.addChildMetaBroker(fmbInstance['BaseName'],
                                fmbInstance)
        return


    

        

    def _FMBGetFileName(self):
        """
        Get Method for FileName,
        """
        return self['BaseName']

    def _FMBGetTargetFileName(self):
        """
        Get Method for TargetFileName
        """
        return self['TargetBaseName']

    def _FMBGetSourceFileName(self):
        """
        Get Method for SourceFileName
        """
        return self['SourceBaseName']


    def __repr__(self):
        return "<FileMetaBroker Instance: %s>" % self['RemoteName']

    
        
