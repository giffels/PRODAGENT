#!/usr/bin/env python
# pylint: disable-msg=W0613
"""
_DirMetaBroker_

DirMetaBroker module containing specialisation of MetaBroker interface
for dealing with directory structures.
"""
__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: DirMetaBroker.py,v 1.1 2005/12/30 18:51:37 evansde Exp $"

import os

from MB.MetaBroker import MetaBroker
from MB.FileMetaBroker import FileMetaBroker
from MB.MBException import MBException


class DirMetaBroker(MetaBroker):
    """
    _DirMetaBroker_

    Specialisation of MetaBroker to deal with a directory
    Can be used to model arbitrary directory structures
    that may be distributed accross nodes
    """
    _DMBInitFields = [ 'DirName',
                       'Alias',
                       'EnvVarName'
                       ]
    
    def __init__(self, **args):
        MetaBroker.__init__(self)
        for item in self._DMBInitFields:
            self.setdefault(item, None)
            
        self._SetHandlers['DirName'] = self._MBSetBaseName
        self._SetHandlers['EnvVarName'] = self._DMBSetEnvVarName
        
        self._GetHandlers['DirName'] = self._DMBGetDirName
        self._GetHandlers['PathName'] = self._DMBGetPathName
        self._GetHandlers['AbsName'] = self._DMBGetAbsName
        
        
        for key, val in args.items():
            self[key] = val


    def addSubdir(self, dmbInstance):
        """
        _addSubdir_

        Add a subdirectory to this dir by adding
        another DMB instance to it.

        Args --

        - *dmbInstance* : DMB Instance representing the
        subdirectory

        """
        if not isinstance(dmbInstance, DirMetaBroker):
            msg = "Non DirMetaBroker Object added as subdir"
            raise MBException(
                msg,
                ModuleName = "MetaBroker.DirMetaBroker",
                ClassName = "DirMetaBroker",
                MethodName = "addSubdir",
                BadObject = dmbInstance,
                )

        self.addChildMetaBroker(dmbInstance['BaseName'],
                                dmbInstance)
        return

    def addFile(self, fmbInstance):
        """
        _addFile_

        Add a file to this directory by adding a
        FileMetaBroker instance to it

        Args --

        - *fmbInstance* : FMB instance representing the
        file to be added to this directory

        """
        if not isinstance(fmbInstance, FileMetaBroker):
            msg = "Non FileMetaBroker Object added as file"
            raise MBException(
                msg,
                ModuleName = "MetaBroker.DirMetaBroker",
                ClassName = "DirMetaBroker",
                MethodName = "addFile",
                BadObject = fmbInstance,
                )

        self.addChildMetaBroker(fmbInstance['BaseName'],
                                fmbInstance)
        return


    def dirs(self):
        """
        return a list of subdirectories
        """
        results = []
        for child in self.children():
            if isinstance(child, DirMetaBroker):
                results.append(child)
        return results

    def files(self):
        """
        return a list of files in this dir
        """
        results = []
        for child in self.children():
            if isinstance(child, FileMetaBroker):
                results.append(child)
        return results
        
        


    def _DMBGetDirName(self):
        """
        Get Method for DirName
        """
        return dict.__getitem__(self, "BaseName")

    def _DMBGetPathName(self):
        """
        If this dir has a parent then get the name from
        the parents
        """
        if self._Parent == None:
            return dict.__getitem__(self, 'PathName')
        
        if dict.__getitem__(self, "EnvVarName") != None:
            result = os.path.join(
                self._Parent['PathName'],
                dict.__getitem__(self, "EnvVarName")
                )
        else:
            
            result = self._Parent['PathName'],
        return result
    
    def _DMBGetAbsName(self):
        """
        Get Method for AbsName
        """
        if self._Parent == None:
            return self._MBGetAbsName()

        absname = ''
        if self._Parent['AbsName'] != None:
            absname = self._Parent['AbsName']
        basename = ''
        if self['BaseName'] != None:
            basename = self['BaseName']
        result = os.path.join(
            absname,
            basename,
            )
        return result
    
    def _DMBSetEnvVarName(self, value, dep=False):
        """
        Set Method for EnvVarName
        """
        if value == None:
            dict.__setitem__(self, "EnvVarName", value)
            return
        if not value.startswith("$"):
            value = "$%s" % value
        dict.__setitem__(self, "EnvVarName", value)
            
