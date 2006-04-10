#!/usr/bin/env python
"""
Module containing utils to add paths to MetaBroker instances
recursively

"""
__version__ = "$Version$"
__revision__ = "$Id: AddPath.py,v 1.1 2005/12/30 18:51:39 evansde Exp $"


import os
import re


from MB.DirMetaBroker import DirMetaBroker
from MB.FileMetaBroker import FileMetaBroker

_PathSplitter = re.compile('/')

def splitPath(path):
    """
    Regexp path splitting function. Ensures splits are
    filtered.
    """
    split = _PathSplitter.split(path)
    result = []
    for item in split:
        stripped = item.strip()
        if len(stripped) > 0 :
            result.append(stripped)
    return result
 
def mergePath(dirlist):
    """
    Path construction utility.
    """
    joiner = "%s" % os.pathsep
    result = joiner.join(dirlist)
    return result



def addPath(dmbInstance, newPath):
    """
    _addPath_

    Add a path structure to a DMB Instance, for example,
    if you have a DMB and want to add the path dir1/dir2/dir3
    to it as a set of subdirs, then this method will create
    and add the required DMB Structure. Directories which already
    exist are not added. The last DMB in the path is returned

    Args --

    - *dmbInstance* : The DMB instance that the path with
    be added to.

    - *newPath* : String representing the path to be added to the DMB

    Returns --

    - *DirMetaBroker* : DMB Instance pointing to the last
    directory added at the bottom of the path
    
    """
    pathlist = splitPath(newPath)
    currentDir = dmbInstance["DirName"]
    currentdmb = dmbInstance
    for dirName in pathlist:
        currentDir = mergePath([currentDir, dirName])
        if dirName not in dmbInstance.childNames():
            newdmb = DirMetaBroker(DirName = dirName)
            currentdmb.addSubdir(newdmb)
            currentdmb = newdmb
        else:
            currentdmb = dmbInstance.childrenMap()[dirName]
    return currentdmb 




def addFilePath(mbInstance, filePath):
    """
    _addFilePath_

    Add a file to the DMB instance creating
    directories if needed and adding the file
    as an FMB in the appropriate place.
    The FMB contains a pointer to the Source file and
    sets the TransportMethod to be local by default.
    Since the resulting FMB is returned by reference, it
    can be manipulated change the way the source is accessed.

    Returns --

    - *FileMetaBroker* : returns the FileMetaBroker created to
    represent the file.
    
    """
    dirname = os.path.dirname(filePath)
    fileName = os.path.basename(filePath)

    currentDmb = addPath(mbInstance, dirname)
    #  //
    # // Now create and add FMB to currentDMB
    #//
    fmb = FileMetaBroker(SourceAbsName = filePath,
                         FileName=fileName,
                         TransportMethod = "local")
    currentDmb.addFile(fmb)
    return fmb

    
