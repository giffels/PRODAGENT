#!/usr/bin/env python
# pylint: disable-msg=W0613
"""
Utilities to build a DirMetaBroker structure from
a path.

Objects --

- *_makeFileList* : use os.walk to generate a list of all files
in a directory structure

- *_makeDirList*  : use os.walk to generate a list of all dirs in
a directory structure

- *BuildDMBFromPath* : create a DMB from a directory structure
located at a path area


"""
__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: BuildDMBFromPath.py,v 1.1 2005/12/30 18:51:39 evansde Exp $"



import os


from MB.DirMetaBroker import DirMetaBroker
from MB.FileMetaBroker import FileMetaBroker


def visitDir(arg, dirname, names):
    """
    Visit method for os.path.walk to make it look like
    os.walk from python 2.3.
    """
    dirs = []
    files = []
    for name in names:
        if os.path.isdir(os.path.join(dirname, name)):
            dirs.append(name)
        else:
            files.append(name)
    result = (dirname, dirs, files)
    arg.append(result)

def osWalk(dirName):
    """
    _osWalk_

    Temp version of os.walk that uses os.path.walk for
    compatibility with older python versions.
    This can be replaced with os.walk when people
    finally realise that python 2.3 is better
    """
    result = []
    os.path.walk(dirName, visitDir, result)
    return result



def buildDMBFromPath(startPath):
    """
    _buildDMBFromPath_

    Create a DMB Structure from the directory provided.

    """
    currentDMB = DirMetaBroker(AbsName = startPath)
    dirContents = osWalk(startPath)[0]
    dirPath = dirContents[0]
    subdirs = dirContents[1]
    files = dirContents[2] 
    for fileName in files:
        childFMB = FileMetaBroker(
            FileName = fileName,
            SourceAbsName = os.path.join(dirPath, fileName),
            TransportMethod = 'cp')
        currentDMB.addFile(childFMB)
    for dirName in subdirs:
        newDir = os.path.join(dirPath, dirName)
        childDMB = buildDMBFromPath(newDir)
        currentDMB.addSubdir(childDMB)
        
    return currentDMB





    
    
    




    


