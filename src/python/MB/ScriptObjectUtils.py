#!/usr/bin/env python
# pylint : disable-msg=W0613,W0613
"""
_ScriptObjectUtils_

Tools for adding MB objects to ScriptObjects

"""
__version__ = "$Revision: 1.1 $"
__revision__ = "$Version$"

from MB.dmb_tools.DMBBuilder import DMBBuilder
from scriptObjects.SOAttribute import registerAttributeType
from scriptObjects.SOAttribute import defaultDescription

def saveDMB(soRef, mdName):
    """
    _saveDMB_

    Save a DMB Object as part of a ScriptObject
    """
    obj = soRef.getAttributeValue(mdName, 'Object')
    target = soRef.getPersistentPath()
    builder = DMBBuilder(target)
    obj.execute(builder)
    sep = '/'
    targetDir = sep.join( [target, obj['DirName']] )
    #  //
    # // ToDo: include call to populate DMB
    #//  with FMBs if there are any
    #  //DMBBuilder also needs to handle 
    # // DMBs with Source arguments...
    #//
    mdLine = "%s MDType=DirMetaBroker DirName=%s\n" % (
        mdName,
        targetDir,
        )
        
    
    return mdLine
    
def loadDMB(soRef, mdName, mdLine=None):
    """
    _loadDMB_

    Load a Saved DMB

    To be implemented....
    """
    pass



def saveFMB(soRef, mdName):
    """placeholder method"""
    return ""

def loadFMB(soRef, mdName, mdLine=None):
    """placeholder method"""
    pass

    

def addDMB(scriptObject, dmbInstance, name = None):
    """
    _addDMB_

    Add a DMB instance to the ScriptObject
    """
    if name == None:
        name = dmbInstance['DirName']

    scriptObject.addItem(name, 'DirMetaBroker',
                         Object = dmbInstance)
    return


def addFMB(scriptObject, fmbInstance, name = None):
    """
    _addFMB_

    Add an FMB instance to the ScriptObject

    """
    if name == None:
        name = dmbInstance['BaseName']

    scriptObject.addItem(name, 'FileMetaBroker',
                         Object = dmbInstance)
    return


registerAttributeType(TypeName = "DirMetaBroker",
                      Schema = ["Object"],
                      SaveFunc = saveDMB,
                      LoadFunc = loadDMB,
                      InfoFunc = defaultDescription)
registerAttributeType(TypeName = "FileMetaBroker",
                      Schema = ["Object"],
                      SaveFunc = saveFMB,
                      LoadFunc = loadFMB,
                      InfoFunc = defaultDescription)
