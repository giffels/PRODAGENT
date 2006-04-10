#!/usr/bin/env python
"""
_TaskDirBuilder_

Traverse a structure of TaskObjects and build all the directories
for the TaskObject Directory attributes, pulling in any files that
are attached to it

"""
__version__ = "$Revision: 1.2 $"
__revision__ = "$Id: TaskDirBuilder.py,v 1.2 2006/02/15 20:55:05 evansde Exp $"
__author__ = "evansde@fnal.gov"

import os
from MB.dmb_tools.DMBBuilder import DMBPopulator

class FlatTaskDirBuilder:
    """
    _FlatTaskDirBuilder_

    Build a flat directory structure for the task object dirs.
    All dirs are created in the targetDirectory provided, rather than
    recursively like the taskObject tree structure

    Location of the physical directory is set via the PathName variable in the
    Directory DMB instance
    
    """
    def __init__(self, targetDirectory):
        self.targetDir = targetDirectory
        
        
        

    def __call__(self, taskObjectRef):
        """
        _operator()_

        Operate on a TaskObject using its Directory attribute
        to generate a concrete dir in the targetDir area specified by
        this object
        
        """
        dmb = taskObjectRef['Directory']
        if dmb == None:
            #  //
            # // If DMB is None, implies that there is no dir to
            #//  be created for this object.
            return 
        dmb['QueryMethod'] = 'local'

        
        #  //
        # // Create and populate the task directory
        #//
        builder = DMBPopulator(self.targetDir)
        dmb.execute(builder)
        dmb['PathName'] = self.targetDir
        taskObjectRef['RuntimeDirectory'] = "%s" % dmb['BaseName']
        taskObjectRef['ShREEKTask'].attrs['Directory'] = "./%s" % (
            dmb['BaseName'],
            )
        return
        


class PathAccumulator:
    """
    _PathAccumulator_

    Utility Object

    Upwards traversal of a TaskObject Tree to construct a path
    based on parentage relations

    """
    def __init__(self, basePath):
        self.path = basePath
        self.steps = []
        
    def __call__(self, taskObject):
        """
        _operator()_

        Act on a TaskObject instance and traverse upwards through its parents
        to build a path to the instance using the same tree structure
        as the TaskObject Tree

        """
        dmb = taskObject['Directory']
        if dmb == None:
            #  //
            # // If DMB is None, implies that there is no dir to
            #//  be created for this object.
            return 
        
        self.steps.insert(0, dmb['BaseName'])

        if taskObject.parent != None:
            self(taskObject.parent)
            
        return

    def result(self):
        """
        _result_

        Extract the resulting path after operation

        """
        result = self.path
        for item in self.steps:
            result = os.path.join(result, item)
        return result

    def relativePath(self):
        """
        _relativePath_

        Extract the relative path not including the base path provided
        in the ctor
        """
        result = "."
        for item in self.steps:
            result = os.path.join(result, item)
        return result
        
    

class TreeTaskDirBuilder:
    """
    _TreeTaskDirBuilder_

    Build a directory tree mimicking the TaskObject Tree structure.

    """
    def __init__(self, targetDirectory):
        self.targetDir = targetDirectory
        

    def __call__(self, taskObjectRef):
        """
        _operator()_

        Operate on a TaskObject using its Directory attribute
        to generate a concrete dir in the targetDir area specified by
        this object
        
        """
        dmb = taskObjectRef['Directory']
        if dmb == None:
            #  //
            # // If DMB is None, implies that there is no dir to
            #//  be created for this object.
            return 
        dmb['QueryMethod'] = 'local'


        accum = PathAccumulator(self.targetDir)
        accum(taskObjectRef)
        targetPath = accum.result()
        
        #  //
        # // Create and populate the task directory
        #//
        builder = DMBPopulator(os.path.dirname(targetPath))
        dmb.execute(builder)
        dmb['PathName'] = os.path.dirname(targetPath)
        taskObjectRef['RuntimeDirectory'] = accum.relativePath()
        taskObjectRef['ShREEKTask'].attrs['Directory'] = accum.relativePath()
        return
