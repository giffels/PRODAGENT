#!/usr/bin/env python
"""
_TaskObject_

Dictionary based object to represent an executable task in a generic way.

The object also supports a tree like structure to model dependencies
in execution order

"""
import os
from TaskObjects.Environment import Environment
from TaskObjects.StructuredFile import StructuredFile
from TaskObjects.Directory import Directory
from IMProv.IMProvDoc import IMProvDoc
from IMProv.IMProvNode import IMProvNode
from ShREEK.ShREEKTask import ShREEKTask


class TaskObject(dict):
    """
    _TaskObject_

    Dictionary based tree node container to represent an executable task
    in a generic in memory structure

    Default Keys:

    - *Name* : The name of this TaskObject

    - *Environment* : An instance of TaskObjects.Environment to contain the
    environment settings in a shell independent way

    - *Directory* : A DirMetaBroker object that models the Task directory.

    - *StructuredFiles* : A list of keys that contain StructuredFile
    objects to be written out into the TaskObject directory

    - *IMProvDocs* : A list of keys that contain IMProvDoc instances
    representing XML files that can be written out to the task directory

    - *ShREEKTask* : A ShREEKTask representing the executable for this
    TaskObject

    """
    def __init__(self, taskObjectName):
        dict.__init__(self)
        self.children = []
        self.parent = None
        self.setdefault("Name", taskObjectName)
        self.setdefault("Environment", Environment())
        self.setdefault("Directory" , Directory(taskObjectName))
        self.setdefault("IMProvDocs", [])
        self.setdefault("ShREEKTask", ShREEKTask(taskObjectName) )
        self.setdefault("StructuredFiles", [])
        
    def __call__(self, operator):
        """
        _operator()_

        Recursively call the operator provided on this task object
        and then its children

        """
        operator(self)
        for child in self.children:
            child(operator)
        return
        

    def addChild(self, taskObject):
        """
        _addChild_

        Add a TaskObject instance as a child of this object

        """
        if not isinstance(taskObject, TaskObject):
            msg = "Attempted to add Non TaskObject instance as child\n"
            msg += "of TaskObject\n"
            raise RuntimeError, msg
        self.children.append(taskObject)
        taskObject.parent = self
        if self['ShREEKTask'] != None:
            childShreekTask = taskObject.get("ShREEKTask", None)
            if childShreekTask != None:
                self['ShREEKTask'].addChild(childShreekTask)
            
        return


    def improvNode(self):
        """
        _improvNode_

        Generate a representation of this node and its
        children as an improv node tree to aid visualisation

        THIS IS NOT A PERSISTENCY MECHANISM
        """
        node = IMProvNode("TaskObject", None, Name = self['Name'])
        children = IMProvNode("Children")
        for child in self.children:
            children.addNode(child.improvNode())
        node.addNode(children)
        return node
    

    def addEnvironmentVariable(self, varname, *values):
        """
        _addEnvironmentVariable_

        Add an environment Variable to the Environment instance
        for this object.

        Args --

        - *varname* : Variable name

        - *values* : list of values to set, if list is greater than
        1 element, it is treated as a path like variable

        """
        self['Environment'].addVariable(varname, *values)
        return

    def addStructuredFile(self, filename):
        """
        _addStructuredFile_

        Add a new StructuredFile instance to this TaskObject.
        This will instantiate a new StructuredFile instance and
        attempt to add it to the TaskObject using the filename
        key. It will fail if this key already exists.
        The StructuredFile will be added to the list of StructuredFiles.
        The new StructuredFile is returned so that it can be populated.

        """
        if self.has_key(filename):
            msg = "Duplicate Key added as StructuredFile:\n"
            msg += "Key %s already exists." % filename
            raise RuntimeError, msg
        newFile = StructuredFile(filename)
        self[filename] = newFile
        self['StructuredFiles'].append(filename)
        return newFile

    def addIMProvDoc(self, docname):
        """
        _addIMProvDoc_

        create a new IMProvDoc instance and add it to this task object.
        The docname is the name of the document key within the task object
        and it will be saved as docname.xml in the task directory.

        The new document object is returned so that it can be manipulated
        If docname is already a key, an exception is raised.
        """
        if self.has_key(docname):
            msg = "Duplicate Key added as IMProvDoc:\n"
            msg += "Key %s already exists." % docname
            raise RuntimeError, msg
        newDoc = IMProvDoc(docname)
        self[docname] = newDoc
        self['IMProvDocs'].append(docname)
        return newDoc
        
        
    def attachFile(self, filename, targetName = None):
        """
        _attachFile_

        Attach an existing file to this TaskObject so that it can
        be added to the Task directory when concretized.
        
        
        
        """
        newFile = self['Directory'].addFile(filename, targetName)
        return
    
    

        
        
        
