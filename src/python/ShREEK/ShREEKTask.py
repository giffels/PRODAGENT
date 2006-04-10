#!/usr/bin/env python
"""
_ShREEKTask_

Tree Node construct representing a ShREEK Processing node.
Models tasks as a tree.
Contains start and end control point instances.
Can be serialised as XML.

"""
__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: ShREEKTask.py,v 1.1 2005/12/30 18:54:25 evansde Exp $"
__author__ = "evansde@fnal.gov"

from IMProv.IMProvNode import IMProvNode

from ShREEK.ShREEKException import ShREEKException
from ShREEK.ControlPoints.ControlPoint import ControlPoint
from ShREEK.ControlPoints.ControlPointUnpacker import ControlPointUnpacker


def intersection(list1, list2):
    """fast intersection of two lists"""
    intDict = {}
    list1Dict = {}
    for entry in list1:
        list1Dict[entry] = 1
    for entry in list2:
        if list1Dict.has_key(entry):
            intDict[entry] = 1
    return intDict.keys()

def listAllNames(shreekTask):
    """
    _listAllNames_
    
    Generate a top-descent based list of all node names for
    all nodes in this node tree. Traverse to the topmost node first
    and then recursively list all the names in the tree.
    
    """
    if shreekTask.parent != None:
        return listAllNames(shreekTask.parent)
    return  shreekTask.listDescendantNames()



class ShREEKTask:
    """
    _ShREEKTask_

    
    """

    def __init__(self, taskName, **attrs):
        self.attrs = {}
        self.attrs.setdefault("Name", taskName)
        self.attrs.setdefault("Executable", None)
        self.attrs.setdefault("Directory", None)
        self.attrs.setdefault("Active", True)
        self.attrs.update(attrs)
        self.children = []
        self.parent = None
        self.startControlPoint = ControlPoint()
        self.endControlPoint = ControlPoint()

    def taskname(self):
        """return task name"""
        return self.attrs['Name']

    def directory(self):
        """return execution dir"""
        return self.attrs['Directory']

    def executable(self):
        """return executable script"""
        return self.attrs['Executable']
    

    def addChild(self, childTask):
        """
        _addChild_

        Add a child task to this task.

        """
        if not isinstance(childTask, ShREEKTask):
            msg = "Non ShREEKTask object added as child:\n"
            msg += str(childTask)
            msg += "\naddChild Argument must be ShREEKTask instance\n"
            raise ShREEKException(msg, ClassInstance = self)

        dupes = intersection(listAllNames(self), listAllNames(childTask))
        if len(dupes) > 0:
            msg = "Duplicate Names already exist in parent task tree:\n"
            msg += "The following names already exist in the parent tree:\n"
            for dupe in dupes:
                msg += "  %s\n" % dupe
            msg += "Each ShREEKTask within the task tree must "
            msg += "have a unique name\n"
            raise ShREEKException(msg, ClassInstance = self)
        self.children.append(childTask)
        childTask.parent = self
        return

    

    def listDescendantNames(self, result = None):
        """
        _listDescendantNames_

        return a list of all names of nodes below this node
        recursively traversing children
        """
        if result == None:
            result = []
        result.append(self.attrs['Name'])
        for child in self.children:
            result = child.listDescendantNames(result)
        return result
                      
    def findTask(self, taskname):
        """
        _findTask_

        Match a task by name in this task or its descendants

        """
        if self.taskname() == taskname:
            return self
        for child in self.children:
            match = child.findTask(taskname)
            if match != None:
                return match
        return None
    
        

    def makeIMProv(self):
        """
        _makeIMProv_

        Make IMProvNode structure representing this object
        and its children in a recursive manner
        """
        thisNode = IMProvNode("ShREEKTask", None, **self.attrs)
        startContPoint = IMProvNode("StartControlPoint")
        startContPoint.addNode(self.startControlPoint.makeIMProv())
        endContPoint = IMProvNode("EndControlPoint")
        endContPoint.addNode(self.endControlPoint.makeIMProv())
        thisNode.addNode(startContPoint)
        thisNode.addNode(endContPoint)
        #  //
        # // Include children recursively
        #//
        for child in self.children:
            thisNode.addNode(child.makeIMProv())
        return thisNode
    
        

    def __str__(self):
        """string repr"""
        return str(self.makeIMProv())


    def populate(self, improvNode):
        """
        _populate_

        Populate self from a serialised ShREEKTask as an improvNode instance

        """
        for attr, value in improvNode.attrs.items():
            self.attrs[str(attr)] = str(value)
        if self.attrs['Active'] in ("True", "TRUE", "true", "1"):
            self.attrs['Active'] = True
        if self.attrs['Active'] in ("False", "FALSE", "false", "0"):
            self.attrs['Active'] = False

        for node in improvNode.children:
            if node.name == "ShREEKTask":
                newTask = ShREEKTask(str(node.attrs['Name']))
                self.addChild(newTask)
                newTask.populate(node)
            elif node.name == "StartControlPoint":
                # extract ControlPoint instance from IMProvNode
                # assign to self.startControlPoint
                unpacker = ControlPointUnpacker()
                unpacker(node.children[0])
                self.startControlPoint = unpacker.result()
                
            elif node.name == "EndControlPoint":
                # extract ControlPoint instance from IMProvNode
                # assign to self.endControlPoint
                unpacker = ControlPointUnpacker()
                unpacker(node.children[0])
                self.endControlPoint = unpacker.result()
                
                
        return
            
        
        
        
        


    
