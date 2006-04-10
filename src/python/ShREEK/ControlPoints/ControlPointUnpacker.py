#!/usr/bin/env python
"""
_ControlPointUnpacker_

Tool to convert an IMProvNode structure containing a serialised control
point into a ControlPoint instance

"""
__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: ControlPointUnpacker.py,v 1.1 2005/12/30 18:54:26 evansde Exp $"
__author__ = "evansde@fnal.gov"


from IMProv.IMProvOperator import IMProvOperator
from ShREEK.ControlPoints.ControlPoint import ControlPoint
from ShREEK.ControlPoints.Conditional import Conditional
from ShREEK.ControlPoints.ControlPointFactory import newConditional
from ShREEK.ControlPoints.ControlPointFactory import newAction
from ShREEK.ShREEKException import ShREEKException


class ControlPointUnpacker(IMProvOperator):
    """
    _ControlPointUnpacker_

    Convert a serialised ControlPoint from an IMProvNode tree to
    a ControlPoint instance

    """
    def __init__(self):
        self._Stack = [ControlPoint()]
        self._InConditional = False
        self._InParams = False
        self._InAction = False


    def stackTop(self):
        """get top of parse stack"""
        return self._Stack[-1]


    def startNode(self, nodename, nodeRef):
        """
        _startNode_

        Override start of node handler

        """

        if nodename == "Conditionals":
            #  //
            # // Entering a Conditionals doc, set 
            #//  flag to treat everything as conditional
            self._InConditional = True
            return
        
        if nodename == "Actions":
            #  //
            # // Entering an Actions Doc, treat everything as
            #//  an Action
            self._InAction = True
            return
        if nodename == "Parameters":
            self._InParams = True
            return
        if self._InParams:
            newParam = str(nodename)
            paramValue = nodeRef.attrs.get("Value", None)
            if paramValue == None:
                return
            self._Stack[0].addParameter(newParam, str(paramValue))
            return

        if self._InConditional:
            #  // 
            # //  Treat Element as a Conditional
            #//
            newCond = self._MakeConditional(str(nodename))
            newCond.content = str(nodeRef.chardata)
            for key, val in nodeRef.attrs.items():
                newCond.attrs[str(key)] = str(val)

            top = self.stackTop()
            if isinstance(top, Conditional):
                top.addChild(newCond)
            if isinstance(top, ControlPoint):
                top.addConditional(newCond)
            self._Stack.append(newCond)
        if self._InAction:
            #  //
            # // treat element as Action
            #//
            action = self._MakeAction(str(nodename),
                                      str(nodeRef.attrs['Name']))
            action.content = str(nodeRef.chardata)
            for key, val in nodeRef.attrs.items():
                action.attrs[str(key)] = str(val)
            self._Stack[0].addAction(action)
        return

    def endNode(self, nodename, nodeRef):
        """
        _endNode_

        End of node handler
        """
        if nodename == "Conditionals":
            #  //
            # // End of Conditionals doc
            #//
            self._InConditional = False
            return
        if nodename == "Actions":
            #  //
            # // End of Actions Block
            #//
            self._InAction = False
            return
        if nodename == "Parameters":
            #  //
            # // End of Parameters block
            #//
            self._InParams = False
            return
        if self._InConditional:
            #  //
            # // If parsing conditionals decrement the stack
            #//
            self._Stack.pop()
            
        return
    
        
    def _MakeConditional(self, name):
        """
        _MakeConditional_

        Request a new Instance of the Conditional named name
        from the Conditional Factory. If it is not available,
        raise an Exception 
        """
        try:
            result = newConditional(name)
            return result
        except StandardError, ex:
            msg = "Failed to Create Conditional named: %s\n" % name
            msg += str(ex)
            raise ShREEKException(msg, ClassInstance = self)

    def _MakeAction(self, actionClass, actionName):
        """
        _MakeAction_

        Request a new Action instance of class actionClass
        from the Action Factory, instantiating it with the
        name actionName.
        If there is a problem, the XML file information is
        added to the Exception
        """
        try:
            result = newAction(actionClass, actionName)
            return result
        except StandardError, ex:
            msg = "Failed to Create Action named: %s\n" % actionName
            msg += "Of Class: %s\n" % actionClass
            msg += str(ex)
            raise ShREEKException(msg, ClassInstance = self)
        
    def result(self):
        """get the result"""
        return self._Stack[0]
