#!/usr/bin/env python
# pylint: disable-msg=W0152
"""
_Action_

Definition of the Action Class

"""
__revision__ = "$Id: Action.py,v 1.1 2005/12/30 18:54:26 evansde Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "evansde@fnal.gov"

from IMProv.IMProvNode import IMProvNode
from ShREEK.ShREEKException import ShREEKException

class Action:
    """
    _Action_

    Base Class for a triggered Action definition
    and implementation within the ControlPoint
    System.
    Should be Subclassed to Implement specific
    behaviour

    """
    def __init__(self, actionName):
        self.name = actionName
        self.attrs = {}
        self.attrs['Name'] = self.name
        self.content = None


    def parseContent(self, chardata):
        """
        _parseContent_

        Parse the information provided in the
        self.content attribute, this is the
        chardata from the XML Element

        Override this method to handle the expected
        Chardata format and store it in the class
        for evaluation
        
        """
        pass

    def action(self, controlPoint):
        """
        _action_

        Perform the action required for this object based on
        the content of this object.
        References to the Executor and its component Threads can be retrieved
        from the control point reference.
        
        """
        pass
    
    def makeIMProv(self):
        """
        _makeIMProv_

        Make IMProv XML Node structure from this action
        """
        result = IMProvNode(self.__class__.__name__,
                            str(self.content).strip(),
                            **self.attrs)
        return result

    def __call__(self, controlPoint):
        """
        _Operator()_

        Execute the action by calling the parseContent method
        and then the action method.
        """
        try:
            self.parseContent(self.content)
        except StandardError, ex:
            msg = "Error Parsing Content of ControlPoint Action:\n"
            msg += "Action: %s Named: %s\n" % (
                self.__class__.__name__,
                self.name
                )
            msg += "Exception Details:\n"
            msg += str(ex)
            raise ShREEKException(
                msg, ClassInstance = self,
                ExecptionInstance = ex,
                ActionClass = self.__class__.__name__,
                ActionName = self.name,
                ControlPoint = controlPoint)

        
        
        try:
            self.action(controlPoint)
        except StandardError, ex:
            msg = "Error Invoking ControlPoint Action:\n"
            msg += "Action: %s Named: %s\n" % (
                self.__class__.__name__,
                self.name
                )
            msg += "Exception Details:\n"
            msg += str(ex)
            raise ShREEKException(
                msg, ClassInstance = self,
                ExecptionInstance = ex,
                ActionClass = self.__class__.__name__,
                ActionName = self.name,
                ControlPoint = controlPoint)
        return
