#!/usr/bin/env python
# pylint: disable-msg=W0152
"""
_Conditional_

Conditional Object definition

"""
__revision__ = "$Id: Conditional.py,v 1.1 2006/04/10 17:38:44 evansde Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "evansde@fnal.gov"

from ShREEK.ShREEKException import ShREEKException
from IMProv.IMProvNode import IMProvNode


class Conditional:
    """
    _Conditional_

    Base Class for Control Point Conditional Objects.
    This class should be inherited and specialised
    to evaluate controld point specifics
    """
    def __init__(self):
        self.name = self.__class__.__name__
        self.attrs = {}
        self.content = None
        self._Children = []
        self._SupportsChildren = True

    def addChild(self, conditional):
        """
        _addChild_

        Add a Conditional Instance as a child
        of this condition
        """
        if not self._SupportsChildren:
            msg = "This Conditional Does not support Children\n"
            raise ShREEKException(msg, ClassInstance = self)
        if not isinstance(conditional, Conditional):
            msg = "Object Not a Conditional Instance\n"
            msg += "addChild Argument must be a "
            msg += "Conditional Object Instance\n"
            raise ShREEKException(msg, ClassInstance = self)
        self._Children.append(conditional)
        return

    def successAction(self):
        """
        _successAction_

        Retrieve the On Success Action for this
        Conditional, return None if not set
        """
        return self.attrs.get("OnSuccess", None)

    def failAction(self):
        """
        _failAction_

        Retrieve the On Fail Action for this conditional,
        return None if not set
        """
        return self.attrs.get("OnFail", None)
    
    

    def __call__(self, controlPoint):
        """
        Operator () is used to evaluate the conditional
        for the controlPoint reference provided.

        This invokes the parseContent method
        and then calls evaluate to eval the Conditional
        Logic.  Based on success or failure, if an action
        is associated with this Conditional, the triggerAction
        call is made to the ControlPoint.

        The Boolean result of the evaluate method is returned
        """
        self.parseContent(self.content)
        result = self.evaluate(controlPoint)
        if result:
            #  //
            # // Result is True, check/invoke successAction 
            #//
            if self.successAction() != None:
                controlPoint.triggerAction(self.successAction(),
                                           self)
            return True
        #  //
        # // Result is False, check/invoke failAction
        #//
        if self.failAction() != None:
            controlPoint.triggerAction(self.failAction(),
                                       self)
        return False
        
        
    
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


    def evaluate(self, controlPoint):
        """
        _evaluate_

        Evaluate the Conditional, returning True or False.
        If an action is provided dispatch the call to
        the controlPoint.

        Override this method for the specific Conditional Logic
        """
        pass

    def makeIMProv(self):
        """
        _makeIMProv_

        Make IMProv XML Node structure from this conditional
        """
        result = IMProvNode(self.name, str(self.content).strip(), **self.attrs)
        for child in self._Children:
            result.addNode(child.makeIMProv())
        return result
