#!/usr/bin/env python
"""
_PrintMessage_

Message generator Action for ControlPoints,
Useful for generating debug information or checking the
Flow through the ControlPoint.

"""
__revision__ = "$Id: PrintMessage.py,v 1.1 2005/12/30 18:54:27 evansde Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "evansde@fnal.gov"

from ShREEK.ControlPoints.Action import Action
import ShREEK.ControlPoints.ControlPointFactory as Factory



class PrintMessage(Action):
    """
    Print the Chardata contained in the Action Defn
    to Stdout.
    
    """
    def action(self, controlPoint):
        """
        _action_

        Implementation of PrintMessage simply prints the
        content contained in the Action
        """
        print self.content
        
    
        
    
Factory.registerAction(PrintMessage)
