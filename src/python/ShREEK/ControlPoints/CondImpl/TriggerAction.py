#!/usr/bin/env python
"""
_TriggerAction_

Conditional that always returns true so that it triggers whatever
Sucess action is attached to it
"""


from ShREEK.ControlPoints.Conditional import Conditional
import ShREEK.ControlPoints.ControlPointFactory as Factory


class TriggerAction(Conditional):
    """
    _TriggerAction_

    Return True to always execute the success Action
    registered to this Conditional
    """
    
    def __init__(self):
        Conditional.__init__(self)
        self._SupportsChildren = False
        
    def evaluate(self, controlPoint):
        """
        _evaluate_

        return True to Trigger the Success Action
        """
        return True

Factory.registerConditional(TriggerAction)
 
