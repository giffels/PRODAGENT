#!/usr/bin/env python
"""
_Counter_

Conditional with a Maximum Counter.
When the conditional is run, it compares a
counter parameter in the ControlPoint to a maximum
value stored in the conditional attributes

"""

from ShREEK.ControlPoints.Conditional import Conditional
import ShREEK.ControlPoints.ControlPointFactory as Factory

class Counter(Conditional):
    """
    _Counter_

    Compare a counter value to a maximum value,
    trigger the OnSuccess Action if the counter is
    equal to or greater than the max, otherwise trigger
    the OnFail Action
    """
    def __init__(self):
        Conditional.__init__(self)
        self.attrs['Maximum'] = 5

    def setMaximum(self, maxValue):
        """
        set the max value for the counter
        """
        self.attrs["Maximum"] = maxValue

    
    def evaluate(self, controlPoint):
        """
        _evaluate_

        Check the Counter Parameter in the controlPoint.
        If the parameter does not exist, then add it, and
        set it to 1.
        Otherwise compare the value to the maximum and
        
        
        """
        params = controlPoint.getParameters()
        value = int(params.get("Counter", 1))
        maxVal = int(self.attrs["Maximum"])
        result = False
        if value >= maxVal:
            result = True
        controlPoint.addParameter("Counter", value + 1)
        return result
        
        
Factory.registerConditional(Counter)
 
