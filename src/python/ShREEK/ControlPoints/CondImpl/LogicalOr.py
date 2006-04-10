#!/usr/bin/env python
"""
_LogicalOr_

Conditional implementation for grouping several conditionals
and evaluating them. The result is a logical OR of each
conditionals result
"""

import os

from ShREEK.ControlPoints.Conditional import Conditional
import ShREEK.ControlPoints.ControlPointFactory as Factory



class LogicalOr(Conditional):
    """
    _LogicalOr_

    Or based operation for grouping other conditionals

    Usage:

    <LogicalOr>
      <Conditional1> ... </Conditional1>
      ...
      <ConditionalN> ... </ConditionalN>
    </LogicalOr>

    The result is a logical Or of the results of all the
    sub conditionals
    """
    def __init__(self):
        Conditional.__init__(self)
        self._SupportsChildren = True

    def evaluate(self, controlPoint):
        """
        Evaluate all children and return a logical
        Or of the results
        """
        result = None
        for child in self._Children:
            childResult = child.evaluate(controlPoint)
            if result == None:
                result = childResult
            else:
                result = result or childResult
        return result
    
Factory.registerConditional(LogicalOr)
