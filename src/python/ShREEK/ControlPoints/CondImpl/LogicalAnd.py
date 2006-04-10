#!/usr/bin/env python
"""
_LogicalAnd_

Conditional implementation for grouping several conditionals
and evaluating them. The result is a logical AND of each
conditionals result
"""

import os

from ShREEK.ControlPoints.Conditional import Conditional
import ShREEK.ControlPoints.ControlPointFactory as Factory



class LogicalAnd(Conditional):
    """
    _LogicalAnd_

    And based operation for grouping other conditionals

    Usage:

    <LogicalAnd>
      <Conditional1> ... </Conditional1>
      ...
      <ConditionalN> ... </ConditionalN>
    </LogicalAnd>

    The result is a logical And of the results of all the
    sub conditionals
    """
    def __init__(self):
        Conditional.__init__(self)
        self._SupportsChildren = True

    def evaluate(self, controlPoint):
        """
        Evaluate all children and return a logical
        And of the results
        """
        result = True
        for child in self._Children:
            childResult = child.evaluate(controlPoint)
            result = result and childResult
        return result
    
Factory.registerConditional(LogicalAnd)
