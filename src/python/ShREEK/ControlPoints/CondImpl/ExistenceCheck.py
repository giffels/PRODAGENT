#!/usr/bin/env python
"""
_ExistenceCheck_

Conditional implementation for testing file
existence
"""

import os

from ShREEK.ControlPoints.Conditional import Conditional
import ShREEK.ControlPoints.ControlPointFactory as Factory


class ExistenceCheck(Conditional):
    """
    _ExistenceCheck_

    os.path.exists check for files.

    Usage:

    <ExistenceCheck>
       file1.dat
       ...
       fileN.dat
    </ExistenceCheck>

    will return True is all file exist , otherwise False
    """
    def __init__(self):
        Conditional.__init__(self)
        self._SupportsChildren = False
        self._Files = []

    def parseContent(self, content):
        """
        split the content by whitespace/newline
        into a list of files
        """
        self._Files = []
        filelist = content.split()
        for item in filelist:
            value = item.strip()
            if len(value) > 0:
                self._Files.append(value)
        return

    def evaluate(self, controlPoint):
        """
        _evaluate_

        Check Existence of each file in File
        List and return True if they all exist
        """
        result = True
        for item in self._Files:
            item = os.path.expandvars(item)
            if not os.path.exists(item):
                result = False
        return result


Factory.registerConditional(ExistenceCheck)
