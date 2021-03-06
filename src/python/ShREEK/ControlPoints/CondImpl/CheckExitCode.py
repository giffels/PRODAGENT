#!/usr/bin/env python
"""
_CheckExitCode_

Conditional to check the exit code by reading the exitCode file
provided as an argument to the conditional.

"""

import os

from ShREEK.ControlPoints.Conditional import Conditional
import ShREEK.ControlPoints.ControlPointFactory as Factory



class CheckExitCode(Conditional):
    """
    _CheckExitCode_

    Reads a specified file to extract the exit code of an executable.
    If the executable exited with zero, then the conditional will
    evaluate to True.
    If the Exit Code cannot be found, or is non-zero, then
    this will evaluate as false.
    """
    def __init__(self):
        Conditional.__init__(self)
        self.attrs['ExitCodeFile'] = "exit.status"

    def setExitCodeFile(self, filename):
        """
        _setExitCodeFile_

        Set the name of the file that contains the exit code 
        """
        self.attrs['ExitCodeFile'] = filename
        return
        

    def evaluate(self, controlPoint):
        """
        _evaluate_

        Find the ExitCode file and read it, and attempt to
        evaluate the exit code.
        
        """
        exitCodeFile = str( self.attrs['ExitCodeFile'])
        if not os.path.exists(exitCodeFile):
            #  //
            # // File not found
            #//
            return False
        #  //
        # // read file
        #//
        handle = open(exitCodeFile, 'r')
        content = handle.read()
        handle.close()
        content = content.strip()
        try:
            exitCode = int(content)
        except ValueError:
            return False

        if exitCode:
            #  //
            # // Non zero
            #//
            return False
        #  //
        # // Zero
        #//
        return True
    
Factory.registerConditional(CheckExitCode)
