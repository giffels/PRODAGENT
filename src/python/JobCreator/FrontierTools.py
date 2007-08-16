#!/usr/bin/env python
"""
_FrontierTools_

Controls for Frontier Debugging and Diagnostic settings

"""
import os
import inspect
import logging
import JobCreator.RuntimeTools.RuntimeFrontierDiagnostic as RuntimeFrontier

class InsertFrontierTools:
    """
    _InsertFrontierTools_

    Enable Frontier Debugging if required

    """
    def __init__(self, active = False):
        self.active = active

    def __call__(self, taskObject):
        if not self.active:
            return
        if taskObject['Type'] != "CMSSW":
            return

        env = taskObject['Environment']
        env.addVariable('FRONTIER_LOG_LEVEL', "warning")
        env.addVariable('FRONTIER_LOG_FILE', "Frontier.log")
        
        
        srcfile = inspect.getsourcefile(RuntimeFrontier)
        taskObject.attachFile(srcfile)
        
        taskObject['PostTaskCommands'].append(
            "chmod +x RuntimeFrontierDiagnostic.py"
            )
        taskObject['PostTaskCommands'].append(
            "./RuntimeFrontierDiagnostic.py"
            )
        return
        
        
