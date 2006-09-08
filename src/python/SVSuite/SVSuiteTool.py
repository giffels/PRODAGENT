#!/usr/bin/env python
"""
_SVSuiteTool_

Object that invokes an SVSuite Tool, providing the proper environment
and managing the execution.
In case of an error during execution, an exception should be thrown


"""


import os
from SVSuite.Execute import executeWithFilter



class OvalFilter(list):
    def __init__(self):
        list.__init__(self)


    def __call__(self, outputChunk):
        lines = outputChunk.split("\n")
        for line in lines:
            if line.startswith("[OVAL]"):
                self.append(line)
        return
                

class SVSuiteTool:
    """
    _SVSuiteTool_

    Object to manage and execute an SVSuite Tool script

    """
    def __init__(self, toolName):
        self.tool = toolName
        self.environment = {}
        self.swSetupCommand = None
        self.filter = OvalFilter()
        

    def __call__(self):
        """
        _operator()_

        Generate the environment, setup command and create
        a command to run the tool.

        Run the command, and return the exit status.

        If the tool fails, throw an exception

        """
        command = \
        """
        #!/bin/sh\n
        echo "===========SVSuite Invoking Tool: %s=============="
        """ %  self.tool

        command += "export PRODAGENT_THIS_TASK_DIR=`pwd`\n"
        
        for key, val in self.environment.items():
            command += "echo \"Setting %s=%s\"\n" % (key, val)
            command += "export %s=%s\n" % (key, val)
            
        command += "\n%s\n" % self.swSetupCommand

        command += "\nexport PATH=$PATH:$SVSUITE_BIN_DIR\n"
        
        command += "\n"

        command += "%s &\n" % self.tool
        command += "PROCID=$!\n"
        command += "echo $PROCID > process_id\n"
        command += "wait $PROCID\n"
        command += "EXIT_STATUS=$?\n"
        
        command += "echo \"==============Tool %s " % self.tool
        command += "finished: $EXIT_STATUS=========\"\n" 
        command += "exit $EXIT_STATUS\n"
        

        #  //
        # // Excecute the command for this tool
        #//
        executeWithFilter(command, self.filter)
        
        
        
        
        
