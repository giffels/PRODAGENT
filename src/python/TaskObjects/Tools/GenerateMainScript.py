#!/usr/bin/env python
"""
_GenerateMainScript_

For a Given TaskObject instance, create a StructuredFile
representing a 'Main' Script to run the task, and insert the
details of the Script into the ShREEKTask.

The StructuredFile instance is added to the TaskObject, and the
script name is set as the Executable attribute of the object

The StructuredFile is not actually populated to run any particular
executable, but rather provides a standard framework in which to
insert commands

"""


class GenerateMainScript:
    """
    _GenerateMainScript_

    Create a StructuredFile instance using the name of the
    TaskObject.
    Insert details of that StructuredFile into the ShREEKTask in
    the taskObject so that it can function as an executable.

    """

    def __call__(self, taskObject):
        """
        _operator()_

        Act on Task Object to Generate a Main script and insert
        details into the ShREEKTask

        
        """
        scriptName = "%s-main.sh" % taskObject['Name']
        
        script = taskObject.addStructuredFile(scriptName)
        script.setExecutable()
        script.append("#!/bin/bash")
        script.append("echo \"Task Running: %s\"" % taskObject['Name'])
        script.append("echo \"From Dir: `pwd`\"" )
        script.append("echo \"Started: `date +%s`\"" )
        

        taskObject['Executable'] = scriptName
        taskObject['ShREEKTask'].attrs['Executable'] = scriptName
        return
        
        
        
        
