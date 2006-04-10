#!/usr/bin/env python
"""
_StructuredFile_

List based file/script creation container for adding small scripts to tasks

"""

__version__ = "$Revision: 1.1 $"
__revision__ = \
        "$Id: StructuredFile.py,v 1.1 2005/12/30 18:46:35 evansde Exp $"
__author__ = "evansde@fnal.gov"

import os


class StructuredFile(list):
    """
    _StructuredFile_
    
    Structured File Object to represent a simple script or text file within
    a task. can simply contain a reference to an existing file.
    The object is essentially a list that contains the script line-by-line
    as strings, with some additional tools to allow it to be written
    out and keep track of its interpreter and target dir.

    Attributes:

    - *name* : name of the file (basename)

    - *interpreter* : name of the interpreter to be used to invoke the script
    if required (Eg /bin/sh, /usr/bin/python etc

    - *executable* : boolean, wether the file should be executable or not.
    Default is not executable.

    - *targetDir* : dir path where file should be written to when write method
    is called.
    
    """
    
    def __init__(self, filename = None, targetDir = None):
        list.__init__(self)
        self.name = filename
        self.interpreter = None
        self.executable = False
        self.targetDir = targetDir
        


    def isExecutable(self):
        """
        _isExecutable_
        
        Returns true if executable.
        """
        return self.executable

    def setExecutable(self):
        """
        _setExecutable_
        
        Set that this script is executable.
        """
        self.executable = True


    def setTargetDir(self, targetDir):
        """
        _setTargetDir_
        
        Set the target directory attribute
        """
        self.targetDir = targetDir
        
    
    def __str__(self):
        """
        Return a string representation of the Structured file  and content
        """
        result = "###StructuredFile\n"
        result += "###Name: %s\n" % self.name
        result += "###Interpreter: %s\n" % self.interpreter
        result += "###Executable: %s\n" % self.executable
        result += "###Target Dir: %s\n" % self.targetDir
        for line in self:
            result += "%s\n" % line
        return result
        

    def write(self):
        """
        _write_
        
        Write the file to disk, using the targetDir and filename,
        and setting the exe bit if applicable.
        targetDir must be set and exist to call this function.
        """
        if self.targetDir == None:
            msg = "Target Directory not set for StructuredFile"
            print "TODO: Exception Type"
            raise RuntimeError, msg
        if not os.path.exists(self.targetDir):
            msg = "Target Directory does not exist for StructuredFile"
            print "TODO: Exception Type"
            raise RuntimeError, msg

        outputName = os.path.join(self.targetDir, self.name)
        output = open(outputName, 'w')
        for line in self:
            output.write("%s\n" % line)
        output.close()
        if self.executable:
            os.system("chmod +x %s" % outputName)
        return

