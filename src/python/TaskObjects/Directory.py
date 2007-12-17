#!/usr/bin/env python
"""
_Directory_

Object representing a directory attached to a TaskObject
that contains other Directory instances and Files


"""

import os
import logging



class File:
    """
    _File_

    Class representing a file added to a directory.

    Contains a source for the file and a name within
    the directory

    """
    def __init__(self, directory, name, source):
        self.directory = directory
        self.name = name
        self.source = source

        


    def path(self):
        """
        _path_

        Get name of this file within directory structure

        """
        return "%s/%s" % (self.directory.path(), self.name)
        
    def fetch(self, targetDir):
        """
        _fetch_

        Get the source and put it in the target dir.

        Note: for now this uses cp, could use other
        things based on source type, eg http:// etc etc

        """
        command = "/bin/cp -rf %s %s/%s" % (self.source,
                                        targetDir,
                                        self.name)
        logging.info("fetch:%s" % command)
        os.system(command)
        return
        



class Directory:
    """
    _Directory_


    """
    def __init__(self, name):
        self.name = name
        self.parent = None
        self.children = {}
        self.files = {}
        self.physicalPath = None
        

    def addDirectory(self, name):
        """
        _addDirectory_

        Add a new child Directory to this.
        Return reference to new Directory instance

        """
        if self.children.has_key(name):
            return self.children[name]

        self.children[name] = Directory(name)
        self.children[name].parent = self
        return self.children[name]


    def addFile(self, source, targetName = None):
        """
        _addFile_

        Add a file to this directory.
        The file will be pulled in from the source specified.
        targetName is the optional name of the file in this
        directory. If not specified, the basename of the file
        will be used

        """
        target = targetName
        if target == None:
            target = os.path.basename(source) 
            
        if self.files.has_key(target):
            msg = "File %s already exists in directory %s" % (
                self.name, target)
            raise RuntimeError, msg

        newFile = File(self, target, source)
        self.files[target] = newFile
        return


    def path(self):
        """
        _path_

        Get name of this dir within directory structure

        """
        if self.parent == None:
            return self.name
        return "%s/%s" % (self.parent.path(), self.name)
        
        
    def create(self, targetDir):
        """
        _create_

        Make this directory in the targetDirectory provided,
        pull in all files and then recursively create any
        children

        """
        newDir = "%s/%s" % (targetDir, self.name)
        logging.info("create(%s)" % newDir)
        if not os.path.exists(newDir):
            os.makedirs(newDir)
        for f in self.files.values():
            f.fetch(newDir)

        for child in self.children.values():
            child.create(newDir)
        return
    
    def __str__(self):
        result = "%s\n" % self.path()
        for f in self.files.values():
            result += "%s ==> %s\n" % (f.path(), f.source)
        for d in self.children.values():
            result += str(d)

        return result

if __name__ == '__main__':

    
    startDir = os.getcwd()

    dir1 = Directory("detritus")
    dir2 = dir1.addDirectory("dir2")
    dir3 = dir1.addDirectory("dir3")
    dir4 = dir1.addDirectory("dir4")
    dir5 = dir2.addDirectory("dir5")
    dir6 = dir2.addDirectory("dir6")
    dir7 = dir6.addDirectory("dir7")


    files = [
        "/home/evansde/ARCCreator.py",
        "/home/evansde/OSGResConBulkSubmitter.py",
        "/home/evansde/ARCSubmitter.py",
        "/home/evansde/ProbotTest.py",
        "/home/evansde/ARCTracker.py",
        "/home/evansde/PYDCAPImpl.py"
        ]

    count = 0
    for f in files:
        dir1.addFile(f)
        dir7.addFile(f, "file%s" % count)
        count += 1
        
        

    #dir1.create(startDir)
    print str(dir1)
