#!/usr/bin/env python
"""
_EventLogger_

Object that reads and updates a cmsRun EventLogger output logfile whenever
it is called

"""

import os
import re
import popen2


MatchRunEvent = re.compile("Run: [0-9]+ Event: [0-9]+$")

class EventLogger:
    """
    _EventLogger_

    Parser object that is instantiated with a file, and every time it
    is called, attempts to read the latest event and run from the log file
    it is watching.
    These are then updated in the instance.

    """
    def __init__(self, filename):
        self.filename = filename
        self.latestRun = 0
        self.latestEvent = 0

    def __call__(self):
        """
        _operator()_

        Update from file when called.

        """
        lines = self.readFile()

        lastMatch = None
        for line in lines:
            if MatchRunEvent.search(line.strip()):
                matches = MatchRunEvent.findall(line.strip())
                lastMatch = matches[-1]
                

        if lastMatch != None:
            #  //
            # // Extract and update last run/event number
            #//
            try:
                runInfo, lastEvent = lastMatch.split("Event:", 1)
                lastRun =  int(runInfo.split("Run:", 1)[1])
                lastEvent = int(lastEvent)
            except Exception:
                return (self.latestRun, self.latestEvent)

            self.latestRun = lastRun
            self.latestEvent = lastEvent

        return (self.latestRun, self.latestEvent)


    def readFile(self):
        """
        _readFile_

        Return last 10 lines of file

        """
        if not os.path.exists(self.filename):
            return []
        command = "tail -10 %s" % self.filename
        pop = popen2.Popen3(command)
        pop.wait()
        exitCode = pop.poll()
        if exitCode:
            return []

        lines = pop.fromchild.readlines()
        return lines



        

        
