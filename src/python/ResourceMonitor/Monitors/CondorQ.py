#!/usr/bin/env python
"""
_CondorQ_



"""

import os


from xml.sax.handler import ContentHandler
from xml.sax import make_parser

def unique(s):
    """
    Return a list of the elements in s, but without duplicates.

    Yoinked from activestate
   
    """
    n = len(s)
    if n == 0:
        return []

    u = {}
    try:
        for x in s:
            u[x] = 1
    except TypeError:
        del u  # move on to the next method
    else:
        return u.keys()

    try:
        t = list(s)
        t.sort()
    except TypeError:
        del t  # move on to the next method
    else:
        assert n > 0
        last = t[0]
        lasti = i = 1
        while i < n:
            if t[i] != last:
                t[lasti] = last = t[i]
                lasti += 1
            i += 1
        return t[:lasti]

    # Brute force is all that's left.
    u = []
    for x in s:
        if x not in u:
            u.append(x)
    return u


class CondorQHandler(ContentHandler):
    """
    _CondorQHandler_

    XML SAX Handler to parse the classads returned by the condor_q -xml command

    """
    def __init__(self):
        ContentHandler.__init__(self)
        self.classads = []
        self.thisClassad = None
        self._CharCache = ""
        self.currentClassad = None
        self.boolean = None

    def startElement(self, name, attrs):
        """
        _startElement_

        Override SAX startElement handler
        """
        if name == "c":
            self.thisClassad = {}
            return
        if name == "a":
            adname = attrs.get("n", None)
            if adname == None:
                return
            self.thisClassad[str(adname)] = None
            self.currentClassad = str(adname)
            return
        if name == "b":
            boolValue = attrs.get("v", None)
            if boolValue == None:
                return
            if boolValue == "t":
                self.boolean = True
            else:
                self.boolean = False
            return
        
        
    def endElement(self, name):
        """
        _endElement_

        Override SAX endElement handler
        """
      
        if name == "c":
            self.classads.append(self.thisClassad)
            self.thisClassad = None
            return
        
        if name == "i":
            self.thisClassad[self.currentClassad] = int(self._CharCache)
            return
        if name == "s":
            self.thisClassad[self.currentClassad] = str(self._CharCache)
            return
        if name == "r":
            self.thisClassad[self.currentClassad] = str(self._CharCache)
            return
        if name == "b":
            if self.boolean != None:
                self.thisClassad[self.currentClassad] = self.boolean
                self.boolean = None
                
        
        self._CharCache = ""
        
    def characters(self, data):
        """
        _characters_

        Accumulate character data from an xml element
        """
        self._CharCache += data.strip()


def condorQ(constraints):
    """
    _condorQ_

    Run a condor_q command and return the XML formatted output as a list
    of classad dictionaries 

    """
    command = "condor_q -xml -constraint %s  " % constraints
    si, sout = os.popen2(command)
    content = sout.read()
    content = content.split("<!DOCTYPE classads SYSTEM \"classads.dtd\">")[1]
    handler = CondorQHandler()
    parser = make_parser()
    parser.setContentHandler(handler)
    try:
        parser.feed(content)
    except Exception, ex:
        # No xml data, no override, nothing to be done...
        return []
    
    return handler.classads
    


def getJobs(jobType,*defaultGatekeepers):
    """
    _getJobs_

    Return a dictionary of gatekeeper: number of jobs for
    each gatekeeper found in the condor_q output

    Default gatekeepers is a list of gatekeepers that you want to
    make sure appear in the output, since if there are no jobs, it
    wont be included. If there are no jobs at a default gatekeeper,
    then it will be returned as having 0 jobs
    """
    theJobs = condorQ("ProdAgent_JobType==\\\"%s\\\""%jobType)
    attributes = {}
    for default in defaultGatekeepers:
        attributes[default] = {"Idle": 0, "Running": 0}

    for item in theJobs:
        try :
            gatekeeper = item['GridResource'].replace(item['JobGridType'], '')
        except:
            gatekeeper = 'cmsosgce.fnal.gov/jobmanager-condor'
        gatekeeper = gatekeeper.strip()
        item['Gatekeeper'] = gatekeeper

        if not attributes.has_key(gatekeeper):
            attributes[gatekeeper] = {"Idle": 0, "Running": 0}

        if item['JobStatus'] == 1:
            attributes[gatekeeper]['Idle'] += 1
        if item['JobStatus'] == 2:
            attributes[gatekeeper]['Running'] += 1

    return attributes

def processingJobs(*defaultGatekeepers):
    """
    _processingJobs_

    Return a dictionary of gatekeeper: number of processing jobs for
    each gatekeeper found in the condor_q output

    Default gatekeepers is a list of gatekeepers that you want to
    make sure appear in the output, since if there are no jobs, it
    wont be included. If there are no jobs at a default gatekeeper,
    then it will be returned as having 0 jobs
    """

    return getJobs("Processing",*defaultGatekeepers)

def mergeJobs(*defaultGatekeepers):
    """
    _mergeJobs_

    Return a dictionary of gatekeeper: number of merge jobs for
    each gatekeeper found in the condor_q output

    Default gatekeepers is a list of gatekeepers that you want to
    make sure appear in the output, since if there are no jobs, it
    wont be included. If there are no jobs at a default gatekeeper,
    then it will be returned as having 0 jobs
    
    """

    return getJobs("Merge",*defaultGatekeepers)

def cleanupJobs(*defaultGatekeepers):
    """
    _processingJobs_

    Return a dictionary of gatekeeper: number of cleanup jobs for
    each gatekeeper found in the condor_q output

    Default gatekeepers is a list of gatekeepers that you want to
    make sure appear in the output, since if there are no jobs, it
    wont be included. If there are no jobs at a default gatekeeper,
    then it will be returned as having 0 jobs
    """

    return getJobs("CleanUp",*defaultGatekeepers)

if __name__ == '__main__':


    print processingJobs("cmsosgce.fnal.gov/jobmanager-condor-opt",
                         "red.unl.edu/jobmanager-pbs")
    print mergeJobs("cmsosgce.fnal.gov/jobmanager-condor-opt",
                    "red.unl.edu/jobmanager-pbs")
    print cleanupJobs("cmsosgce.fnal.gov/jobmanager-condor-opt",
                    "red.unl.edu/jobmanager-pbs")
    

    
    
        
    
    
    
