#!/usr/bin/env python
"""
_CondorQ_



"""

import os
import string

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
    

class CondorQClass:
    """
    _CondorQClass_

    A class that hold ste result of condorQ
    """
    def __init__(self,constraints,load_on_create=True):
        self.constraints=constraints[0:]
        # self.jobs will hold the result of condor_q
        self.jobs=None

        if load_on_create:
            self.load()
        return

    def change_constraints(self,constraints,load_on_create=True):
        """
        _change_constraints_


        Change constraints.
        Reset or reload the jobs with condorQ.
        """
        self.constraints=constraints[0:]
        self.jobs=None

        if load_on_create:
            self.load()
        return
        

    def load(self):
        """
        _load_

        (Re)load the jobs with condorQ.
        """
        self.jobs=condorQ(self.constraints)
        return

    def copy(self,filter_function=None):
        """
        _copy_

        Make a copy of the object, possibly filtering the jobs in the process.
        filter_function will be called on each job in the cache; the job will be
        copied only if the function returns True
        """
        new_obj=CondorQClass(constreaints=self.constraints,load_on_create=False)
        if self.jobs!=None:
            if filter_function==None:
                new_obj.jobs=self.jobs.copy()
            else:
                new_obj.jobs=[]
                for job in self.jobs:
                    if filter_function(job):
                        new_obj.jobs.append(job.copy())
            
        return new_obj

class CondorPAJobs(CondorQClass):
    """
    _CondorPAJobs_

    Retrieve only a subset of jobs based on ClassAd ID.
    The jobs can be further filtered in memory.
    """
    def __init__(self,jobID='ProdAgent_JobType',jobTypes=set(["Processing","Merge","CleanUp"]),load_on_create=True):
        self.jobID=jobID
        self.jobTypes=jobTypes.copy()
        CondorQClass.__init__(self,constraint="stringListMember(%s,\\\"%s\\\")"%(jobID,string.join(self.jobTypes,',')),
                              load_on_create=load_on_create)
        return

    def copy(self,jobTypes=None):
        """
        _copy_

        Make a copy of the object, possibly filtering the jobs in the process.
        """
         if jobTypes==None:
            new_obj=CondorPAJobs(self.jobID,self.jobTypes,load_on_create=False)
            if self.jobs!=None:
                new_obj.jobs=self.jobs.copy()
        else:
            jobTypes=jobTypes.intersection(self.jobTypes) # if a user specifies new elements, discard them
            new_obj=CondorPAJobs(self.jobID,jobTypes,load_on_create=False)
            if self.jobs!=None:
                new_obj.jobs=[]
                for job in self.jobs:
                    if job[self.jobID] in jobTypes:
                        new_obj.jobs.append(job.copy())

        return new_obj

def countJobs(jobs,id_function,default_ids=[],default_value="",statuses=[[1,'Idle'],[2,'Running']]):
    """
    _countJobs_

    Inputs:
     jobs          - result of condorQ
     id_function   - a function that given a job returns a string
     default_ids   - see below
     default_value - string to use if id_function throws an exception
     statuses      - which JobStatus-es are of interest
        
    Return a dictionary of ids: number of jobs for
    each id found in the jobs list
    
    
    Default ids is a list of ids that you want to
    make sure appear in the output, since if there are no jobs, it
    wont be included. If there are no jobs at a default ids,
    then it will be returned as having 0 jobs
    """

    def_attribute={}
    for status in statuses:
        AttrStatus=status[1]
        def_attribute[AttrStatus]=0
        
    attributes = {}
    for default in default_ids:
        attributes[default] = def_attribute.copy()

    for item in jobs:
        try:
            id_str  = id_function(item)
        except:
            id_str = default_value.copy()

        if not attributes.has_key(id_str):
            attributes[id_str] = def_attribute.copy()

        for status in statuses:
            JobStatus,AttrStatus=status
            if item['JobStatus'] == JobStatus:
                attributes[id_str][AttrStatus] += 1

    return attributes

# getJobs helper function
def extractGatekeeper(item):
    gatekeeper = item['GridResource'].replace(item['JobGridType'], '')
    gatekeeper = gatekeeper.strip()
    return gatekeeper

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
    return countJobs(jobs=theJobs,default_ids=defaultGatekeepers,
                     id_function=extractGatekeeper,default_value='cmsosgce.fnal.gov/jobmanager-condor')

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
    

    
    
        
    
    
    
