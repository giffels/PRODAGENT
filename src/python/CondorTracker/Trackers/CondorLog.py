#!/usr/bin/env python
"""
_CondorLog_

Object representing a condor log file & parser for log files written as XML




"""
from xml.sax import make_parser
from ResourceMonitor.Monitors.CondorQ import CondorQHandler




_StatusMap = {
    0 : "Submitted",
    1 : "Executed",
    5 : "Terminated",
    9 : "Aborted",
    12: "Held",
    }


class CondorLog(dict):
    """
    _CondorLog_

    Condor Log file details, stored as a dictionary

    """
    def __init__(self):
        dict.__init__(self)
        self.setdefault("Cluster", None)
        self.setdefault("Submitted", False)
        self.setdefault("Executed", False)
        self.setdefault("Terminated", False)
        self.setdefault("Aborted", False)
        self.setdefault("Held", False)

    def success(self):
        """
        _success_

        Return True or False if job was a success or not

        """
        if self['Terminated']:
            if self.has_key('TerminatedNormally'):
                if self['TerminatedNormally']:
                    return True
                else:
                    return False
            # Not sure about this...
            return True
        if self['Aborted']:
            return False
        if self['Held']:
            return False
        return False

    def condorStatus(self):
        """
        _condorStatus_

        Map status in this object to a condor_q status value

        """
        if self.success():
            return 4
        if self['Held']:
            return 5
        if self['Aborted']:
            return 6
        if self['Executed']:
            return 2
        if self['Submitted']:
            return 1
        return 3
        

def readCondorLog(logfile):
    """
    _readCondorLog_

    Read a condor XML logfile and return a CondorLog object


    """
    content = file(logfile).read()
    if content.find("<!DOCTYPE classads SYSTEM \"classads.dtd\">") > -1:
        content = content.replace("<!DOCTYPE classads SYSTEM \"classads.dtd\">", "")
    content = "<xml>%s</xml>" % content
    
    handler = CondorQHandler()
    parser = make_parser()
    parser.setContentHandler(handler)
    try:
        parser.feed(content)
    except Exception, ex:
        # No xml data, no override, nothing to be done...
        return None

    condorLog = CondorLog()
    
    for classad in handler.classads:
        evNum = classad["EventTypeNumber"]
        cluster = classad.get('Cluster', None)
        if cluster != None:
            condorLog['Cluster'] = cluster
        if evNum in _StatusMap.keys():
            condorLog[_StatusMap[evNum]] = True

        if classad.has_key("Reason"):
            condorLog['Reason'] = classad['Reason']

        if classad.has_key("TerminatedNormally"):
            condorLog['TerminatedNormally'] = classad['TerminatedNormally']

    return condorLog

        

    
    


if __name__ == '__main__':
    f1 = "/home/evansde/work/PRODAGENT/work/JobCreator/PhysVal120-SingleMuMinus-Pt10/373/PhysVal120-SingleMuMinus-Pt10-373-condor.log"
    f2 = "/home/evansde/work/PRODAGENT/work/JobCreator/PhysVal120-SingleMuMinus-Pt10/373/PhysVal120-SingleMuMinus-Pt10-373-condor.log"

    f3 = "/home/evansde/work/PRODAGENT/work/JobCreator/PhysVal120-SingleMuMinus-Pt10/204/PhysVal120-SingleMuMinus-Pt10-204-condor.log"

    log1 = readCondorLog(f1)
    log2 = readCondorLog(f2)
    log3 = readCondorLog(f3)

    print log1['Cluster'], log1.success()
    print log2['Cluster'], log2.success()
    print log3['Cluster'], log3.success(), log3['Reason']
