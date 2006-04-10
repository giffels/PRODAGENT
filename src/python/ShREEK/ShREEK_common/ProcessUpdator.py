#!/usr/bin/env python
"""
_ProcessUpdator_

Updator method that generates a list of child processes belonging to the
current task process

"""
import os
import popen2

from ShREEK.ShREEKPluginMgr import registerShREEKUpdator


def listChildProcesses(state):
    """
    _listChildProcesses_

    Get a list of child processes for the CurrentProcess listed in
    the state instance, if available.
    Return a list of process IDs

    """
    if not state.has_key("CurrentProcess"):
        return []
    pop = popen2.Popen4(
        "/bin/ps --no-heading --ppid %s -o pid" % state['CurrentProcess']
        )
    while pop.poll() == -1:
        exitCode = pop.poll()
    exitCode = pop.poll()
        
    output = pop.fromchild.read().strip()
    result = []
    for item in output.split():
        try:
            result.append(int(item))
        except StandardError, ex:
            continue
        
    return result


def processToBinary(state):
    """
    _processToBinary_

    For each process name in the CurrentProcess and ChildProcesses
    list if present, generate a map of pid to executable name

    """
    if not state.has_key("CurrentProcess"):
        return {}
    processList = [state['CurrentProcess']]
    if state.has_key("ChildProcesses"):
        processList.extend(state['ChildProcesses'])
    result = {}
    for procId in processList:
        try:
            procIdInt = int(procId)
        except StandardError, ex:
            continue
        result[procId] = os.path.realpath("/proc/%s/exe" % procId)
    return result
    
    


registerShREEKUpdator(listChildProcesses, "ChildProcesses")
registerShREEKUpdator(processToBinary, "ProcessToBinary")
    
