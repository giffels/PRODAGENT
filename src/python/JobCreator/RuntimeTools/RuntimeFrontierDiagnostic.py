#!/usr/bin/env python
"""
_RuntimeFrontierDiagnostic_

Runtime script to run a frontier diagnostic test on
the frontier log and report it somewhere (FJR?)

"""



import os


def frontierDiagnostic():
    """
    _frontierDiagnostic_


    """
    frontierLog = os.path.join(os.getcwd(), "Frontier.log")
    print "Running diagnostic on Frontier log:"
    print frontierLog
    if not os.path.exists("Frontier.log"):
        msg = "Frontier.log not found..."
        print msg
        return

    errors = 0
    warnings = 0
    print "=============Dump Frontier Log=============="
    handle = open("Frontier.log", 'r')
    for line in handle.readlines():
        print line
        if line.startswith("error"):
            errors += 1
        if line.startswith("warn"):
            warnings += 1
    print "=========End Dump Frontier Log=============="

    msg = "==========Frontier Summary==========\n"
    msg += "  Warnings : %s\n" % warnings
    msg += "  Errors   : %s\n" % errors
    msg += "======End Frontier Summary==========\n"
    print msg
    
    return



if __name__ == '__main__':
    frontierDiagnostic()
