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

    return



if __name__ == '__main__':
    frontierDiagnostic()
