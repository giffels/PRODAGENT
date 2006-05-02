#!/usr/bin/env python
"""
Generic test methods that are included in 
multiple suite tests.

"""
import os

def preTests():
    print "****************CHECKLIST**********************"
    print "-MCPROTO/install/setup.sh sourced"
    print "-MCPROTO/test/python is included in python path"
    print "-Database configure and schema loaded"
    print "-Python client has access to a mysql database"

    print "Make sure the test input does not conflict"
    print "with the data in the database!"
    print " "
    print "Make sure the database (and client) are properly"
    print " "

    print "*****************START TEST*******************"

def postTests(testResult):

    print "*****************TEST REPORT*******************"
    print "Number of tests run: "+str(testResult.testsRun)
    print "Number of failures:  "+str(len(testResult.failures))
    print "Number of errors  :  "+str(len(testResult.errors))

    print "*******************FAILURES*******************"

    for i in testResult.failures:
       obj,msg=i
       print(str(msg))


    print "*******************ERRORS********************"

    for i in testResult.errors:
       obj,msg=i
       print(str(msg))

    print "******************IMPORTANT*****************"
    print "Please read the componentLog to check for anomalies"
    print "The component might have logged errors as part of its internal"
    print "error handling."
    print ""
    print "The log file can contain Failed messages "
    print "This might be  intended behaviour depending on the tests."


