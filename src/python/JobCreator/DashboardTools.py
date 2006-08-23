#!/usr/bin/env python
"""
_DashboardTools_

Utils to add Dashboard monitoring information to prodAgent jobs via the
task objects for that job


"""

import popen2
import os
import socket
import time

from ShREEK.CMSPlugins.DashboardInfo import DashboardInfo
from IMProv.IMProvDoc import IMProvDoc
from ProdAgentCore.Configuration import loadProdAgentConfiguration


def gridProxySubject():
    """
    _vomsProxySubject_

    Get subject of certificate using voms-proxy-info -subject
    """
    pop = popen2.Popen4("voms-proxy-info -identity")
    pop.tochild.close()
    output = pop.fromchild.read()
    exitCode = pop.wait()
    if exitCode > 0:
        return None

    for line in output.split("\n"):
        if not line.startswith("/"):
            continue
        else:
            return line.strip()
    return None
        
    

def generateGlobalJobID(taskObject):
    """
    _generateGlobalJobID_

    Generate a global job ID for the dashboard

    """
    try:
        prodAgentConfig = loadProdAgentConfiguration()
        prodAgentName = prodAgentConfig['ProdAgent']['ProdAgentName']
    except StandardError:
        prodAgentName = "ProdAgent" 

    hostname = socket.gethostname()
    if hostname not in prodAgentName:
        prodAgentName = "%s@%s" % (prodAgentName, socket.gethostname())
    jobSpecId = taskObject['JobSpecNode'].jobName
    result = "ProdAgent_%s_%s" %(
        prodAgentName,
        jobSpecId,
        )
    return result


def installDashboardInfo(taskObject):
    """
    _installDashboardInfo_

    Use the Top task Object in the Tree to install the
    DashboardInfo instance into the Job in a common place

    """
    dashboardInfo = DashboardInfo()
    dashboardInfo.job = generateGlobalJobID(taskObject)
    dashboardInfo.task = "ProdAgent_%s" % taskObject['JobSpecNode'].workflow
    dashboardInfo['GridUser'] = gridProxySubject()
    dashboardInfo['User'] = os.environ.get('USER', 'ProdAgent')


    taskObject['DashboardInfoInstance'] =  dashboardInfo
    taskObject['DashboardInfo'] =  IMProvDoc("DashboardMonitoring")
    taskObject['DashboardInfo'].addNode(dashboardInfo.save())
    taskObject['IMProvDocs'].append('DashboardInfo')
    taskObject['DashboardInfoLocation'] = os.path.join(
        "$PRODAGENT_JOB_DIR", 
        taskObject['Name'],
        "DashboardInfo.xml")
    return

def writeDashboardInfo(taskObject, cacheDir):
    """
    _writeDashboardInfo_

    Write the DashboardInfo into two places:

    1. The job itself
    2. The Job cache so that Submitters can get it

    """
    
    dashboardInfo = taskObject.get('DashboardInfoInstance', None)
    if dashboardInfo == None:
        return
    #  //
    # // Refresh save document to go into job
    #//
    taskObject['DashboardInfo'] =  IMProvDoc("DashboardMonitoring")
    taskObject['DashboardInfo'].addNode(dashboardInfo.save())
    
    #  //
    # // Save to job cache
    #//
    dashboardInfo.write(os.path.join(cacheDir, "DashboardInfo.xml"))

    return
                        
