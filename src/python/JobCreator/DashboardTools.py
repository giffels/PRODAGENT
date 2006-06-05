#!/usr/bin/env python
"""
_DashboardTools_

Utils to add Dashboard monitoring information to prodAgent jobs via the
task objects for that job


"""

import popen2
import os
import socket

from ShREEK.CMSPlugins.DashboardInfo import DashboardInfo
from IMProv.IMProvDoc import IMProvDoc


def gridProxySubject():
    """
    _vomsProxySubject_

    Get subject of certificate using voms-proxy-info -subject
    """
    pop = popen2.Popen4("grid-proxy-info -identity")
    pop.tochild.close()
    output = pop.fromchild.read()
    exitCode = pop.wait()
    if exitCode > 0:
        return None
    return output.strip()
    

    


def installDashboardInfo(taskObject):
    """
    _installDashboardInfo_

    Use the Top task Object in the Tree to install the
    DashboardInfo instance into the Job in a common place

    """
    dashboardInfo = DashboardInfo()
    dashboardInfo.job = taskObject['JobSpecNode'].jobName
    dashboardInfo.task = taskObject['JobSpecNode'].workflow
    dashboardInfo['GridUser'] = gridProxySubject()
    dashboardInfo['User'] = os.environ.get('USER', 'ProdAgent')
    dashboardInfo['NodeName'] = socket.gethostname()

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
                        
