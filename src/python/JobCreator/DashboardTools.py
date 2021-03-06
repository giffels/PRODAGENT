#!/usr/bin/env python
"""
_DashboardTools_

Utils to add Dashboard monitoring information to prodAgent jobs via the
task objects for that job


"""
from subprocess import Popen, PIPE
import os
import socket
import time

from ShREEK.CMSPlugins.DashboardInfo import DashboardInfo, generateDashboardID, extractDashboardID
from IMProv.IMProvDoc import IMProvDoc
from ProdAgentCore.Configuration import loadProdAgentConfiguration


def gridProxySubject():
    """
    _vomsProxySubject_

    Get subject of certificate using voms-proxy-info -subject
    """
    pop = Popen("voms-proxy-info -identity",
                shell=True,
                stdin=PIPE,
                stdout=PIPE,
                stderr=PIPE,
                close_fds=True)
    exitCode = pop.wait()
    output = pop.stdout
    output = output.read()
    if exitCode > 0:
        return None
    for line in output.split("\n"):
        if not line.startswith("/"):
            continue
        else:
            return line.strip()
    return None
        
    

##def generateGlobalJobID(taskObject):
##    """
##    _generateGlobalJobID_

##    Generate a global job ID for the dashboard

##    WONT WORK FOR BULK OPS

##    """
##    try:
##        prodAgentConfig = loadProdAgentConfiguration()
##        prodAgentName = prodAgentConfig['ProdAgent']['ProdAgentName']
##    except StandardError:
##        prodAgentName = "ProdAgent" 

##    hostname = socket.gethostname()
##    if hostname not in prodAgentName:
##        prodAgentName = "%s@%s" % (prodAgentName, socket.gethostname())

##    prodAgentName = prodAgentName.replace("_", "-")
##    jobSpecId = taskObject['JobSpecNode'].jobName
##    jobName = jobSpecId.replace("_", "-")    
##    result = "ProdAgent_%s_%s" %(
##        prodAgentName,
##        jobName,
##        )

##    taskName = "ProdAgent_%s_%s" % ( taskObject['JobSpecNode'].workflow,
##                                     prodAgentName)
    
##    return result, taskName




def installPADetails(dashboardInfo):
    """
    _installPADetails_

    Add PA IDs & meta information to dashboard info

    """
    prodAgentConfig = loadProdAgentConfiguration()
    paBlock = prodAgentConfig.get('ProdAgent', {})
    prodMonBlock = prodAgentConfig.get("ProdMon", {})

    paName = paBlock.get("ProdAgentName", "ProdAgent")
    team = prodMonBlock.get("Team", "NoTeam")

    dashboardInfo['ProdAgent'] = paName
    dashboardInfo['ProductionTeam'] = team
    return
                                  

def getActivity(jobSpecNode):
    """
    _getActivity_

    See if the jobspec has an Activity parameter, and return
    that or None if not present

    """
    activities = jobSpecNode.getParameter("Activity")
    if len(activities) == 0:
        return None
    return activities[-1]


def installDashboardInfo(taskObject):
    """
    _installDashboardInfo_

    Use the Top task Object in the Tree to install the
    DashboardInfo instance into the Job in a common place

    """
    dashboardInfo = DashboardInfo()
    dashboardInfo.task , dashboardInfo.job = generateDashboardID(taskObject['JobSpecInstance'])
    dashboardInfo['GridUser'] = gridProxySubject()
    dashboardInfo['User'] = os.environ.get('USER', 'ProdAgent')
    dashboardInfo['Workflow'] = taskObject['RequestName']
    dashboardInfo['JobType'] = taskObject['JobType']
    activity = getActivity(taskObject['JobSpecNode'])
    if activity != None:
        dashboardInfo['TaskType'] = activity
    installPADetails(dashboardInfo)
    taskObject['DashboardInfoInstance'] =  dashboardInfo
    taskObject['DashboardInfo'] =  IMProvDoc("DashboardMonitoring")
    taskObject['DashboardInfo'].addNode(dashboardInfo.save())
    taskObject['IMProvDocs'].append('DashboardInfo')
    taskObject['DashboardInfoLocation'] = os.path.join(
        "$PRODAGENT_JOB_DIR", 
        taskObject['Name'],
        "DashboardInfo.xml")
    return

def installBulkDashboardInfo(taskObject):
    """
    _installDashboardInfo_

    Use the Top task Object in the Tree to install the
    DashboardInfo instance into the Job in a common place

    """
    dashboardInfo = DashboardInfo()
    dashboardInfo.job = taskObject['JobName']
    dashboardInfo.task = taskObject['RequestName']
    dashboardInfo['GridUser'] = gridProxySubject()
    dashboardInfo['User'] = os.environ.get('USER', 'ProdAgent')
    dashboardInfo['Workflow'] = taskObject['RequestName']
    dashboardInfo['JobType'] = taskObject['JobType']
    if taskObject['Activity'] != None:
        dashboardInfo['TaskType'] = taskObject['Activity']
    
    installPADetails(dashboardInfo)
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
                        
