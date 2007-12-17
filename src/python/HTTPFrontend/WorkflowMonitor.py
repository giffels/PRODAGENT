#!/usr/bin/env python
"""
_WorkflowMonitor_

CherryPy handler for displaying the list of workflows in the PA instance

"""
import os
from ProdCommon.Database import Session
from ProdAgentDB.Config import defaultConfig as dbConfig
import ProdAgent.WorkflowEntities.Utilities as WEUtil


_Owners = [ 'RelValInjector', 'WorkflowInjector', 'ProdMgrInterface']

_States = ['register','released','create','submit','inProgress',
           'finished','reallyFinished','failed']


class WorkflowMonitor:

    def __init__(self, graphMonUrl = None):
        self.graphmon = graphMonUrl
    
    def index(self):
        Session.set_database(dbConfig)
        Session.connect()
        Session.start_transaction()
        html = """<html><body><h2>ProdAgent Workflows </h2>\n """
        for owner in _Owners:
            workflowList = WEUtil.listWorkflowsByOwner(owner)
            html += "<h4>Workflow Owner: %s</h4>\n<ul>\n" % owner
            
            for workflow in workflowList:
                html += "<li><a href=\"%s?workflow=%s\">%s</a></li>\n" % (
                    self.graphmon, workflow, workflow)
            html += "</ul>\n"
            
        
        html += """</body></html>"""
        Session.commit_all()
        Session.close_all()


        return html
    index.exposed = True


class WorkflowGraph:

    def __init__(self, imageUrl, imageDir):
        self.imageServer = imageUrl
        self.workingDir = imageDir

    def index(self, workflow):

        errHtml = "<html><body><h2>No Graph Tools installed!!!</h2>\n "
        errHtml += "</body></html>"
        try:
            from graphtool.graphs.common_graphs import StackedBarGraph
        except ImportError:
            
            return errHtml
        Session.set_database(dbConfig)
        Session.connect()
        Session.start_transaction()
        procStatus = {}
        mergeStatus = {}
        for state in _States:
            procStatus[state] = len(
                WEUtil.jobsForWorkflow(workflow, "Processing", state ))
            mergeStatus[state] = len(
                WEUtil.jobsForWorkflow(workflow, "Merge", state))
        Session.commit_all()
        Session.close_all()

        
        pngfile = os.path.join(self.workingDir, "%s-WorkflowGraph.png" % workflow)
        pngfileUrl = "%s?filepath=%s" % (self.imageServer, pngfile)

        data =  { "Processing" : procStatus, "Merge" : mergeStatus}
        metadata = {"title" : "Job States for %s" % workflow }
        plotfile = open(pngfile, 'w')
        SBG = StackedBarGraph()
        SBG(data, plotfile, metadata)
        plotfile.close()
        
        html = "<html><body><img src=\"%s\"></body></html>" % pngfileUrl
        return html

        
    index.exposed = True
