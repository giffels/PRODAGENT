#!/usr/bin/env python
"""
_WorkflowMonitor_

CherryPy handler for displaying the list of workflows in the PA instance

"""

from ProdCommon.Database import Session
from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdAgent.WorkflowEntities.Utilities import listWorkflowsByOwner


_Owners = [ 'RelValInjector', 'WorkflowInjector', 'ProdMgrInterface']

class WorkflowMonitor:

    def index(self):
        Session.set_database(dbConfig)
        Session.connect()
        Session.start_transaction()

        html = """<html><body><h2>ProdAgent Workflows </h2>\n """
        for owner in _Owners:
            workflowList = listWorkflowsByOwner(owner)
            html += "<h4>Workflow Owner: %s</h4>\n<ul>\n" % owner
            
            for workflow in workflowList:
                html += "<li>%s</li>/n" % workflow
            html += "</ul>\n"
            
        
        html += """</body></html>"""
        Session.commit_all()
        Session.close_all()


        return html
    index.exposed = True


