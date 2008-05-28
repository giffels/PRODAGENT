#!/usr/bin/env python
"""
Components and Services Monitoring 
"""

import os
import sys
import getopt
import inspect
import time
import popen2

from ProdAgentCore.Configuration import ProdAgentConfiguration
from ProdAgentCore.DaemonDetails import DaemonDetails
from TaskTracking.TaskStateAPI import *
from pylab import *
import cherrypy
from cherrypy import tools
from numpy import *
import time, os, random, datetime
from matplotlib.font_manager import FontProperties
from matplotlib.backends.backend_agg import RendererAgg
from matplotlib.transforms import Value
from graphtool.graphs.common_graphs import PieGraph, BarGraph, CumulativeGraph
from graphtool.graphs.graph import TimeGraph
from calendar import *
import API

def status():
    """
    _status_

    Print status of all components in config file

    """
    config = os.environ.get("PRODAGENT_CONFIG", None)
    cfgObject = ProdAgentConfiguration()
    cfgObject.loadFromFile(config)

    components = cfgObject.listComponents()

    component_run = []
    component_down = []
    for component in components:
        compCfg = cfgObject.getConfig(component)
        compDir = compCfg['ComponentDir']
        compDir = os.path.expandvars(compDir)
        daemonXml = os.path.join(compDir, "Daemon.xml")
        if not os.path.exists(daemonXml):
            continue
        daemon = DaemonDetails(daemonXml)
        if not daemon.isAlive():
    
	    component_down.append(component)
        else:
	    tmp=[component, daemon['ProcessID']]
	    component_run.append(tmp)
    return component_run, component_down

class ComponentMonitor:

    def __init__(self, graphMonUrl = None):
        self.graphmon = graphMonUrl            
    @cherrypy.expose
    def showimage(self):
        cherrypy.response.headers['Content-Type']= "image/png"
        path= os.getcwd()+'/image/services.png'
        f = open(path, "rb")
        contents = f.read()
        f.close()

        return   contents

    def index(self):

        _header = """
                            <html>
                            <head>
                            <title>CRABSERVER Monitor</<title>
                            </head>
                            <body>
                            <div class="container">"""
        _footer = """
                            </div>
                            </body>
                            </html>"""

        run , not_run = status()
        delegation = API.getpidof("delegation", "Delegation Service")
        gridftp = API.getpidof("gridftp-server","Globus GridFtp")
        page = [_header]
        table_run = '<h2> Status Components :</h2>'
        table_run += '<table>\n'

        sum = 0
        for r in run:    
            table_run += '<tr><td align="left">'+str(r[0])+': </td><td><b>PID : '+str(r[1])+'</b></td></tr>\n'
		
        for n in not_run:
            table_run += '<tr><td align="left">'+str(n)+': </td><td><b>Not Running </b></td></tr>\n'

        table_run += "</table>\n"

        table_run += '<h2> Status Services :</h2>'
        table_run += '<table>\n'
    
        table_run += "<tr><td align=\"left\">"+str(gridftp[0])+": </td><td><b>"+str(gridftp[1])+"</b></td></tr>\n"
        table_run += "<tr><td align=\"left\">"+str(delegation[0])+": </td><td><b>"+str(delegation[1])+"</b></td></tr>\n"
        table_run += "</table>\n"
 
        page.append(table_run)
 
        page.append(_footer)
 
        return page
 
    index.exposed = True

