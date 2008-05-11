#!/usr/bin/env python
"""
_LastSitesMonitor_

CherryPy handler for displaying the plot of workflow history 

"""

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
import API


def draw_pie_Distribution(past,site):
        file = os.getcwd()+'/image/SiteDistribution.png'
        data, sum = API.getSites(past,site)
        metadata = {'title':'Destination Sites Distribution'}
        pie = PieGraph()
        if sum > 0:
                coords = pie.run( data, file, metadata )
        return sum

class LastSitesMonitor:

    def __init__(self, past, site = 'all', sitename = 'all sites', graphMonUrl = None):
        self.graphmon = graphMonUrl
        self.site = site
        self.past = past
        self.sitename = sitename

    @cherrypy.expose
    def showimage(self):
        cherrypy.response.headers['Content-Type']= "image/png"
        path= os.getcwd()+'/image/SiteDistribution.png'
	f = open(path, "rb")
        contents = f.read()
        f.close()
        return   contents
        

    def index(self):
        
	_header = """
                                <html>
                                <head>
                                <title>"""+os.environ['HOSTNAME']+""" - CRABSERVER Monitor</title>
                                </head>
                                <body>
                                <div class="container">"""
        _footer = """
                                </div>
                                </body>
                                </html>"""


        queues = API.getQueues(self.site)

        cherrypy.response.headers['Content-Type']= 'text/html'
        
        page = [_header]
        sum = draw_pie_Distribution(self.past,self.site)
        page.append("<br/><b>Submission to %s during last %i hour(s):</b><br/><br/>"%(self.sitename,self.past/3600))
        if sum > 0:
                page.append('<img src="showimage" width="800" height="500" />' )
        else:
                page.append('<br/><b>No job submitted in this period...</b><br/><br/>')
                

        current = API.getBossLiteRunningJobs('status_scheduler',self.site)
        table = "<br/><table>\n"
        table += "<tr colspan=2><b>Current Overall Status of the server for %s:</b></tr>"%(self.sitename)
        sum = 0;
        for row in current:
                table += '<tr><td align="right">'+str(row[1])+':&nbsp;</td><td><b>'+str(row[0])+"</b></td></tr>\n"
                sum += row[0]
        table += '<tr><td align="right"><b>Total</b>:&nbsp;</td><td><b>'+str(sum)+"</b></td></tr>\n"
        table += "</table>\n"
        page.append(table)
        page.append("<br/>queues found belonging to %s:<br/>"%(self.sitename))
        for queue in queues:
                page.append("%s<br/>"%(queue))

        page.append(_footer)
	return page
        
    index.exposed = True
