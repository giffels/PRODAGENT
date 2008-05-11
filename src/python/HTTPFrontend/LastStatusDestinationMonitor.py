#!/usr/bin/env python
"""
_LastStatusDestinationMonitor_

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
from graphtool.graphs.common_graphs import PieGraph, BarGraph, CumulativeGraph, StackedBarGraph
from graphtool.graphs.graph import TimeGraph
import API

def gatherDistribution(past,site):
        distribution = API.getNumLastBossLiteRunningJobs('status_scheduler',past,site)
        statuses = API.getBossLiteRunningJobs('status_scheduler',site);
        data = {};
        sum = 0
        for status in statuses:
                data[status[1]] = 0;
                for row in distribution:
                        if row[1] == status[1]:
                                data[row[1]] = row[0];
                                sum += row[0]
        return data, sum

def draw_Stacked_Distribution(past,Sites):
#       data['Status'] = {'Site1':X, 'Site2':Y, 'Site3':Z'} 
        file = os.getcwd()+'/image/distribution.png'
        data = {}
        Sum = 0
        statuses = API.getBossLiteRunningJobs('status_scheduler','all');
        for status in statuses:
                data[status[1]] = {}
        for site in Sites.keys():
                tmpdata, sum =  gatherDistribution(past,Sites[site])
                Sum+=sum
                for status in statuses:
                        dummy1, dummy2, label = site.split('_',2)
                        if tmpdata.has_key(status[1]):
                                data[status[1]][label]=tmpdata[status[1]]
                        else:
                                data[status[1]][label]=0
        metadata = {'title':'Job Status per Site Distribution'}
        sbg = StackedBarGraph()
        if Sum > 0:
                sbg.run( data, file, metadata )
        return Sum


class LastStatusDestinationMonitor:

    def __init__(self, past, Sites, graphMonUrl = None):
        self.graphmon = graphMonUrl
        self.Sites = Sites
        self.past = past

    @cherrypy.expose
    def showimage(self):
        cherrypy.response.headers['Content-Type']= "image/png"
        path= os.getcwd()+'/image/distribution.png'
	f = open(path, "rb")
        contents = f.read()
        f.close()
        return   contents
    showimage.exposed = True
        

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

        cherrypy.response.headers['Content-Type']= 'text/html'
        
        page = [_header]
        Sum = draw_Stacked_Distribution(self.past,self.Sites)
        page.append("<br/><b>Submission to all sites during last %i hour(s):</b><br/><br/>"%(self.past/3600,))
        if Sum > 0:
                page.append('<img src="showimage" wNOWIDTHidth="800" hNOHEIGHTeight="500" />' )
        else:
                page.append('<br/><b>No job submitted in this period...</b><br/><br/>')

        page.append(_footer)
	return page
        
    index.exposed = True
