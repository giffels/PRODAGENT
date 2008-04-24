#!/usr/bin/env python
"""
_HistStatusMonitor_

CherryPy handler for displaying the history workflow plot 

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

def make_time(length,span):
        end_time = time.time()-time.altzone; # end_time -= end_time % span; end_time += span;
        begin_time = end_time - length*span
        return begin_time, end_time

def gatherNumBossLiteRunningJobs(key, statuses, begin_time, end_time, span):
        data = {};
        for status in statuses:
                data[status] = {};
        for t in range(begin_time, end_time, span):
                tmp = API.getNumBossLiteRunningJobs(key,t)
                for status in statuses:
                        data[status][t] = 0;
                        for row in tmp:
                                if row[1] == status:
                                        data[row[1]][t] = row[0];
        return data


def draw_TimeGraph(key,statuses,length,span):
        path= os.getcwd()
	file = path+'/image/hist__'+key+'.png'
        begin_time, end_time = make_time(length,span);
        data = gatherNumBossLiteRunningJobs(key,statuses, begin_time, end_time, span);
        metadata = {'title':'Job per '+key+' history', 'starttime':begin_time, 'endtime':end_time, 'span':span, 'is_cumulative':True }
        Graph = CumulativeGraph()
        Graph( data, file, metadata )


class HistStatusMonitor:

    def __init__(self, length, span, key, statuses, graphMonUrl = None):
        self.graphmon = graphMonUrl
        self.length = length
        self.span = span
        self.key = key
        self.statuses = statuses

    @cherrypy.expose
    def showimage(self):
        cherrypy.response.headers['Content-Type']= "image/png"
        path= os.getcwd()+'/image/hist__'+self.key+'.png'

	f = open(path, "rb")
        contents = f.read()
        f.close()
        return   contents
        

    def index(self):
        
	_header = """
                                <html>
                                <head>
                                <title>CRABSERVER Monitor</title>
                                </head>
                                <body>
                                <div class="container">"""
        _footer = """
                                </div>
                                </body>
                                </html>"""

        draw_TimeGraph(self.key,self.statuses,self.length,self.span)
        cherrypy.response.headers['Content-Type']= 'text/html'
        
        page = [_header]
        page.append('<img src="showimage" width="800" height="500" />' )
        page.append(_footer)
	return page
        
    index.exposed = True
