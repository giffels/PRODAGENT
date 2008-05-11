#!/usr/bin/env python
"""
_HistStatusMonitor_

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

def make_time(length,span):
        end_time = time.time()-time.altzone; # end_time -= end_time % span; end_time += span;
        begin_time = end_time - length*span
        return begin_time, end_time

def gatherNumBossLiteRunningJobs(key, begin_time, end_time, span, site = 'all'):
        data = {};
        statuses = API.getBossLiteRunningJobs(key);
        for status in statuses:
                data[status[1]] = {};
        for t in range(begin_time, end_time, span):
                tmp = API.getNumBossLiteRunningJobs(key,t,site) 
                for status in statuses:
                        data[status[1]][t] = 0;
                        for row in tmp:
                                if row[1] == status[1]:
                                        data[row[1]][t] = row[0];
        return data


def draw_TimeGraph(key,length,span, site = 'all'):
        path= os.getcwd()
	file = path+'/image/hist__'+key+'.png'
        begin_time, end_time = make_time(length,span);
        data = gatherNumBossLiteRunningJobs(key, begin_time, end_time, span, site);
        metadata = {'title':'Job per '+key+' history', 'starttime':begin_time, 'endtime':end_time, 'span':span, 'is_cumulative':True }
        Graph = CumulativeGraph()
        Graph( data, file, metadata )


class HistStatusMonitor:

    def __init__(self, length, span, key, site = 'all', graphMonUrl = None):
        self.graphmon = graphMonUrl
        self.length = length
        self.span = span
        self.key = key
        self.site = site

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
                                <title>"""+os.environ['HOSTNAME']+""" - CRABSERVER Monitor</title>
                                </head>
                                <body>
                                <div class="container">"""
        _footer = """
                                </div>
                                </body>
                                </html>"""

        draw_TimeGraph(self.key,self.length,self.span,self.site)
        queues = API.getQueues(self.site)

        cherrypy.response.headers['Content-Type']= 'text/html'
        
        page = [_header]
        page.append('<img src="showimage" width="800" height="500" />' )

        current = API.getBossLiteRunningJobs(self.key,self.site)
        table = "<br/><table>\n"
        table += "<tr colspan=2><b>Current Status</b></tr>"
        sum = 0;
        for row in current:
                table += '<tr><td align="right">'+str(row[1])+':&nbsp;</td><td><b>'+str(row[0])+"</b></td></tr>\n"
                sum += row[0]
        table += '<tr><td align="right"><b>Total</b>:&nbsp;</td><td><b>'+str(sum)+"</b></td></tr>\n"
        table += "</table>\n"
        page.append(table)
        page.append("<br/>queues found:<br/>")
        for queue in queues:
                page.append("%s<br/>"%(queue))

        page.append(_footer)
	return page
        
    index.exposed = True
