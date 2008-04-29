#!/usr/bin/env python
"""
_TimeSpanMonitor_

CherryPy handler for displaying the plot of jobs per time life span 

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

def gatherDeltaTimeBossLiteRunningJobs(from_time, to_time, Nbin):
        max, binning = API.getDeltaTimeBossLiteRunningJobs(from_time,to_time,Nbin)
        data = {};
        h = int(max/Nbin)+1
        I = range(0,Nbin)
        for i in I:
                label = int(h*(i+1)/3600)
                data[label] = 0;
                for row in binning:
                        if row[1] == i:
                                data[label]=row[0];
        return data, binning, h

def draw_BarGraph():
        path= os.getcwd()

# 	file = path+'/image/TimeSpan__submission_time-start_time.png'
#         data = gatherDeltaTimeBossLiteRunningJobs('submission_time', 'start_time',20);
#         metadata = {'title':'Job per time-to-run: start_time-submission_time'}
#         Graph = BarGraph()
#         Graph( data, file, metadata )

# 	file = path+'/image/TimeSpan__start_time-stop_time.png'
#         data = gatherDeltaTimeBossLiteRunningJobs('start_time', 'stop_time',20);
#         metadata = {'title':'Job per running time: stop_time-start_time'}
#         Graph = BarGraph()
#         Graph( data, file, metadata )

# 	file = path+'/image/TimeSpan__stop_time-getoutput_time.png'
#         data = gatherDeltaTimeBossLiteRunningJobs('stop_time', 'getoutput_time',20);
#         metadata = {'title':'Job per running time: getoutput_time-stop_time'}
#         Graph = BarGraph()
#         Graph( data, file, metadata )

	file = path+'/image/TimeSpan__submission_time-getoutput_time.png'
        data, binning, h = gatherDeltaTimeBossLiteRunningJobs('submission_time', 'getoutput_time',300);
        metadata = {'title':'Job per running time: getoutput_time-submission_time'}
        Graph = BarGraph()
        Graph( data, file, metadata )
        return binning, h


class TimeSpanMonitor:

    def __init__(self, graphMonUrl = None):
        self.graphmon = graphMonUrl

    @cherrypy.expose
    def showimage_torunT(self):
        cherrypy.response.headers['Content-Type']= "image/png"
        path= os.getcwd()+'/image/TimeSpan__submission_time-start_time.png'

	f = open(path, "rb")
        contents = f.read()
        f.close()
        return   contents

    @cherrypy.expose
    def showimage_runningT(self):
        cherrypy.response.headers['Content-Type']= "image/png"
        path= os.getcwd()+'/image/TimeSpan__start_time-stop_time.png'

	f = open(path, "rb")
        contents = f.read()
        f.close()
        return   contents

    @cherrypy.expose
    def showimage_tooutputT(self):
        cherrypy.response.headers['Content-Type']= "image/png"
        path= os.getcwd()+'/image/TimeSpan__stop_time-getoutput_time.png'

	f = open(path, "rb")
        contents = f.read()
        f.close()
        return   contents
        
    @cherrypy.expose
    def showimage_AllT(self):
        cherrypy.response.headers['Content-Type']= "image/png"
        path= os.getcwd()+'/image/TimeSpan__submission_time-getoutput_time.png'

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

        binning, h = draw_BarGraph()
        cherrypy.response.headers['Content-Type']= 'text/html'
        
        page = [_header]
        page.append('<img src="showimage_AllT" width="800" height="500" />' )
#         page.append('<img src="showimage_torunT" width="800" height="500" />' )
#         page.append('<img src="showimage_runningT" width="800" height="500" />' )
#         page.append('<img src="showimage_tooutputT" width="800" height="500" />' )
        page.append("<table>\n")
        page.append( '<tr><td width="100px"># jobs</td><td>time bin (hours)</td>'+"\n")
        for row in binning:
                t = "%.2f" % (h*float(row[1]+1)/3600)
                page.append( "<tr><td>"+str(row[0])+"</td><td>"+t+"</td>\n")
        page.append("</table>\n")

        page.append(_footer)
	return page
        
    index.exposed = True
