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

def gatherDeltaTimeBossLiteRunningJobs(from_time, to_time, site = 'all'):
        max, Nbin, binning = API.getDeltaTimeBossLiteRunningJobs(from_time,to_time,site)
        data = {};
        h = int(max/Nbin)+1
        I = range(0,Nbin)
        for i in I:
#                label = (h*(i+1)/60.0)
                label = i
                data[label] = 0;
                for row in binning:
                        if row[1] == i:
                                data[label]=row[0];
        return data, binning, h

def draw_BarGraph(site):
        path= os.getcwd()

	file = path+'/image/TimeSpan__submission_time-getoutput_time.png'
        data, binning, h = gatherDeltaTimeBossLiteRunningJobs('submission_time', 'getoutput_time',site);
        metadata = {'title':'Job per running time: getoutput_time-submission_time'}
        Graph = BarGraph()
        Graph( data, file, metadata )
        return binning, h


class TimeSpanMonitor:

    def __init__(self, site = 'all', graphMonUrl = None):
        self.graphmon = graphMonUrl
        self.site = site

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

        binning, h = draw_BarGraph(self.site)
        queues = API.getQueues(self.site)
        cherrypy.response.headers['Content-Type']= 'text/html'
        
        page = [_header]
        page.append('<img src="showimage_AllT" width="800" height="500" />' )
        page.append("<br/>queues found:<br/>")
        for queue in queues:
                page.append("%s<br/>"%(queue))
        page.append("<br/><b>each bin is %i seconds, i.e. ~%.2f minutes, i.e. ~%.2f hours</b><br/>"%(h,h/60.,h/3600.))
        page.append("<table>\n")
        page.append( '<tr><td width="100px"># jobs</td><td>time bin<br/>(minutes)</td><td>time bin<br/>(hours)</td><td>bin</td></tr>'+"\n")
        for row in binning:
#                t = "%.2f" % (h*float(row[1]+1)/60)
#                page.append( "<tr><td>"+str(row[0])+"</td><td>"+t+"</td><td>"+str(int(row[1]))+"</td></tr>\n")
                page.append( "<tr><td>%i</td><td>%.2f</td><td>%.2f</td><td>%i</td></tr>\n"%(row[0],h*(row[1]+1)/60.,h*(row[1]+1)/3600.,row[1]))
        page.append("</table>\n")

        page.append(_footer)
	return page
        
    index.exposed = True
