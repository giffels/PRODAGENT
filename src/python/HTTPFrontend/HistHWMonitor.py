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
from ProdAgentCore.Configuration import loadProdAgentConfiguration


try:
        config = loadProdAgentConfiguration()
        compCfg = config.getConfig("HTTPFrontend")
except StandardError, ex:
        msg = "Error reading configuration:\n"
        msg += str(ex)
        raise RuntimeError, msg

if not compCfg.has_key("HWmonitorLogFile"):
        # no LoadLogFile so write an empty plot
        # maybe a message that LoadLogFile need to be specified to have
        # this plot
        NOmonitorMSG = 'HWmonitor plots not available: no HWmonitorLogFile specified in configuration file...';
else:
        NOmonitorMSG = 'OK';
        logFilename = compCfg['HWmonitorLogFile'];


def gatherData(Nbins):
        load1 = {}; load5 = {}; load15 = {}; mem = {}; cached = {}; swap = {};
        logLines = open(logFilename,'r').readlines()
        end = len(logLines)-1;
        start = end - Nbins;
        if start < 0:
                start = 0;
        begin_time = int(logLines[start].split()[1]);
        end_time = int(logLines[end].split()[1]);
        for line in logLines:
                time = int(line.split()[1]);
                load1[time]  = float(line.split(',')[3].split()[2].split(',')[0])
                load5[time]  = float(line.split(',')[4])
                load15[time] = float(line.split(',')[5].split()[0])
                mem[time]    = float(line.split(',')[5].split()[2])
                cached[time] = float(line.split(',')[5].split()[4])
                swap[time]   = float(line.split(',')[5].split()[6])
        return begin_time, end_time, load1, load5, load15, mem, cached, swap

def draw_TimeGraph(Nbins):
        path= os.getcwd()
        begin_time, end_time, load1, load5, load15, mem, cached, swap = gatherData(Nbins);
	file = path+'/image/mem.png'
        metadata = {'title':'Memory Usage History', 'starttime':begin_time, 'endtime':end_time, 'span':180, 'is_cumulative':True } # span:180 is the 3min crontab freq.
        Graph = CumulativeGraph()
        data = {'cached (%)':cached, 'mem (%)':mem }
        Graph( data, file, metadata )
	file = path+'/image/swp.png'
        metadata = {'title':'Swap Usage History', 'starttime':begin_time, 'endtime':end_time, 'span':180, 'is_cumulative':True } # span:180 is the 3min crontab freq.
        Graph = CumulativeGraph()
        data = {'swap (%)':swap}
        Graph( data, file, metadata )
	file = path+'/image/cpu.png'
        metadata = {'title':'CPU Load History', 'starttime':begin_time, 'endtime':end_time, 'span':180, 'is_cumulative':True } # span:180 is the 3min crontab freq.
        Graph = CumulativeGraph()
        data = {'load 5m':load5 } #, 'load 5m':load5, 'load 15m':load15  }
        Graph( data, file, metadata )


class HistHWMonitor:

    def __init__(self, Nbins, graphMonUrl = None):
        self.graphmon = graphMonUrl
        self.Nbins = Nbins

    @cherrypy.expose
    def showimage_mem(self):
        cherrypy.response.headers['Content-Type']= "image/png"
        path= os.getcwd()+'/image/mem.png'

	f = open(path, "rb")
        contents = f.read()
        f.close()
        return   contents

    @cherrypy.expose
    def showimage_swp(self):
        cherrypy.response.headers['Content-Type']= "image/png"
        path= os.getcwd()+'/image/swp.png'

	f = open(path, "rb")
        contents = f.read()
        f.close()
        return   contents

    @cherrypy.expose
    def showimage_cpu(self):
        cherrypy.response.headers['Content-Type']= "image/png"
        path= os.getcwd()+'/image/cpu.png'

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
        cherrypy.response.headers['Content-Type']= 'text/html'
   
        page = [_header]
        
        if NOmonitorMSG == 'OK':
                draw_TimeGraph(self.Nbins)
                page.append('<img src="showimage_cpu" width="800" height="500" />' )
                page.append('<img src="showimage_mem" width="800" height="500" />' )
                page.append('<img src="showimage_swp" width="800" height="500" />' )
        else:
                page.append('<br/>'+NOmonitorMSG+'<br/>')
                
        page.append(_footer)
	return page
        
    index.exposed = True
