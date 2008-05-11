#!/usr/bin/env python
"""
_HistMonitor_

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


def draw_TimeKeyGraph(key,length,span, site = 'all'):
        path= os.getcwd()
	file = path+'/image/hist__'+key+'.png'
        begin_time, end_time = make_time(length,span);
        data = gatherNumBossLiteRunningJobs(key, begin_time, end_time, span, site);
        metadata = {'title':'Job per '+key+' history', 'starttime':begin_time, 'endtime':end_time, 'span':span, 'is_cumulative':True }
        Graph = CumulativeGraph()
        Graph( data, file, metadata )



def gatherHWData(Nbins):
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
                load1[time]  = float(line.split('l')[1].split()[2].split(',')[0])
                load5[time]  = float(line.split('l')[1].split()[3].split(',')[0])
                load15[time] = float(line.split('l')[1].split()[4])
                mem[time]    = float(line.split('l')[1].split()[6])
                cached[time] = float(line.split('l')[1].split()[8])
                swap[time]   = float(line.split('l')[1].split()[10])
        return begin_time, end_time, load1, load5, load15, mem, cached, swap

def draw_TimeHWGraph(Nbins):
        path= os.getcwd()
        begin_time, end_time, load1, load5, load15, mem, cached, swap = gatherHWData(Nbins);
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




class HistMonitor:

    def __init__(self, length, span, key, site = 'all', graphMonUrl = None):
        self.graphmon = graphMonUrl
        self.length = length
        self.span = span
        self.key = key
        self.site = site
        self.Nbins = int(self.length*self.span/180.)

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
        
        if self.key == 'hardware sensors':
                if NOmonitorMSG == 'OK':
                        draw_TimeHWGraph(self.Nbins)
                        page.append('<img src="showimage_cpu" width="800" height="500" />' )
                        page.append('<img src="showimage_mem" width="800" height="500" />' )
                        page.append('<img src="showimage_swp" width="800" height="500" />' )
                else:
                        page.append('<br/>'+NOmonitorMSG+'<br/>')
        else:
                draw_TimeKeyGraph(self.key,self.length,self.span,self.site)
                queues = API.getQueues(self.site)
                page.append('<img src="showimage__hist__'+self.key+'" width="800" height="500" />' )
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
