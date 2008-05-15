#!/usr/bin/env python
"""
_TimeSpreadMonitor_

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
from Sites import SiteMap, SiteRegExp

def gatherLastDeltaTimeBossLiteRunningJobs(keyB, keyE,twindow, site = 'all'):
        max, Nbin, binning = API.getLastDeltaTimeBossLiteRunningJobs(keyB,keyE,twindow*3600,site)
        data = {};
        h = int(max/Nbin)+1
        I = range(0,Nbin)
#        logging.info("GATHER_DELTA ####### max=%i; h=%i;Nbin=%i"%(max,h,Nbin))
        for i in I:
#                label = (h*(i+1)/60.0)
                label = i
#                logging.info("GATHER_DELTA #######====== i=%i; label=%f"%(i,label))
                data[label] = 0;
                for row in binning:
                        if row[1] == i:
#                                logging.info("GATHER_DELTA #######======+++++ i=%i; label=%f,row[1]=%i,row[0]=%i"%(i,label,row[1],row[0]))
                                data[label]=row[0];
#        logging.info("GATHER_DELTA ==================%s,%s,%s"%(str(data),site,str(binning)))
        return data, binning, h

def draw_BarGraph(site,keyB,keyE,twindow):
        path= os.getcwd()
        filename= 'TimeSpread__'+keyE+'-'+keyB+'__'+str(twindow)+'__'+str(site)+'.png'
	file = path+'/image/'+filename
        siteRE = SiteRegExp(site)
        data, binning, h = gatherLastDeltaTimeBossLiteRunningJobs(keyB,keyE,twindow,siteRE);
        metadata = {'title':'Running time ('+keyE+'-'+keyB+') distribution for '+site+' site(s)'}
        Graph = BarGraph()
        Graph( data, file, metadata )
        return binning, h


class TimeSpreadMonitor:

    def __init__(self, twindow, keyB, keyE, site = 'all', graphMonUrl = None): 
        self.graphmon = graphMonUrl
        self.site = site
        self.siteRE = SiteRegExp(site)
        self.twindow = twindow
        self.keyB = keyB
        self.keyE = keyE

#     @cherrypy.expose
#     def showimage_torunT(self):
#         cherrypy.response.headers['Content-Type']= "image/png"
#         path= os.getcwd()+'/image/TimeSpan__submission_time-start_time.png'

# 	f = open(path, "rb")
#         contents = f.read()
#         f.close()
#         return   contents

#     @cherrypy.expose
#     def showimage_runningT(self):
#         cherrypy.response.headers['Content-Type']= "image/png"
#         path= os.getcwd()+'/image/TimeSpan__start_time-stop_time.png'

# 	f = open(path, "rb")
#         contents = f.read()
#         f.close()
#         return   contents

#     @cherrypy.expose
#     def showimage_tooutputT(self):
#         cherrypy.response.headers['Content-Type']= "image/png"
#         path= os.getcwd()+'/image/TimeSpan__stop_time-getoutput_time.png'

# 	f = open(path, "rb")
#         contents = f.read()
#         f.close()
#         return   contents
        
#     @cherrypy.expose
#     def showimage_AllT(self):
#         cherrypy.response.headers['Content-Type']= "image/png"
#         path= os.getcwd()+'/image/TimeSpan__submission_time-getoutput_time.png'

# 	f = open(path, "rb")
#         contents = f.read()
#         f.close()
#         return   contents
        


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

        binning, h = draw_BarGraph(self.site,self.keyB,self.keyE,self.twindow)
        queues = API.getQueues(self.siteRE)
        cherrypy.response.headers['Content-Type']= 'text/html'
        
        page = [_header]
        if self.twindow > 0:
                twindow_label = " for jobs submitted during last %i hour(s)"%(self.twindow)
        else:
                twindow_label = ""
        page.append("<br/><b>Running time ("+self.keyE+"-"+self.keyB+") distribution at %s%s:</b><br/><br/>"%(self.site,twindow_label))
        page.append('<img src="showimage__TimeSpread?twindow='+str(self.twindow)+'&keyB='+self.keyB+'&keyE='+self.keyE+'&site='+self.site+'" width="800" height="500" />' )
#        page.append('<img src="showimage_AllT" width="800" height="500" />' )
        page.append("<br/>queues found at "+self.site+":<br/>")
        for queue in queues:
                page.append("%s<br/>"%(queue))
        page.append("<br/><b>each bin is %i seconds, i.e. ~%.2f minutes, i.e. ~%.2f hours</b><br/>"%(h,h/60.,h/3600.))
        page.append("<table>\n")
        page.append( '<tr><td width="100px"># jobs</td><td>time bin<br/>(minutes)</td><td>time bin<br/>(hours)</td><td>bin</td></tr>'+"\n")
        tot = 0;
        for row in binning:
                tot += row[0];
#                t = "%.2f" % (h*float(row[1]+1)/60)
#                page.append( "<tr><td>"+str(row[0])+"</td><td>"+t+"</td><td>"+str(int(row[1]))+"</td></tr>\n")
                page.append( "<tr><td>%i</td><td>%.2f</td><td>%.2f</td><td>%i</td></tr>\n"%(row[0],h*(row[1]+1)/60.,h*(row[1]+1)/3600.,row[1]))
        page.append( "<tr><td>&nbsp;</td><td>&nbsp;</td><td>&nbsp;</td><td>&nbsp;</td></tr>\n")
        page.append( "<tr><td>%i</td><td colspan=3>total jobs counted</td></tr>\n"%(tot))
        page.append("</table>\n")

        page.append(_footer)
	return page
        
    index.exposed = True
