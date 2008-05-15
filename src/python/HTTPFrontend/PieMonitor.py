#!/usr/bin/env python
"""
_PieMonitor_

CherryPy handler for displaying the plot of workflow summary

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
from graphtool.graphs.common_graphs import PieGraph
# from graphtool.graphs.graph import TimeGraph
import API
from Sites import SiteMap, SiteRegExp

def gatherDistribution(twindow,key,siteRE):
        distribution = API.getNumLastBossLiteRunningJobs(key,twindow*3600,siteRE)
        statuses = API.getBossLiteRunningJobs(key,siteRE);
        data = {};
        sum = 0
        for status in statuses:
                data[status[1]] = 0;
                for row in distribution:
                        if row[1] == status[1]:
                                data[row[1]] = row[0];
                                sum += row[0]
        return data, sum

def draw_pie_Distribution(twindow,key,site):
        filename= 'pie__'+key+'__'+str(twindow)+'__'+site+'.png'
        file= os.getcwd()+'/image/'+filename
        siteRE = SiteRegExp(site)
        if key == 'wrapper errors':
                ErrorsList=API.getList_WrapperErrors(siteRE,twindow*3600)
                data = {}	
                for i in ErrorsList:
                        data['Code '+str(i[0])] = i[1]
                metadata = {'title':'Wrapper Error Codes'}
                sum = len(ErrorsList) 
        elif key == 'application errors':
                ErrorsList=API.getList_ApplicationErrors(siteRE,twindow*3600)
                data = {}	
                for i in ErrorsList:
                        data['Code '+str(i[0])] = i[1]
                metadata = {'title':'Application Error Codes'}
                sum = len(ErrorsList) 
        elif key == 'efficiency':
                Nsub = API.getNumJobs(siteRE,twindow*3600)
                Nsubmitted=float(Nsub[0][0])
                statuses = API.getNumLastBossLiteRunningJobs('status_scheduler',twindow*3600,siteRE)
                Naborted = 0
                for row in statuses:
                        if row[1] == 'Aborted':
                                Naborted = row[0]
                Nsuc = API.getNumSuccessJob(siteRE,twindow*3600)
                Nsuccess=float(Nsuc[0][0])
                Nfailw=API.getNumFailWrapperJob(siteRE,twindow*3600)
                Nfailwrap=float(Nfailw[0][0])
                Npending = Nsubmitted-(Naborted+Nfailwrap+Nsuccess)
                data={'Pending':Npending, 'Aborted':Naborted, 'Wrapper failure':Nfailwrap, 'Success':Nsuccess}
                logging.info("=============> %s"%str(data))
                metadata = {'title':'Efficiency'}
                sum = Nsubmitted
        else:
                data, sum = gatherDistribution(twindow,key,siteRE)
                metadata = {'title':'Job '+key+' distribution for '+site+' site(s)'}
        pie = PieGraph()
        if sum > 0:
                coords = pie.run( data, file, metadata )
        return sum


class PieMonitor:

    def __init__(self, twindow, key, site = 'all', graphMonUrl = None):
        self.graphmon = graphMonUrl
        self.site = site
        self.siteRE = SiteRegExp(site)
        self.twindow = twindow
        self.key = key

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


        queues = API.getQueues(self.siteRE)

        cherrypy.response.headers['Content-Type']= 'text/html'
        
        page = [_header]
        sum = draw_pie_Distribution(self.twindow,self.key,self.site)
        if self.twindow > 0:
                twindow_label = " for jobs submitted during last %i hour(s)"%(self.twindow)
        else:
                twindow_label = ""
        page.append("<br/><b>%s distribution at %s%s:</b><br/><br/>"%(self.key,self.site,twindow_label))
        if sum > 0:
                page.append('<img src="showimage__pie?twindow='+str(self.twindow)+'&key='+self.key+'&site='+self.site+'" width="800" height="500" />' )
#                page.append('<img src="showimage" width="800" height="500" />' )
        else:
                page.append('<br/><b>No job submitted in this period...</b><br/><br/>')

        
        current = API.getBossLiteRunningJobs('status_scheduler',self.siteRE)
        table = "<br/><table>\n"
        table += "<tr colspan=2><b>Current Overall Status of the server for %s:</b></tr>"%(self.site)
        sum = 0;
        for row in current:
                table += '<tr><td align="right">'+str(row[1])+':&nbsp;</td><td><b>'+str(row[0])+"</b></td></tr>\n"
                sum += row[0]
        table += '<tr><td align="right"><b>Total</b>:&nbsp;</td><td><b>'+str(sum)+"</b></td></tr>\n"
        table += "</table>\n"
        page.append(table)
        page.append("<br/>queues found belonging to %s:<br/>"%(self.site))
        for queue in queues:
                page.append("%s<br/>"%(queue))

        page.append(_footer)
	return page
        
    index.exposed = True
