#!/usr/bin/env python
"""
_EfficiencyMonitor_

CherryPy handler for displaying the list of workflows in the PA instance

"""

from TaskTracking.TaskStateAPI import *
from pylab import *
import cherrypy
from cherrypy import tools
from numpy import *
import os
from matplotlib.font_manager import FontProperties
from matplotlib.backends.backend_agg import RendererAgg
from matplotlib.transforms import Value
from graphtool.graphs.common_graphs import PieGraph
from graphtool.graphs.common_graphs import BarGraph
from graphtool.tools.common import expand_string
import API


def draw_pie_Efficiency(Nsubmitted,Naborted,Nfailwrap,Nsuccess):
        file = os.getcwd()+'/image/efficiency.png'
        Npending = Nsubmitted-(Naborted+Nfailwrap+Nsuccess)
        data={'Pending':Npending, 'Aborted':Naborted, 'Wrapper failure':Nfailwrap, 'Success':Nsuccess}
        logging.info("=============> %s"%str(data))
        metadata = {'title':'Efficiency'}
        pie = PieGraph()
        coords = pie.run( data, file, metadata )

class EfficiencyMonitor:

    def __init__(self, site = 'all', past=0, sitename = 'all sites', graphMonUrl = None):
        self.graphmon = graphMonUrl
        self.site = site
        self.past = past
        self.sitename = sitename

    @cherrypy.expose
    def showimage_Efficiency(self):
        cherrypy.response.headers['Content-Type']= "image/png"
	path = os.getcwd()+'/image/efficiency.png'
	f = open(path, "rb")
	contents = f.read()
        f.close()
        return   contents

    def index(self):

        _header = """
                                <html>
                                <head>
                                <title>CRABSERVER Monitor</<title>
                                </head>
                                <body>
                                <div class="container">"""
        _footer = """
                                </div>
                                </body>
                                </html>"""


        Nsub = API.getNumJobs(self.site,self.past)
        Nsubmitted=float(Nsub[0][0])
        statuses = API.getNumLastBossLiteRunningJobs('status_scheduler',self.past,self.site)
        Naborted = 0
        for row in statuses:
                if row[1] == 'Aborted':
                        Naborted = row[0]
        Nsuc = API.getNumSuccessJob(self.site,self.past)
        Nsuccess=float(Nsuc[0][0])
        Nfailw=API.getNumFailWrapperJob(self.site,self.past)
        Nfailwrap=float(Nfailw[0][0])

        queues = API.getQueues(self.site)
	
        cherrypy.response.headers['Content-Type']= 'text/html'
        
	page = [_header]
        page.append("<br/><b>Submission to %s during last %i hour(s):</b><br/><br/>"%(self.sitename,self.past/3600))

	if Nsubmitted > 0:
		draw_pie_Efficiency(Nsubmitted,Naborted,Nfailwrap,Nsuccess)
		page.append('<img src="showimage_Efficiency" width="800" height="500" />' )
        else:
                page.append('<br/><b>No job submitted in this period...</b><br/><br/>')

        page.append("<br/>queues found:<br/>")
        for queue in queues:
                page.append("%s<br/>"%(queue))

	page.append(_footer)

	return page



    index.exposed = True

