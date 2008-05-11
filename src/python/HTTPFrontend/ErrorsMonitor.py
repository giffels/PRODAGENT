#!/usr/bin/env python
"""
_ErrorsMonitor_

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




def draw_pie_WA_Errors(numWerrors,numAerrors):
        file = os.getcwd()+'/image/errors.png'
        data={'Application Errors':numWerrors, 'Wrapper Errors':numAerrors}
        metadata = {'title':'Wrapper/Application Errors'}
        pie = PieGraph()
        coords = pie.run( data, file, metadata )

def draw_pie_Wrapper_ErrorCodes(listWerrors):
        file = os.getcwd()+'/image/Werrors.png'
        data = {}	
        for i in listWerrors:
                data['Code '+str(i[0])] = i[1]
        metadata = {'title':'Wrapper Error Codes'}
        pie = PieGraph()
        coords = pie.run( data, file, metadata )


def draw_pie_Application_ErrorCodes(listAerrors):
        file =  os.getcwd()+'/image/Aerrors.png'
        data= {}
        for i in listAerrors:
                data['Code '+str(i[0])] = i[1]

        metadata = {'title':'Application Error Codes'}
        pie = PieGraph()
        coords = pie.run( data, file, metadata )








class ErrorsMonitor:

    def __init__(self, site = 'all', past=0, sitename = 'all sites', graphMonUrl = None):
            self.graphmon = graphMonUrl
            self.site = site
            self.past = past
            self.sitename = sitename
                                            


    @cherrypy.expose
    def showimage_WA_Errors(self):
        cherrypy.response.headers['Content-Type']= "image/png"
	path = os.getcwd()+'/image/errors.png'
	f = open(path, "rb")
	contents = f.read()

        f.close()
        return   contents

    @cherrypy.expose
    def showimage_Application_ErrorCodes(self):
        cherrypy.response.headers['Content-Type']= "image/png"
        path = os.getcwd()+'/image/Aerrors.png'
	f = open(path, "rb")
        contents = f.read()
        f.close()
        return   contents
	
    @cherrypy.expose
    def showimage_Wrapper_ErrorCodes(self):
        cherrypy.response.headers['Content-Type']= "image/png"
        path = os.getcwd()+'/image/Werrors.png'
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

        listWerrors=API.getList_WrapperErrors(self.site,self.past)
	listAerrors=API.getList_ApplicationErrors(self.site,self.past)
        numAerrors=API.getNum_ApplicationErrors(self.site,self.past)
        numWerrors=API.getNum_WrapperErrors(self.site,self.past)
        queues = API.getQueues(self.site)
	
        cherrypy.response.headers['Content-Type']= 'text/html'
        
	page = [_header]
        page.append("<br/><b>Submission to %s during last %i hour(s):</b><br/><br/>"%(self.sitename,self.past/3600))

#	if numAerrors !=0 and numWerrors !=0:
#		draw_pie_WA_Errors(numAerrors,numWerrors)
#		page.append('<img src="showimage_WA_Errors" width="800" height="500" />')

	if len(listAerrors) !=0:
		draw_pie_Application_ErrorCodes(listAerrors)
        	page.append('<img src="showimage_Application_ErrorCodes" width="800" height="500" />' )
        else:
                page.append('<br/><b>No job submitted in this period...</b><br/><br/>')
	
	if len(listWerrors) !=0:
		draw_pie_Wrapper_ErrorCodes(listWerrors)
		page.append('<img src="showimage_Wrapper_ErrorCodes" width="800" height="500" />' )
        else:
                page.append('<br/><b>No job submitted in this period...</b><br/><br/>')

        page.append("<br/>queues found:<br/>")
        for queue in queues:
                page.append("%s<br/>"%(queue))

	page.append(_footer)

	return page



    index.exposed = True

