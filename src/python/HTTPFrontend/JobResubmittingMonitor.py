#!/usr/bin/env python
"""
_JobResubmittingMonitor_

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




def draw_pie(exit_code,wrap_code):
        file = os.getcwd()+'/image/jobresubmitting1.png'
        data={'Application Error':exit_code, 'Wrapper Error': wrap_code}
        metadata = {'title':'Graph of Error'}
        pie = PieGraph()
        coords = pie.run( data, file, metadata )

def draw_pie_error_code_Wrapper(Wrapper_Error_List):
        file = os.getcwd()+'/image/jobresubmitting2.png'
        data = {}	
        for i in Wrapper_Error_List:
                data['Code '+str(i[0])] = i[1]
        metadata = {'title':'Error Code Wrapper'}
        pie = PieGraph()
        coords = pie.run( data, file, metadata )


def draw_pie_error_code_Application(Application_Error_List):
        file =  os.getcwd()+'/image/jobresubmitting3.png'
        data= {}
        for i in Application_Error_List:
                data['Code '+str(i[0])] = i[1]

        metadata = {'title':'Error Code Application'}
        pie = PieGraph()
        coords = pie.run( data, file, metadata )








class JobResubmittingMonitor:

    def __init__(self, graphMonUrl = None):
        self.graphmon = graphMonUrl

    @cherrypy.expose
    def showimage_numError_NumWrapper(self):
        cherrypy.response.headers['Content-Type']= "image/png"
	path = os.getcwd()+'/image/jobresubmitting1.png'
	f = open(path, "rb")
	contents = f.read()

        f.close()
        return   contents

    @cherrypy.expose
    def showimage_error_codeApplication(self):
        cherrypy.response.headers['Content-Type']= "image/png"
        path = os.getcwd()+'/image/jobresubmitting2.png'
	f = open(path, "rb")
        contents = f.read()
        f.close()
        return   contents
	
    @cherrypy.expose
    def showimage_errorCodeWrapper(self):
        cherrypy.response.headers['Content-Type']= "image/png"
        path = os.getcwd()+'/image/jobresubmitting3.png'
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




        
	jobresubmitting=API.getNumJobResubmitting()
        listWrapError=API.getList_Wapper_Error()
	listApplError=API.getList_Application_Error()
	numApplError=API.getNum_Application_Error()
	numWrapError=API.getNum_Wrap_Error()

        
	
        cherrypy.response.headers['Content-Type']= 'text/html'
        
	page = [_header]
        
	if numApplError !=0 and numWrapError !=0:
		draw_pie(numApplError,numWrapError)
		page.append('<img src="showimage_numError_NumWrapper" width="800" height="500" />')

	if len(listApplError) !=0:
		draw_pie_error_code_Application(listApplError)
        	page.append('<img src="showimage_error_codeApplication" width="800" height="500" />' )
	
	if len(listWrapError) !=0:
		draw_pie_error_code_Wrapper(listWrapError)
		page.append('<img src="showimage_errorCodeWrapper" width="800" height="500" />' )


	page.append(_footer)

        page.append('Job in ReSubmitting : '+str(jobresubmitting))


	return page



    index.exposed = True

