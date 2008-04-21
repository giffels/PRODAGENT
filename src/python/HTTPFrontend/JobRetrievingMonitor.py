#!/usr/bin/env python
"""
__JobRetrievingMonitor__

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
import API	


def draw_pie(cleared,not_cleared):
        path= os.getcwd()
	file = path+'/image/retrieving.png'
        data = {'Cleared': cleared, 'Not Cleared': not_cleared}
        metadata = {'title':'Done(Cleared)" vs "Done(Not Cleared)'}
        pie = PieGraph()
        coords = pie.run( data, file, metadata )



class JobRetrievingMonitor:

    def __init__(self, graphMonUrl = None):
        self.graphmon = graphMonUrl

    @cherrypy.expose
    def showimage(self):
        cherrypy.response.headers['Content-Type']= "image/png"
        path= os.getcwd()+'/image/retrieving.png'

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
        
         
	cleared = API.getNumJobCleared()
	not_cleared = API.getNumJobNotCleared()
	cherrypy.response.headers['Content-Type']= 'text/html'
	if cleared == 0 and not_cleared == 0 :
		page = [_header]
        	page.append('There are not status job cleared and not cleared' )
	        page.append(_footer)
		
	else :
		draw_pie(cleared,not_cleared)
        	page = [_header]
		page.append('<img src="showimage" width="800" height="500" />' )
        	page.append(_footer)
	
        	
	return page



        
    index.exposed = True
