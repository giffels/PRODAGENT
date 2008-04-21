#!/usr/bin/env python
"""
_TaskMonitor_

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


 
  
def draw_pie(end,not_sub,sub,part_killed,killed,arrived,submitting,unpack,partially_sub,range_sub):
        path= os.getcwd()
	file = path+'/image/task.png'
        data = {'Ended':end, 'Not Submitted':not_sub, 'Submitted':sub, 'Partially killed':part_killed, 'Killed':killed, 'Arrived':arrived, 'Submitting':submitting, 'Unpacked':unpack, 'Partially submitted':partially_sub, 'Range submitted':range_sub}
        metadata = {'title':'Graph of Task'}
        pie = PieGraph()
        coords = pie.run( data, file, metadata )



class TaskMonitor:

    def __init__(self, graphMonUrl = None):
        self.graphmon = graphMonUrl

    @cherrypy.expose
    def showimage(self):
        cherrypy.response.headers['Content-Type']= "image/png"
        path= os.getcwd()+'/image/task.png'

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
        
         
	killed=API.getNumTaskKilled()
	arrived=API.getNumTask('arrived',False)
 	submitting=API.getNumTask('submitting',False)
	unpack=API.getNumTask('unpacked',False)
	partially_sub=API.getNumTask('partially submitted',False)
	range_sub=API.getNumTask('range submitted',False)
	ended=API.getNumTaskFinished()
        notended=API.getNumTaskNotFinished()
        submitted=API.getNumTask('submitted',False)
#	partially_killed=getNumTaskPartiallyKilled()
	partially_killed=API.getNumTask('partially killed',None)
	not_sub=API.getNumTaskNotSubmitted()	
#	not_sub=getNumTask('not submitted',None)
	
	total = killed+arrived+submitting+unpack+partially_sub+range_sub+ended+notended+submitted+partially_killed+not_sub
	if total==0:
       		cherrypy.response.headers['Content-Type']= 'text/html'
		page = [_header]
		page.append('Task not found in DB')
		page.append(_footer)

	else:
		draw_pie(ended,not_sub,submitted,partially_killed,killed,arrived,submitting,unpack,partially_sub,range_sub)
        	cherrypy.response.headers['Content-Type']= 'text/html'
        
		page = [_header]
		page.append('<img src="showimage" width="800" height="500" />' )
        	page.append(_footer)
		page.append('Task in progress : '+str(notended))
	
        	
	return page



        
    index.exposed = True
