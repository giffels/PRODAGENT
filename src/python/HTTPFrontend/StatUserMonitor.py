#!/usr/bin/env python
"""
_StatUserMonitor_
"""

from TaskTracking.TaskStateAPI import *
from pylab import *
import cherrypy
from cherrypy import tools
from numpy import *
import time, os, random, datetime, string
from matplotlib.font_manager import FontProperties
from matplotlib.backends.backend_agg import RendererAgg
from matplotlib.transforms import Value
from graphtool.graphs.common_graphs import PieGraph, BarGraph, CumulativeGraph
from graphtool.graphs.graph import TimeGraph
from calendar import *
import API




def make_time(length,span):
    end_time = time.time()-time.altzone; # end_time -= end_time % span; end_time += span;
    begin_time = end_time - length*span

    return begin_time, end_time

def make_time_data(length,span ):
    end_time = time.time(); end_time -= end_time % span
    begin_time = end_time - length*span
    return begin_time, end_time


def splitter(proxy):
    tmp = string.split(str(proxy[1]),'/')
    cn=[]
    for t in tmp:
        if t[:2]== 'CN':
            cn.append(t[3:])
    return cn

def dataCreate(from_time,begin,end,span):
    data=API.getUser(from_time)
    user = []
    x = []
    buff={}
    count=0
    for t in data:
        firstdata=t[2].timetuple()
        stamp = timegm(firstdata)
        tmp = [t[2],t[1]]
        user.append(tmp)
        x.append(stamp)
    range_time= range(begin,end,span)
    for i in range_time:
        for t in x:
            if t > i and t < i+span:
                count +=1 
                buff[i]= count
    return buff ,user

def getData_andDraw(length,span,from_time):
    path= os.getcwd()
    file = path+'/image/user.png'
    begin_time, end_time = make_time_data(length,span)
    data1, user = dataCreate(from_time,begin_time, end_time,span)
#	os.system("echo "+str(data1)+" >> test")
    data={'User': data1}
    metadata = {'title':'User Statistics', 'starttime':begin_time, 'endtime':end_time, 'span':span, 'is_cumulative':True }
    Graph = CumulativeGraph()
    Graph( data, file, metadata )
    return user

class StatUserMonitor:
    def __init__(self,length, span,from_time, graphMonUrl = None):
        self.graphmon = graphMonUrl
 	self.length = length
        self.span = span
        self.from_time = from_time

    @cherrypy.expose
    def showimage(self):
        cherrypy.response.headers['Content-Type']= "image/png"
        path= os.getcwd()+'/image/user.png'

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
	
        user = getData_andDraw(self.length,self.span,self.from_time)


	page = [_header]
        page.append('<img src="showimage" width="800" height="500" />' )
	table_user = "<br/><table>\n"
	table_user += "<tr colspan=2><b>CrabServer usage by user :</b></tr>"
	sum = 0
	for row in user:
		name = API.getNameFromProxy(row[1])
		name = splitter(name)
       		table_user += "<tr><td align=\"right\">From "+str(row[0])+": </td><td><b>"+str(name[0])+"</b></td></tr>\n"
		sum +=1


	table_user += '<tr><td align="right"><b>Total</b>:&nbsp;</td><td><b>'+str(sum)+"</b></td></tr>\n"
       	table_user += "</table>\n"
        page.append(table_user)

        page.append(_footer)
   	

        return page

    index.exposed = True

