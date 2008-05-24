#!/usr/bin/env python
"""
_HTTPFrontend_

Component that runs a CherryPy based web server to provide HTTP
access to the JobCreator cache.

May also add some interactive monitoring as time goes on.

Introduces a dependency on the cherrypy package

"""
import socket
import logging
import time, os, random, datetime
import cherrypy
from cherrypy.lib.static import serve_file
from ProdAgentCore.Configuration import prodAgentName
from ProdAgentCore.Configuration import loadProdAgentConfiguration

import ProdAgentCore.LoggingUtils as LoggingUtils

from graphtool.graphs.common_graphs import PieGraph, BarGraph, CumulativeGraph
from graphtool.graphs.graph import TimeGraph

from TaskMonitor import TaskMonitor
from JobRetrievingMonitor import JobRetrievingMonitor
from JobResubmittingMonitor import JobResubmittingMonitor
from ResourceMonitors import ResourceDetails,ResourceStatus
# to be removed?
from HistStatusMonitor import HistStatusMonitor
from StatUserMonitor import StatUserMonitor
from ComponentMonitor import ComponentMonitor

from HistMonitor import HistMonitor
from HistHWMonitor import HistHWMonitor
from PieMonitor import PieMonitor
# from TimeSpanMonitor import TimeSpanMonitor
from TimeSpreadMonitor import TimeSpreadMonitor
from ErrorsMonitor import ErrorsMonitor
from LastStatusMonitor import LastStatusMonitor
from LastSitesMonitor import LastSitesMonitor
from EfficiencyMonitor import EfficiencyMonitor
from LastStatusDestinationMonitor import LastStatusDestinationMonitor
import API
from Sites import SiteMap, SiteRegExp



def getLocalDBSURL():
    try:
        config = loadProdAgentConfiguration()
    except StandardError, ex:
        msg = "Error reading configuration:\n"
        msg += str(ex)
        logging.error(msg)
        raise RuntimeError, msg

    try:
        dbsConfig = config.getConfig("LocalDBS")
    except StandardError, ex:
        msg = "Error reading configuration for LocalDBS:\n"
        msg += str(ex)
        logging.error(msg)
        raise RuntimeError, msg

    return dbsConfig.get("DBSURL", None)

class Root:
    """
    _Root_

    Main index page for the component, will appear as the index page
    of the toplevel address

    """
    def __init__(self, myUrl, Sites):
        self.myUrl = myUrl
        self.Sites = Sites


#
# generic (parametric) IMAGEs
#

    @cherrypy.expose
    def showimage__hist(self,length,span,key,site='all'):
        if key == 'mem' or key == 'cpu' or key == 'swp':
            Nbins = int(int(length)*int(span)/180.)
            filename = 'hist__'+key+'__'+str(Nbins)+'.png'
        else:
            filename= 'hist__'+key+'__'+str(length)+'__'+str(span)+'__'+str(site)+'.png'
        cherrypy.response.headers['Content-Type']= "image/png"
        path= os.getcwd()+'/image/'+filename
        f = open(path, "rb")
        contents = f.read()
        f.close()
        return   contents

    @cherrypy.expose
    def showimage__pie(self,twindow,key,site):
        filename= 'pie__'+key+'__'+str(twindow)+'__'+str(site)+'.png'
        cherrypy.response.headers['Content-Type']= "image/png"
        path= os.getcwd()+'/image/'+filename
        f = open(path, "rb")
        contents = f.read()
        f.close()
        return   contents

    @cherrypy.expose
    def showimage__TimeSpread(self,twindow,keyB,keyE,site):
        filename= 'TimeSpread__'+keyE+'-'+keyB+'__'+str(twindow)+'__'+str(site)+'.png'
        cherrypy.response.headers['Content-Type']= "image/png"
        path= os.getcwd()+'/image/'+filename
        f = open(path, "rb")
        contents = f.read()
        f.close()
        return   contents

#
# generic (parametric) PAGEs
#
    def history(self, length=24, span=3600, key='status_scheduler', site = 'all', graphMonUrl = None):
        length=int(length)
        if span == 'hours':
            span=3600
        elif span == 'days':
            span = 86400        
        page = HistMonitor(length,span,key,site,graphMonUrl).index()
        return page
    history.exposed = True

    def summary(self, length=24, span=1, key='status_scheduler', site = 'all', graphMonUrl = None):
        length=int(length)
        if span == 'hours':
            span=1
        elif span == 'days':
            span = 24        
        twindow = length*span
        page = PieMonitor(twindow,key,site,graphMonUrl).index()
        return page
    summary.exposed = True

    @cherrypy.expose
    def TimeSpread(self, length=24, span=1, keyB='submission_time', keyE='getoutput_time', site = 'all', graphMonUrl = None):
        length=int(length)
        if span == 'hours':
            span=1
        elif span == 'days':
            span = 24        
        twindow = length*span
        page = TimeSpreadMonitor(twindow,keyB,keyE,site,graphMonUrl).index()
        return page

#
# main PAGE
#
        
    def index(self):
        html = """<html><head><title>%s CrabServer monitor</title></head><body><h1>%s - CRAB WEB MONITOR</h1>\n """ % (
            os.environ['HOSTNAME'], os.environ['HOSTNAME'], )
	html += "\n"

# Current Status

        html += "<h2>Current Status</h2>"
        html += "<table width=\"100"+'%'+"\">\n"
        html += "<tr><th  align=\"left\" width=\"32"+'%'+"\">Service</th><th  align=\"left\">Description</th></tr>\n"
        html += "<tr><td width=\"32"+'%'+"\"><a href=\"task\">Task</a></td>\n" 
        html += "<td>Task status information </td></td>\n"

        html += "<tr><td width=\"32"+'%'+"\"><a href=\"resubmitting\">Job Resubmitting</a></td>\n" 
        html += "<td>Show number of job in resubmitting</td></td>\n"
        html += "<tr><td><a href=\"retrieving\">Job Retrieving </a></td>\n"
        html += "<td>Done Cleared vs Done not Cleared</td></td>\n"
	
	html += "<tr><td><a href=\"status_components\">Components and Services Status</a></td>\n"
        html += "<td>Show status of both server components and other services</td></td>\n"


        html += """</table>"""


# History Plots:
        html += "<h2>History Plots</h2>" 
        html += "History plot for the last custom period of hours or days<br/><br/>"
        html += '<form action="history" method="get" target="_blank">'
        html += 'key: '
        html += '<select name="key" style="width:160px"><option>hardware sensors</option></select>'
        html += ' for last '
        html += '<input type="text" name="length" size=4 value=24>'
        html += '<select name="span" style="width:80px"><option>hours</option><option>days</option></select>'
        html += '<input type="submit" value="Show History Plot"/>'
        html += '</form>'

        html += '<form action="history" method="get" target="_blank">'
        html += 'key: '
        html += '<select name="key" style="width:160px"><option>status_scheduler</option><option>process_status</option></select>'
        html += ' for last '
        html += '<input type="text" name="length" size=4 value=24>'
        html += '<select name="span" style="width:80px"><option>hours</option><option>days</option></select>'
        html += '<input type="submit" value="Show History Plot"/>'
        html += ' for '
        html += '<select name="site">'
        html += '<option>all</option>'
        sitesnames = SiteMap().keys()
        sitesnames.sort()
        for sitename in sitesnames:
            html += '<option>'+sitename+'</option>'
        html += '</select>'
        html += ' site(s) '
        html += '</form>'

# Summary Pies:
        html += "<br/><h2>Summary Pies</h2>" 
        html += "Summary Pies for the past custom-sized time window<br/>"
        html += "<i>a time-window of 0 (zero) means all available statistics</i><br/><br/>"
        html += '<form action="summary" method="get" target="_blank">'
        html += 'key: '
        html += '<select name="key" style="width:160px">'
        html += '<option>status_scheduler</option>'
        html += '<option>process_status</option>'
        html += '<option>wrapper errors</option>'
        html += '<option>application errors</option>'
        html += '<option>efficiency</option>'
        html += '</select>'
        html += ' for last '
        html += '<input type="text" name="length" size=4 value=0>'
        html += '<select name="span" style="width:80px"><option>hours</option><option>days</option></select>'
        html += '<input type="submit" value="Show Summary Plot"/>'
        html += ' for '
        html += '<select name="site">'
        html += '<option>all</option>'
        sitesnames = SiteMap().keys()
        sitesnames.sort()
        for sitename in sitesnames:
            html += '<option>'+sitename+'</option>'
        html += '</select>'
        html += ' site(s) '
        html += '</form>'

# TimeSpread Plots:
        html += "<br/><h2>Time Spread Distribution</h2>" 
        html += "Running time distribution for the past custom-sized time window<br/>"
        html += "<i>a time-window of 0 (zero) means all available statistics</i><br/><br/>"
        html += '<form action="TimeSpread" method="get" target="_blank">'
        html += 'from: '
        html += '<select name="keyB" style="width:160px">'
        html += '<option>submission_time</option>'
        html += '</select>'
        html += ' to: '
        html += '<select name="keyE" style="width:160px">'
        html += '<option>getoutput_time</option>'
        html += '</select>'
        html += ' for last '
        html += '<input type="text" name="length" size=4 value=0>'
        html += '<select name="span" style="width:80px"><option>hours</option><option>days</option></select>'
        html += '<input type="submit" value="Show Summary Plot"/>'
        html += ' for '
        html += '<select name="site">'
        html += '<option>all</option>'
        sitesnames = SiteMap().keys()
        sitesnames.sort()
        for sitename in sitesnames:
            html += '<option>'+sitename+'</option>'
        html += '</select>'
        html += ' site(s) '
        html += '</form>'

# Overall:
        html += "<br/><h2>Overall Statistics</h2>" 
        html += "<table width=\"100"+'%'+"\">\n"
        html += "<tr><th align=\"left\">Service</th><th align=\"left\">Description</th></tr>\n"

#         html += "<tr><td width=\"32"+'%'+"\"><a href=\"errors_all\">Errors Pies</a></td>\n" 
#         html += "<td>Application and wrapper errors distributions</td></td>\n"

#         html += "<tr><td width=\"32"+'%'+"\"><a href=\"timespan_all\">job time span</a></td>\n" 
#         html += "<td>Number of job per life time span (per time-to-run, per running-time, per time-to-output)</td></td>\n"

#        html += "<tr><td width=\"32"+'%'+"\">Scheduler-status: <a href=\"lastStatus1_all\">1h</a>/<a href=\"lastStatus2_all\">2h</a>/<a href=\"lastStatus3_all\">3h</a>/<a href=\"lastStatus6_all\">6h</a>/<a href=\"lastStatus12_all\">12h</a>/<a href=\"lastStatus24_all\">24h</a>/<a href=\"lastStatusAll_all\">All</a></td>\n"
#        html += "<td>Current job status pies for job submittend in the last 1/2/3/6/12/24 hour(s) period</td></td>\n"
#
        html += "<tr><td width=\"32"+'%'+"\">Destination sites: <a href=\"lastSites1_all\">1h</a>/<a href=\"lastSites2_all\">2h</a>/<a href=\"lastSites3_all\">3h</a>/<a href=\"lastSites6_all\">6h</a>/<a href=\"lastSites12_all\">12h</a>/<a href=\"lastSites24_all\">24h</a>/<a href=\"lastSitesAll_all\">All</a></td>\n"
        html += "<td>Current job destination pie for job submittend in the last 1/2/3/6/12/24 hour(s) period</td></td>\n"
#
        html += "<tr><td width=\"32"+'%'+"\">Status per Destination sites: <a href=\"lastStatusSites1_all\">1h</a>/<a href=\"lastStatusSites2_all\">2h</a>/<a href=\"lastStatusSites3_all\">3h</a>/<a href=\"lastStatusSites6_all\">6h</a>/<a href=\"lastStatusSites12_all\">12h</a>/<a href=\"lastStatusSites24_all\">24h</a>/<a href=\"lastStatusSitesAll_all\">All</a></td>\n"
        html += "<td>Current per-site job status distribution for jobs submitted in the last 1/2/3/6/12/24 hour(s) period</td></td>\n"

	html += "<tr><td width=\"32"+'%'+"\">Users monitor: <a href=\"%s/stat_US_24\">24</a>&nbsp;/&nbsp;<a href=\"%s/stat_US_7\">7</a>&nbsp;/&nbsp;<a href=\"%s/stat_US_30\">30</a></td>\n" % (
            self.myUrl,self.myUrl,self.myUrl,)
        html += "<td>History plot of CrabServer usage by different users for last 24 hours, 7 days or month</td></td>\n"

        html += "</table>\n"

# # # # Sites:
# # #         html += "<h2>Per-Sites statistics:</h2>"
# # #         html += "<table width=\"100"+'%'+"\">\n"

# # #         for site in self.Sites.keys():
# # #             html += "<tr><td>&nbsp;</td></tr><tr><td wREMOVEMEidth=\"32"+'%'+"\"><b>%s</b></td></tr><tr>"%(site)
# # # #            html += "<td>Handling History: <a href=\"hist_procsstat24_%s\">24h</a>/<a href=\"hist_procsstat7_%s\">7d</a>/<a href=\"hist_procsstat30_%s\">30d</a></td>\n" %(site,site,site,)
# # # #            html += "<td>Status History: <a href=\"hist_schedstat24_%s\">24h</a>/<a href=\"hist_schedstat7_%s\">7d</a>/<a href=\"hist_schedstat30_%s\">30d</a></td>\n"%(site,site,site,)
# # # #            html += "<td>Last Status Pies: <a href=\"lastStatus1_%s\">1h</a>/<a href=\"lastStatus2_%s\">2h</a>/<a href=\"lastStatus3_%s\">3h</a>/<a href=\"lastStatus6_%s\">6h</a>/<a href=\"lastStatus12_%s\">12h</a>/<a href=\"lastStatus24_%s\">24h</a>/<a href=\"lastStatus48_%s\">48h</a>/<a href=\"lastStatusAll_%s\">All</a></td>\n"%(site,site,site,site,site,site,site,site)
# # #             html += "<td>Last Error Pies: <a href=\"lastErrors1_%s\">1h</a>/<a href=\"lastErrors2_%s\">2h</a>/<a href=\"lastErrors3_%s\">3h</a>/<a href=\"lastErrors6_%s\">6h</a>/<a href=\"lastErrors12_%s\">12h</a>/<a href=\"lastErrors24_%s\">24h</a>/<a href=\"lastErrorsAll_%s\">All</a></td>\n"%(site,site,site,site,site,site,site)
# # #             html += "<td>Last Efficiency Pies: <a href=\"lastEfficiency1_%s\">1h</a>/<a href=\"lastEfficiency2_%s\">2h</a>/<a href=\"lastEfficiency3_%s\">3h</a>/<a href=\"lastEfficiency6_%s\">6h</a>/<a href=\"lastEfficiency12_%s\">12h</a>/<a href=\"lastEfficiency24_%s\">24h</a>/<a href=\"lastEfficiencyAll_%s\">All</a></td>\n"%(site,site,site,site,site,site,site)
# # # #            html += "<td><a href=\"errors_%s\">Error pies</a></td>\n" % (site,)
# # #             html += "<td><a href=\"timespan_%s\">job time span</a></td>\n" % (site,)
            
# # # #            html += "<tr><th align=\"left\"><a href=\"\">"+str(site)+"</th><th align=\"left\">"+str(vars().keys())+"</th></tr>\n"

# # # # Foot

# # #         html += "</table>\n"


        html +="<br/><h6>version "+os.environ['CRAB_SERVER_VERSION']+"</h6>"
        html += """</body></html>"""
        
#        html +="version "+os.environ['CRAB_SERVER_VERSION']
	return html
    index.exposed = True

class Downloader:
    """
    _Downloader_

    Serve files from the JobCreator Cache via HTTP

    """
    def __init__(self, rootDir):
        self.rootDir = rootDir

    def index(self, filepath):
        """
        _index_

        index response to download URL, serves the file
        requested

        """
        pathInCache = os.path.join(self.rootDir, filepath)
        logging.debug("Download Agent serving file: %s" % pathInCache)
        return serve_file(pathInCache, "application/x-download", "attachment")
    index.exposed = True


class ImageServer:

    def __init__(self, rootDir):
        self.rootDir = rootDir

    def index(self, filepath):
        pathInCache = os.path.join(self.rootDir, filepath)
        logging.debug("ImageServer serving file: %s" % pathInCache)
        return serve_file(pathInCache, content_type="image/png")
    index.exposed = True

class HTTPFrontendComponent:


    def __init__(self, **args):
        self.args = {}
        self.args['Logfile'] = None
        self.args['HTTPLogfile'] = None
        self.args['Host'] = socket.gethostname()
        self.args['Port'] = 8888
        self.args['ThreadPool'] = 10
        self.args['JobCreatorCache'] = None
        self.args.update(args)

        [ self.args.__setitem__(x, int(self.args[x])) for x in [
            'Port', 'ThreadPool'] ]

        self.staticDir = os.path.join(self.args['ComponentDir'], "static")
        self.imageDir = os.path.join(self.args['ComponentDir'],"image")
	if not os.path.exists(self.staticDir):
            os.makedirs(self.staticDir)
        
	if not os.path.exists(self.imageDir):
            os.makedirs(self.imageDir)
	
        if self.args['Logfile'] == None:
            self.args['Logfile'] = os.path.join(self.args['ComponentDir'],
                                                "ComponentLog")
        LoggingUtils.installLogHandler(self)
        logging.getLogger().setLevel(logging.DEBUG)

        if self.args['HTTPLogfile'] == None:
            self.args['HTTPLogfile'] = os.path.join(self.args['ComponentDir'],
                                                    "HTTPLog")

	


    def __call__(self, message, payload):
        """
        _operator(message, payload)_

        Respond to messages: No messages for this component

        """
        pass



    def startComponent(self):
        """
        _startComponent_

        Start up the cherrypy service for this component

        """
        cherrypy.config.update({'environment': 'production',
                                'log.error_file': self.args['HTTPLogfile'],
                                'log.screen': True})
        cherrypy.config.update({
        "global" : {
        "server.socket_host" :  self.args['Host'],
        "server.socket_port" :  self.args['Port'],
        "server.thread_pool" :  self.args['ThreadPool'],
        }})
        
        baseUrl = "http://%s:%s" % (
            self.args['Host'], self.args['Port'])

        T2 = {}
# pisa        
###        T2['T2_BE_IIHE'] = ('iihe',)
###        T2['T2_BE_UCL'] = ('ucl',)
###        T2['T2_DE_DESY'] = ('gridka',)
###        T2['T2_DE_RWTH'] = ('rwth',)
###        T2['T2_IT_Pisa'] = ('pi.infn.it',)
###        T2['T2_US_Nebraska'] = ('unl.edu',)
# crabas2
        T2['T2_US_Nebraska'] = ('unl.edu',)
        T2['T2_CH_CSCS'] = ('cscs.ch',)
        T2['T2_TW_Taiwan'] = ('sinica.edu.tw',)
        T2['T2_CN_Beijing'] = ('ac.cn',)
        T2['T2_ES_CIEMAT'] = ('ciemat',)
        T2['T2_ES_IFCA'] = ('ifca.es',)
        T2['T2_FR_CCIN2P3'] = ('in2p3',)
        T2['T2_HU_Budapest'] = ('.hu',)
        T2['T2_IT_Bari'] = ('ba.infn.it',)
        T2['T2_IT_Legnaro'] = ('lnl',)
        T2['T2_IT_Rome'] = ('roma',)
        T2['T2_US_Caltech'] = ('ultralight.org',)
        T2['T2_US_UCSD'] = ('ucsd.edu',)
        T2['T2_US_Purdue'] = ('purdue.edu',)
        T2['T2_US_Florida'] = ('ufl.edu',)
        T2['T2_IT_Pisa'] = ('pi.infn.it',)
        T2['T2_KR_KNU'] = ('knu.ac.kr',)
        T2['T2_UK_SGrid_Bristol'] = ('bris.ac.uk',)
        T2['T2_UK_SGrid_RALPP'] = ('rl.ac.uk',)
#        T2['T2_UK_SouthGrid'] = ('rl.ac.uk',)
        T2['T2_US_MIT'] = ('mit.edu',)
#AF new
        T2['T2_PL_Warsaw'] = ('polgrid.pl',)
# nothing
#        T2['T2_FR_GRIF'] = ()
#        T2['T2_UK_London'] = ('ic','.uk',)#
#        T2['T2_US_All'] = ('fnal.gov',)
#        T2['T2_US_MIT'] = ()
#        T2['T2_US_Wisconsin'] = ()
#        T2['T2_BR_UERJ'] = ('what?!?',)
#        T2['T2_BR_SPRACE'] = ('what?!?',)




# this part is executed only when (re)started the component, so is useless!
#         Sites, dummy = API.getSites(Sites=T2)
#         T2sk = {}
#         for site in Sites.keys():
#             if Sites[site] > 0:
#                 T2sk[site] = Sites[site]
#         logging.info("=============> %s"%str(T2sk))



        root = Root(baseUrl,T2)
        root.download = Downloader(self.args['JobCreatorCache'])
        root.images = ImageServer(self.staticDir)
#        root.workflowgraph = WorkflowGraph(
#            "%s/images" % baseUrl,
#            self.staticDir)
        
            
        root.task = TaskMonitor()
        root.retrieving = JobRetrievingMonitor()
	root.stat_US_24 = StatUserMonitor(96,900,1)
        root.stat_US_7 = StatUserMonitor(7*24,3600,7)
        root.stat_US_30 = StatUserMonitor(30*24,3600,30)
	root.status_components = ComponentMonitor()

#        root.datasets = DatasetMonitor(getLocalDBSURL())

#        root.resources = ResourceDetails()
#        root.resourcestate = ResourceStatus()
#        root.mergedataset = MergeMonitor()
#        root.mergedgraph = MergeGraph(
#            "%s/images" % baseUrl,
#            self.staticDir)
        root.resubmitting = JobResubmittingMonitor()
#        statuses = ['Retrieved', 'Done', 'Cleared', 'Aborted', 'Running', 'Created', 'NULL', 'Scheduled']

#        root.hist_schedstat = root.show_hist_schedstat()
#        root.hist_schedstat24 = HistStatusMonitor(24,3600,'status_scheduler')
#        root.hist_schedstat7 = HistStatusMonitor(7*4,6*3600,'status_scheduler')
#        root.hist_schedstat30 = HistStatusMonitor(30,24*3600,'status_scheduler')
#        statuses = ['not_handled','handled','failed','failure_handled','output_requested','in_progress','output_retrieved','processed', 'NULL']
# #         root.hist_procsstat24 = HistStatusMonitor(24,3600,'process_status')
# #         root.hist_procsstat7 = HistStatusMonitor(7*4,6*3600,'process_status')
# #         root.hist_procsstat30 = HistStatusMonitor(30,24*3600,'process_status')
# #         root.hist_HW_24 = HistHWMonitor(480)   # 480 times 3min is 24 hours
# #         root.hist_HW_7 = HistHWMonitor(3360)   # 3360 times 3min is 7 days
# #         root.hist_HW_30 = HistHWMonitor(14400)   # 14400 times 3min is 7 days
#         root.timespan_all  = TimeSpanMonitor('all');
#         root.errors_all  = ErrorsMonitor('all');
#         for site in T2.keys():
#             exec_str = 'root.timespan_'+site+"  = TimeSpanMonitor(T2[site])";
#             exec(exec_str);
#             exec_str = 'root.hist_procsstat24_'+site+"  = HistStatusMonitor(24,3600,'process_status',T2[site])";
#             exec(exec_str);
#             exec_str = 'root.hist_procsstat7_'+site+"  = HistStatusMonitor(7*4,6*3600,'process_status',T2[site])";
#             exec(exec_str);
#             exec_str = 'root.hist_procsstat30_'+site+"  = HistStatusMonitor(30,24*3600,'process_status',T2[site])";
#             exec(exec_str);
#             exec_str = 'root.hist_schedstat24_'+site+"  = HistStatusMonitor(24,3600,'status_scheduler',T2[site])";
#             exec(exec_str);
#             exec_str = 'root.hist_schedstat7_'+site+"  = HistStatusMonitor(7,24*3600,'status_scheduler',T2[site])";
#             exec(exec_str);
#             exec_str = 'root.hist_schedstat30_'+site+"  = HistStatusMonitor(30,24*3600,'status_scheduler',T2[site])";
#             exec(exec_str);
#             exec_str = 'root.lastErrors1_'+site+"  = ErrorsMonitor(T2[site],3600,site)";
#             exec(exec_str);
#             exec_str = 'root.lastErrors2_'+site+"  = ErrorsMonitor(T2[site],2*3600,site)";
#             exec(exec_str);
#             exec_str = 'root.lastErrors3_'+site+"  = ErrorsMonitor(T2[site],3*3600,site)";
#             exec(exec_str);
#             exec_str = 'root.lastErrors6_'+site+"  = ErrorsMonitor(T2[site],6*3600,site)";
#             exec(exec_str);
#             exec_str = 'root.lastErrors12_'+site+"  = ErrorsMonitor(T2[site],12*3600,site)";
#             exec(exec_str);
#             exec_str = 'root.lastErrors24_'+site+"  = ErrorsMonitor(T2[site],24*3600,site)";
#             exec(exec_str);
#             exec_str = 'root.lastErrorsAll_'+site+"  = ErrorsMonitor(T2[site],0,site)";
#             exec(exec_str);
#             exec_str = 'root.lastStatus1_'+site+"  = LastStatusMonitor(1*3600,T2[site],site)";
#             exec(exec_str);
#             exec_str = 'root.lastStatus2_'+site+"  = LastStatusMonitor(2*3600,T2[site],site)";
#             exec(exec_str);
#             exec_str = 'root.lastStatus3_'+site+"  = LastStatusMonitor(3*3600,T2[site],site)";
#             exec(exec_str);
#             exec_str = 'root.lastStatus6_'+site+"  = LastStatusMonitor(6*3600,T2[site],site)";
#             exec(exec_str);
#             exec_str = 'root.lastStatus12_'+site+"  = LastStatusMonitor(12*3600,T2[site],site)";
#             exec(exec_str);
#             exec_str = 'root.lastStatus24_'+site+"  = LastStatusMonitor(24*3600,T2[site],site)";
#             exec(exec_str);
#             exec_str = 'root.lastStatus48_'+site+"  = LastStatusMonitor(48*3600,T2[site],site)";
#             exec(exec_str);
#             exec_str = 'root.lastStatusAll_'+site+"  = LastStatusMonitor(0,T2[site],site)";
#             exec(exec_str);
#             exec_str = 'root.lastSites1_'+site+"  = LastSitesMonitor(1*3600,T2[site],site)";
#             exec(exec_str);
#             exec_str = 'root.lastSites2_'+site+"  = LastSitesMonitor(2*3600,T2[site],site)";
#             exec(exec_str);
#             exec_str = 'root.lastSites3_'+site+"  = LastSitesMonitor(3*3600,T2[site],site)";
#             exec(exec_str);
#             exec_str = 'root.lastSites6_'+site+"  = LastSitesMonitor(6*3600,T2[site],site)";
#             exec(exec_str);
#             exec_str = 'root.lastSites12_'+site+"  = LastSitesMonitor(12*3600,T2[site],site)";
#             exec(exec_str);
#             exec_str = 'root.lastSites24_'+site+"  = LastSitesMonitor(24*3600,T2[site],site)";
#             exec(exec_str);
#             exec_str = 'root.lastSitesAll_'+site+"  = LastSitesMonitor(0,T2[site],site)";
#             exec(exec_str);
#             exec_str = 'root.lastEfficiency1_'+site+"  = EfficiencyMonitor(T2[site],3600,site)";
#             exec(exec_str);
#             exec_str = 'root.lastEfficiency2_'+site+"  = EfficiencyMonitor(T2[site],2*3600,site)";
#             exec(exec_str);
#             exec_str = 'root.lastEfficiency3_'+site+"  = EfficiencyMonitor(T2[site],3*3600,site)";
#             exec(exec_str);
#             exec_str = 'root.lastEfficiency6_'+site+"  = EfficiencyMonitor(T2[site],6*3600,site)";
#             exec(exec_str);
#             exec_str = 'root.lastEfficiency12_'+site+"  = EfficiencyMonitor(T2[site],12*3600,site)";
#             exec(exec_str);
#             exec_str = 'root.lastEfficiency24_'+site+"  = EfficiencyMonitor(T2[site],24*3600,site)";
#             exec(exec_str);
#             exec_str = 'root.lastEfficiencyAll_'+site+"  = EfficiencyMonitor(T2[site],0,site)";
#             exec(exec_str);
            
        root.lastStatus1_all  = LastStatusMonitor(1*3600,'all');
        root.lastStatus2_all  = LastStatusMonitor(2*3600,'all');
        root.lastStatus3_all  = LastStatusMonitor(3*3600,'all');
        root.lastStatus6_all  = LastStatusMonitor(6*3600,'all');
        root.lastStatus12_all  = LastStatusMonitor(12*3600,'all');
        root.lastStatus24_all  = LastStatusMonitor(24*3600,'all');
        root.lastStatusAll_all  = LastStatusMonitor(0,'all');

        root.lastSites1_all  = LastSitesMonitor(1*3600,T2);
        root.lastSites2_all  = LastSitesMonitor(2*3600,T2);
        root.lastSites3_all  = LastSitesMonitor(3*3600,T2);
        root.lastSites6_all  = LastSitesMonitor(6*3600,T2);
        root.lastSites12_all  = LastSitesMonitor(12*3600,T2);
        root.lastSites24_all  = LastSitesMonitor(24*3600,T2);
        root.lastSitesAll_all  = LastSitesMonitor(0,T2);

        root.lastStatusSites1_all  = LastStatusDestinationMonitor(1*3600,T2);
        root.lastStatusSites2_all  = LastStatusDestinationMonitor(2*3600,T2);
        root.lastStatusSites3_all  = LastStatusDestinationMonitor(3*3600,T2);
        root.lastStatusSites6_all  = LastStatusDestinationMonitor(6*3600,T2);
        root.lastStatusSites12_all  = LastStatusDestinationMonitor(12*3600,T2);
        root.lastStatusSites24_all  = LastStatusDestinationMonitor(24*3600,T2);
        root.lastStatusSitesAll_all  = LastStatusDestinationMonitor(0,T2);
           

#            )

        
        cherrypy.quickstart(root)
        
