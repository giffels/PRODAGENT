#!/usr/bin/env python
"""
__QueryStatus__

"""

__version__ = "$Id: QueryStatus.py,v 1.6 2009/03/13 12:00:27 gcodispo Exp $"
__revision__ = "$Revision"
__author__ = "Giuseppe.Codispoti@bo.infn.it"

from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdCommon.BossLite.API.BossLiteAPI import  BossLiteAPI
from ProdCommon.BossLite.API.BossLiteAPISched import BossLiteAPISched
from ProdCommon.BossLite.Common.Exceptions import SchedulerError
from ProdCommon.BossLite.API.BossLiteAPI import parseRange
import sys
import traceback

taskRange = ""
jobRange  = ""
scheduler = ""
proxy = ""
args = sys.argv
try :
    taskRange = args[1]
    jobRange  = args[2]
    scheduler = sys.argv[3]
except :
    print "\nUsage:  QueryStatus <taskRange> <jobRange> <scheduler> <proxy>\n"
    sys.exit()
try :
    proxy = sys.argv[4]
except :
    proxy = ''

# BossLiteApi session
bossSession = BossLiteAPI( "MySQL", dbConfig)

# Scheduler session
schedulerConfig = { 'name' : scheduler,
                    'user_proxy' : proxy,
                    'skipProxyCheck' : True}

try:
    schedSession = BossLiteAPISched( bossSession, schedulerConfig)
except SchedulerError, err:
    print "Error in ", taskRange, " status query with proxy ", proxy
    print str(err)
    sys.exit( 1 )

for taskId in parseRange( taskRange ) :
    try:
        print 'checking status of task ' + str(taskId) + ' ....'
        task = schedSession.query( taskId, jobRange, \
                                   queryType='parent', \
                                   runningAttrs={'processStatus': '%handled',
                                                 'closed' : 'N'}, \
                                   strict=False )
        for job in task.jobs :
            print job.runningJob['jobId'], \
                  job.runningJob['schedulerId'], \
                  job.runningJob['statusScheduler'], \
                  job.runningJob['status'], \
                  job.runningJob['statusReason'], \
                  job.runningJob['lbTimestamp']
    except SchedulerError, err:
        print "Error in ", taskId, " status query"
        print schedSession.getLogger()
        print str(err)
        print traceback.format_exc()
    except Exception, err :
        print "Error in ", taskId, " status query"
        print str(err)
        print traceback.format_exc()


