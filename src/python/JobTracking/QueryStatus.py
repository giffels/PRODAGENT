#!/usr/bin/env python
"""
__QueryStatus__

"""

__version__ = "$Id"
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
schedulerConfig = { 'name' : scheduler, 'user_proxy' : proxy }

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
                                                 'closed' : 'N',
                                                 'submissionTime' : '20%'}, \
                                   strict=False )
        for job in task.jobs :
            print job.runningJob['jobId'], \
                  job.runningJob['schedulerId'], \
                  job.runningJob['statusScheduler'], \
                  job.runningJob['statusReason']
    except SchedulerError, err:
        print "Error in ", taskId, " status query"
        print str(err)
    except :
        print "Error in ", taskId, " status query"
        print traceback.format_exc()
        
