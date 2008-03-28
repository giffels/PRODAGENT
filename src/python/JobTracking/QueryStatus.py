#!/usr/bin/env python
"""
__QueryStatus__

"""

__version__ = "$Id"
__revision__ = "$Revision"
__author__ = "Giuseppe.Codispoti@bo.infn.it"

from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdCommon.BossLite.API.BossLiteAPI import  BossLiteAPI
from ProdCommon.BossLite.API.BossLiteAPI import  parseRange
from ProdCommon.BossLite.API.BossLiteAPISched import BossLiteAPISched
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
    proxy = sys.argv[4]
except :
    print "\nUsage:  QueryStatus <taskRange> <jobRange> <scheduler> <proxy>\n"
    sys.exit()

# BossLiteApi session
bossSession = BossLiteAPI( "MySQL", dbConfig)

# Scheduler session
schedulerConfig = { 'name' : scheduler, 'user_proxy' : proxy }
schedSession = BossLiteAPISched( bossSession, schedulerConfig)

for taskId in parseRange( taskRange ) :
    try:
        print 'checking status of task ' + str(taskId) + ' ....'
        task = schedSession.query( taskId, jobRange )
        for job in task.jobs :
            print job.runningJob['schedulerId'], job.runningJob['statusScheduler']
    except :
        print "Error in ", taskId, " status query"
        print traceback.format_exc()
        

