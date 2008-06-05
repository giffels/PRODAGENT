#!/usr/bin/env python

"""
_BOSSInfo_


Just a few function based on BOSS to determine the number of
running/waiting jobs at a site.

"""

#from ProdAgentBOSS import BOSSCommands
import re

from ProdCommon.BossLite.API.BossLiteDB import  BossLiteDB
from ProdAgentDB.Config import defaultConfig as dbConfig
#bossConfig = PAcfg.get("BOSS")
#bossCfgDir = bossConfig['configDir']




## a dictionary with all states that are considered waiting per scheduler
## normally these names are the same for SCHED_edg, SCHED_glite,
## SCHED_gliteCollection and SCHED_gliteParam
waiting_name = { 'default' : ['Created', 'Scheduled', 'Submitted']}

## a dictionary with all states that are considered running per scheduler
running_name = { 'default':['Running'] }

## all possible BOSS schedulers
## BOSS_all_scheds=['bbs','condor','condor_g',
##                  'edg','fork','glite',
##                  'gliteCollection','gliteParam',
##                  'lsf','pbs','sge']
BOSS_all_scheds = [
    'SchedulerLsf',
    'SchedulerGLiteAPI',
    'SchedulerGLite',
    'SchedulerCondor',
    'SchedulerCondorG',
    'SchedulerGlidein']



#def bossAdmin(sql):
#    timeout=100
#    outfile=BOSSCommands.executeCommand("bossAdmin SQL -query \""+sql+"\" -c " + bossCfgDir)
#    answ=[]
#    reg_white=re.compile(r"\s+$")#

#    for li in outfile.split("\n"):
#        ## remove whitespace at the end
#        answ.append(reg_white.sub('',li))

#    return(answ)

def bossAdmin(sql):

    bossLiteDB = BossLiteDB( 'MySQL', dbConfig )
    answ = bossLiteDB.select( sql )

    if answ is None :
        return []

    return [ line for line in answ ]

def all_sched():
    sql = 'select scheduler from bl_runningjob group by scheduler where scheduler is not NULL;'

    bossLiteDB = BossLiteDB( 'MySQL', dbConfig )
    answ = bossLiteDB.select( sql )

    if answ is None :
        return []

    return [ line[0] for line in answ ]

def activeJobs():
    ## returns a simple dictionary with total number of jobs, running and waiting
    bossLiteDB = BossLiteDB( 'MySQL', dbConfig )

    act_re = re.compile(r"\s*(?P<count>\d+)")

    ce_act = {'Idle':0, 'Running':0}

    found_scheds = all_sched()
    for sched in found_scheds:
        ## select the names for states that are considered waiting for this scheduler
        if waiting_name.has_key(sched):
            wnames = waiting_name[sched]
        else:
            wnames = waiting_name['default']
        ## select the names for states that are considered running for this scheduler
        if running_name.has_key(sched):
            rnames = running_name[sched]
        else:
            rnames = running_name['default']

        ## get the number of waiting jobs
        sql = "select count(*) from bl_runningjob where scheduler='" \
              + sched + "' and "
        for name in wnames:
            sql += 'status_scheduler = \'' + name + '\' or '
        sql = re.sub(' or $', '', sql)
        sql += 'group by destination;'

        bossLiteDB = BossLiteDB( 'MySQL', dbConfig )
        answ = bossLiteDB.select( sql )
        for count in answ :
            if act_re.search(count[0]):
                ce_act['Idle'] += int(act_re.search(count[0]).group('count'))

        ## get the number of running jobs
        sql = "select count(*) from bl_runningjob where scheduler='" \
              + sched + "' and "
        for name in rnames:
            sql += 'status_scheduler = \'' + name + '\' or '
        sql = re.sub(' or $', '', sql)
        sql += 'group by destination;'

        answ = bossLiteDB.select( sql )
        for count in answ :
            if act_re.search(count[0]):
                ce_act['Running'] += int(act_re.search(count[0]).group('count'))

    return ce_act	

def activeSiteJobs():
    ## returns a dictionary with ce_queue regexp and number of active jobs.
    ## portnumbers are not available, hope they are unique per ce
    bossLiteDB = BossLiteDB( 'MySQL', dbConfig )
    ce_act = {}
    act_re = re.compile(r"\s*(?P<dest_ce>\S+)\s+(?P<dest_queue>\S+)\s+(?P<count>\d+)")

    found_scheds = all_sched()
    for sched in found_scheds:
        ## select the names for states that are considered waiting for this scheduler
        if waiting_name.has_key(sched):
            wnames = waiting_name[sched]
        else:
            wnames = waiting_name['default']
        ## select the names for states that are considered running for this scheduler
        if running_name.has_key(sched):
            rnames = running_name[sched]
        else:
            rnames = running_name['default']

        ## get the number of waiting jobs
        sql = \
            "select destination,count(*) from bl_runningjob where scheduler='"\
            + sched + "' and "
        for name in wnames:
            sql += 'status_scheduler = \''+name+'\' or '
        sql = re.sub(' or $', '', sql)
        sql += 'group by destination;'

        answ = bossLiteDB.select( sql )
        for ce in answ:
            if act_re.search(ce[0]):
                reggy = ce[0]
                if not ce_act.has_key(reggy):
                    ce_act[reggy] = {'Idle':0, 'Running':0}
                ## prepare regexp for ce matching
                ce_act[reggy]['Idle'] += int(ce[1])

        ## get the number of running jobs
        sql = \
            "select destination,count(*) from bl_runningjob where scheduler='"\
            + sched + "' and "
        for name in rnames:
            sql += 'status_scheduler = \'' + name + '\' or '
        sql = re.sub(' or $', '', sql)
        sql += 'group by destination;'

        answ = bossLiteDB.select( sql )
        for ce in answ:
            if act_re.search(ce[0]):
                reggy = ce[0]
                if not ce_act.has_key(reggy):
                    ce_act[reggy] = {'Idle':0, 'Running':0}
                ## prepare regexp for ce matching
                ce_act[reggy]['Running'] += int(ce[1])

    return ce_act	

def anySiteJobs(defaultGatekeepers):
    """
    _anySiteJobs_

    Return a dictionary of gatekeeper: number of processing jobs for
    each gatekeeper found in the boss activeSiteJobs output

    Default gatekeepers is a list of gatekeepers that you want to
    make sure appear in the output, since if there are no jobs, it
    wont be included. If there are no jobs at a default gatekeeper,
    then it will be returned as having 0 jobs
    """
    ces_regexp = activeSiteJobs()
    attributes = {}
    for default in defaultGatekeepers:
        attributes[default] = {'Idle':0, 'Running':0}

    for regexp in ces_regexp.keys():
        reg = re.compile(regexp)
        for ce in defaultGatekeepers:
            if reg.search(r""+ce): 
                attributes[ce] = ces_regexp[regexp]

    return attributes

