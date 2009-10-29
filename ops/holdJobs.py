#!/usr/bin/env python
from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdCommon.Database import Session
from JobQueue.JobQueueDB import JobQueueDB
import sys, getopt

__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: holdJobs.py,v 1.1 2009/10/29 17:59:58 direyes Exp $"


valid = [
    'workflow=',        # filter by workflow
    'site=',            # filter by site index
    'list',            # show jobs
    'hold',
    'unhold',
    'type=',
    'njobs=',
    'help'
    ]

valid_type = ['Merge', 'Processing', 'Repack', 'Express', 'Skim', 'Harvesting']

workflow = None
site = None
list_jobs = False
job_type = None
action = None
n_jobs = None

usage = """
holdJobs.py [--hold|--unhold] --list --njobs=<#> --workflow=<WorflowID> [--site=<Site> --type=<type> --released]

Options description:
    hold                    : Take jobs out of the queue. Mark them as 'held'.
    unhold                  : Remove 'held' flag.
    njobs                   : Number of jobs.
    list                    : Show held jobs.
    workflow=<WorkflowID>   : WorkflowSpecID.  Wild card '%' is accepted.
    site=<SiteIndex>        : Site. Could be (site name, not supported yet) or site index.
    type=<type>             : Filter by job type. Types are:
                              - Merge, Processing, Repack, Express, Skim, Harvesting
    help                    : Print this.

Examples:

- List held jobs:

python holdJobs.py --list --workflow CMSSW_3_4_0_pre3-RelValJpsiMM-STARTUP3X_V11_v1%

- Hold jobs:

python holdJobs.py --hold --workflow CMSSW_3_4_0_pre3-RelValJpsiMM-STARTUP3X_V11_v1 --njobs 10

- Unhold jobs:

python holdJobs.py --unhold --workflow CMSSW_3_4_0_pre3-RelValJpsiMM-STARTUP3X_V11_v1 --njobs 10

- Hold jobs by site:

python holdJobs.py --hold --site 3 --njobs 10

- Hold and print held jobs:

python holdJobs.py --hold --workflow CMSSW_3_4_%-RelVal%-STARTUP3X_V11_v% --njobs 10 --list
"""

try:
    opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
    print usage
    print str(ex)
    sys.exit(1)


for opt, arg in opts:
    if opt == "--help":
        print usage
        sys.exit(0)
    if opt == "--njobs":
        n_jobs = int(arg)
    if opt == "--workflow":
        workflow = arg
    if opt == "--site":
        site  = arg
    if opt == "--list":
        list_jobs = True
    if opt == '--type':
        if arg in tuple(valid_type):
            job_type = arg
        else:
            msg = "Invalid type argument. Valid options are %." % (
                ", ".join(valid_type))
            print usage
            print msg
            sys.exit(1)
    if opt == "--hold":
        if action not in ('hold', None):
            msg = "Can't hold and unhold at the same time."
            print usage
            print msg
            sys.exit(1)
        else:
            action = 'hold'
    if opt == "--unhold":
        if action not in ('unhold', None):
            msg = "Can't hold and unhold at the same time."
            print usage
            print msg
            sys.exit(1)
        else:
            action = 'unhold'

if n_jobs is None and action is not None:
    msg = 'You must provide --njobs option.'
    print usage
    print msg
    sys.exit(1)

if action is None and not list_jobs:
    msg = 'You must provide any of --hold, --unhold or --list options.'
    print usage
    print msg
    sys.exit(1)


#  //
# // starting DB session
#//
Session.set_database(dbConfig)
Session.connect()
Session.start_transaction()
jobQueue = JobQueueDB()


#  //
# // Holding jobs?
#//
if action == 'hold':
    sql_str = \
        """SELECT DISTINCT jobQ.job_index, jobQ.job_spec_id
        FROM jq_queue jobQ LEFT OUTER JOIN jq_site siteQ
        ON jobQ.job_index = siteQ.job_index
        WHERE status = 'new'
        """
    if site is not None:
        sql_str += "AND siteQ.site_index=%s " % site
    if workflow is not None:
        sql_str += "AND jobQ.workflow_id like \"%s%s%s\" " % (
            '%', workflow.strip(), '%')
    if job_type is not None:
        sql_str += "AND jobQ.job_type=\"%s\" " % job_type
    sql_str += " ORDER BY priority ASC, time LIMIT %s;" % (
        n_jobs)

    Session.execute(sql_str)
    result = Session.fetchall()
    job_ids = [int(x[0]) for x in result]

    output = "%s jobs will be held." % len(job_ids)
    print output

    sql_str = \
        """UPDATE jq_queue SET status='held'
        WHERE status = 'new' AND job_index IN %s;
        """ % str(tuple(job_ids))

    try:
        Session.execute(sql_str)
    except ex:
        print "Couldn't update DB."
        print ex

    output = "%s more jobs are now held." % len(job_ids)
    print output


#  //
# // Unholding jobs?
#//
elif action == 'unhold':
    sql_str = \
        """SELECT DISTINCT jobQ.job_index, jobQ.job_spec_id
        FROM jq_queue jobQ LEFT OUTER JOIN jq_site siteQ
        ON jobQ.job_index = siteQ.job_index
        WHERE status = 'held'
        """
    if site is not None:
        sql_str += "AND siteQ.site_index=%s " % site
    if workflow is not None:
        sql_str += "AND jobQ.workflow_id like \"%s%s%s\" " % (
            '%', workflow.strip(), '%')
    if job_type is not None:
        sql_str += "AND jobQ.job_type=\"%s\" " % job_type
    sql_str += " ORDER BY priority ASC, time LIMIT %s;" % (
        n_jobs)

    Session.execute(sql_str)
    result = Session.fetchall()
    job_ids = [int(x[0]) for x in result]

    output = "%s jobs will be unheld." % len(job_ids)
    print output

    sql_str = \
        """UPDATE jq_queue SET status='new'
        WHERE status = 'held' AND job_index IN %s;
        """ % str(tuple(job_ids))

    try:
        Session.execute(sql_str)
    except ex:
        print "Couldn't update DB."
        print ex

    output = "%s jobs were unheld." % len(job_ids)
    print output


#  //
# // Listing held jobs?
#//
if list_jobs:
    sql_str = \
        """SELECT DISTINCT jobQ.job_index, jobQ.job_spec_id 
        FROM jq_queue jobQ LEFT OUTER JOIN jq_site siteQ 
        ON jobQ.job_index = siteQ.job_index 
        WHERE status = 'held'
        """
    if site is not None:
        sql_str += "AND siteQ.site_index=%s " % site
    if workflow is not None:
        sql_str += "AND jobQ.workflow_id like \"%s%s%s\" " % (
            '%', workflow.strip(), '%')
    if job_type is not None:
        sql_str += "AND jobQ.job_type=\"%s\" " % job_type
    sql_str += " ORDER BY priority DESC, time;"

    Session.execute(sql_str)
    result = Session.fetchall()
    jobs = [x[1] for x in result]

    n_jobs_found = len(jobs)

    output = "Total of %s HELD Jobs matching constraints:\n" % len(jobs)
    output += " Workflow: %s\n" % workflow
    output += " Site:     %s\n" % site
    output += " Type:     %s\n" % job_type
    print output
    for job in jobs:
        print job


Session.commit_all()
Session.close_all()

