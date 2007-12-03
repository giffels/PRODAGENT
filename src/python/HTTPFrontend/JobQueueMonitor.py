#!/usr/bin/env python
"""
_JobQueueMonitor_

CherryPy handler for displaying the list of jobs in the JobQueue

"""

from ProdCommon.Database import Session
from ProdAgentDB.Config import defaultConfig as dbConfig
from JobQueue.JobQueueDB import JobQueueDB




class JobQueueMonitor:

    def index(self):
        Session.set_database(dbConfig)
        Session.connect()
        Session.start_transaction()

        html = """<html><body><h2>JobQueue State </h2>\n """

        jobQueue = JobQueueDB()
        jobQueue.loadSiteMatchData()
            
        releasedProcJobs = jobQueue.retrieveReleasedJobs(1000000, "Processing")
        queuedProcJobs = jobQueue.retrieveJobs(1000000, "Processing")

        releasedMrgJobs = jobQueue.retrieveReleasedJobs(1000000, "Merge")
        queuedMrgJobs = jobQueue.retrieveJobs(1000000, "Merge")

        html += "<table>\n"
        html += " <tr><th>Job Type</th><th>Status</th><th>Total</th></tr>\n"

        html += " <tr><td>Processing</td><td>Queued</td>"
        html += "<td>%s</td></tr>\n" % len(queuedProcJobs)

        html += " <tr><td>Processing</td><td>Released</td>"
        html += "<td>%s</td></tr>\n" % len(releasedProcJobs)

        html += " <tr><td>Merge</td><td>Queued</td>"
        html += "<td>%s</td></tr>\n" % len(queuedMrgJobs)

        html += " <tr><td>Merge</td><td>Released</td>"
        html += "<td>%s</td></tr>\n" % len(releasedMrgJobs)
        
        
        html += "</table>\n"
        html += """</body></html>"""
        Session.commit_all()
        Session.close_all()


        return html
    index.exposed = True


