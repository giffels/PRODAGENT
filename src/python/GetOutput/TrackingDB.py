#!/usr/bin/env python
"""
_TrackingDB_

"""

__version__ = "$Id: TrackingDB.py,v 1.1.2.1 2007/12/10 18:24:51 ckavka Exp $"
__revision__ = "$Revision: 1.1.2.1 $"

class TrackingDB:
    """
    _TrackingDB_
    """

    def __init__(self, session):
        """
        __init__
        """

        self.session = session

    def addJobs(self, jobList):
        """
        _addJobs_
        """

        # check
        if len(jobList) == 0:
            return 0

        # prepare query
        jobs = ["('" + x + "')" for x in jobList]
        query = """insert ignore into jt_activejobs(job_id)
                        values """ + (",".join(jobs))

        # execute query
        rows = self.session.execute(query)

        return rows

    def getJobs(self, status):
        """
        __getJobs__

        get a list of jobs in a specific status
        """

        # execute query
        query = """select job_id
                     from jt_activejobs
                    where status='""" + str(status) + "'"
        rows = self.session.execute(query)

        # verify number of jobs
        if rows == 0:
            return []

        # get them
        results = self.session.fetchall()

        # return them as a list
        return [x[0] for x in results]           

    def setJobs(self, jobList, status):
        """
        __setJobs__

        set job status for a set of jobs
        """
 
        # build query
        query = """update jt_activejobs
                      set status='""" + str(status) + """'
                    where job_id in (""" + str(jobList)[1:-1] + ")"
        rows = self.session.execute(query)

        return rows

    def getJobInfo(self, jobId):
        """
        __getJobInfo__

        get job information
        """

        # execute query
        query = """select job_id, status, directory, output, boss_status,
                          job_spec_id
                     from jt_activejobs
                    where job_id='""" + str(jobId) + "'"
        rows = self.session.execute(query)

        if rows == 0:
            return {}

        results = self.session.fetchone()

        # build result
        return {'jobId' : results[0],
                'status' : results[1],
                'directory' : results[2],
                'output' : results[3],
                'bossStatus' : results[4],
                'jobSpecId' : results[5]}

    def getJobsInfo(self, jobList):
        """
        __getJobList__

        get jobs information
        """

        # execute query
        query = """select job_id, status, directory, output, boss_status,
                          job_spec_id
                     from jt_activejobs
                    where job_id in (""" + str(jobList)[1:-1] + ")"
        rows = self.session.execute(query)

        if rows == 0:
            return {}

        results = self.session.fetchall()

        # build result
        result = []
        for job in results:
            job = {'jobId' : result[0],
                   'status' : result[1],
                   'directory' : result[2],
                   'output' : result[3],
                   'bossStatus' : result[4],
                   'jobSpecId' : result[5]}
            result.append(job)

        # return results
        return result

    def setJobInfo(self, jobId, status = '', output = '', jobSpecId = '', \
                   directory = '', bossStatus = ''):
        """
        __setJob__

        updates job information
        """

        # build stucture
        fields = {'status' : status,
                  'directory' : directory,
                  'output' : output,
                  'boss_status' : bossStatus,
                  'job_spec_id' : jobSpecId}

        update = ",".join(["%s='%s'" % (k, v) for k, v in fields.items() \
                                              if v != ''])

        # at least one field must be provided
        if update == '':
            return 0

        # execute query
        query = """update jt_activejobs
                      set """ + str(update) +  """
                    where job_id='""" + str(jobId) + "'"

        # execute query #Fabio
        self.session.startTransaction()
        rows = self.session.execute(query)
        self.session.commit()
        return rows

    def removeJobs(self, status):
        """
        __removeJobs__

        remove jobs with a specific status
        """

        # execute query
        query = """delete
                     from jt_activejobs
                    where status='""" + str(status) + "'"

        # execute query #Fabio
        self.session.startTransaction()
        rows = self.session.execute(query)
        self.session.commit()

        return rows

