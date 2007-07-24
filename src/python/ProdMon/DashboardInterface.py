# !/usr/bin/env python
"""
_DashboardInterface_

Interface to Dashboard (external information repository)
"""

from xml.dom.minidom import Document
import logging
from ProdMon.ProdMonDB import getJobInstancesToExport, markInstancesExported, \
                                                        getJobStatistics
import urllib, urllib2

USER_AGENT = \
"ProdMon/1.0 https://twiki.cern.ch/twiki/bin/view/CMS/ProdAgentProdMon"


def exportToDashboard(maxRecords, url, team, agent):
    """
    _exportToDashboard_
    
    Send un-exported job instances to Dashboard
    
    """
    
    try:
        instances = getJobInstancesToExport(maxRecords)
        
        if not instances:
            return
        
        logging.debug("%s job instances to export to external monitoring" % \
                                                                len(instances))
        
        # format and export
        prodReport = createProdReport(instances, team, agent)

        # send to dashboard
        # takes list of (key, value) pairs
        HTTPpost([("report", prodReport.toxml())], url)

        # update instance's status
        markInstancesExported(instances)
    
    except Exception, ex:
        raise RuntimeError, "Error exporting data to dashboard: %s" % str(ex)
    
    logging.debug("export complete")
    return


def createProdReport(instances, team_name, agent_name):
    """
    _jobInstanceToXML_
    
    Format job instances into xml
    
    """

    # do beginning xml suff
    doc = Document()
    report = doc.createElement("production_report")
    doc.appendChild(report)
    
    # add team name
    team = doc.createElement("team")
    team.setAttribute("name", team_name)
    report.appendChild(team)
    
    # add agent name
    agent = doc.createElement("agent")
    agent.setAttribute("name", agent_name)
    team.appendChild(agent)
    
    # get info for each instance
    jobStatistics = getJobStatistics(instances)

    # call jobToXML() for each job and its instances
    # iterate through instances pre-sorted by job - form list of instances
    # for each job and pass both to jobToXML()
    # TODO: Find a better algorithm for this
    previous_job = jobStatistics[0]["job_id"]
    temp = []
    for instance in jobStatistics:
        if instance["job_id"] == previous_job:
            temp.append(instance)
        else:
            jobToXML(doc, agent, temp)
            temp = []
            temp.append(instance)
        previous_job = instance["job_id"]
    # dont forget last job
    jobToXML(doc, agent, temp)

    return doc


def jobToXML(document, parent, instances):
    """
    _jobToXML_
    
    Format a job and instances as XML
    """
  
    # can pull job info from any instance (all same job)
    jobInfo = instances[0]
    
    # get standard job info
    job = document.createElement("job")
    job.setAttribute("type", jobInfo["job_type"])
    job.setAttribute("job_spec_id", str(jobInfo["job_spec_id"]))
    job.setAttribute("request_id", str(jobInfo["request_id"]))
    job.setAttribute("workflow_name", str(jobInfo["workflow_name"]))
    job.setAttribute("app_version", str(jobInfo["app_version"]))
    parent.appendChild(job)
    
    # find input/output datasets
    input_datasets_node = document.createElement("input_datasets")
    job.appendChild(input_datasets_node)
    for dataset in jobInfo["input_datasets"]:
        addTextNode(document, input_datasets_node, "dataset", str(dataset))
    output_datasets_node = document.createElement("output_datasets")
    job.appendChild(output_datasets_node)
    for dataset in jobInfo["output_datasets"]:
        addTextNode(document, output_datasets_node, "dataset", str(dataset))
    
    # now add instances
    instancesToXML(document, job, instances)
    
    return


def instancesToXML(document, parent, instances):
    """
    _instanceToXML_
    
    Format a tuple of job instances as XML - appended to the given
    document and node
    """
    
    for instanceInfo in instances:
        instance_node = document.createElement("instance")
        
        # add dashboard id
        # used by dashboard as a unique key so should be present
        # if missing fake it to ensure uniqueness of instances
        # TODO: When dashboard_id guarenteed remove this extra code
        if instanceInfo["dashboard_id"] == None:
            instanceInfo["dashboard_id"] = "_".join((instanceInfo["job_spec_id"], \
                            str(instanceInfo["timing"]["AppStartTime"]), \
                            str(instanceInfo["instance_id"])))
        
        instance_node.setAttribute("dashboard_id", str(instanceInfo["dashboard_id"]))
        parent.appendChild(instance_node)

        # add resource node
        resource_node = document.createElement("resource")
        resource_node.setAttribute("site_name", str(instanceInfo["site_name"]))
        resource_node.setAttribute("ce_name", str(instanceInfo["ce_hostname"]))
        resource_node.setAttribute("se_name", str(instanceInfo["se_hostname"]))
        instance_node.appendChild(resource_node)
        
        addTextNode(document, instance_node, "events_written", instanceInfo["evts_written"])
        addTextNode(document, instance_node, "events_read", instanceInfo["evts_read"])
        addTextNode(document, instance_node, "exit_code", instanceInfo["exit_code"])
        addTextNode(document, instance_node, "start_time", instanceInfo["timing"]["AppStartTime"])
        addTextNode(document, instance_node, "end_time", instanceInfo["timing"]["AppEndTime"])
        
        # add error info
        # if instanceInfo["error_type"]:
        addTextNode(document, instance_node, "error_type", instanceInfo["error_type"])
        # if instanceInfo["error_desc"]:
        addTextNode(document, instance_node, "error_message", instanceInfo["error_message"])
        
        # LFN's
        input_node = document.createElement("input_files")
        instance_node.appendChild(input_node)
        for infile in instanceInfo["input_files"]:
            addTextNode(document, input_node, "LFN", infile)
                
        output_node = document.createElement("output_files")
        instance_node.appendChild(output_node)
        for outfile in instanceInfo["output_files"]:
            addTextNode(document, output_node, "LFN", outfile)
        
        # timing
        timing_node = document.createElement("timings")
        instance_node.appendChild(timing_node)
        for timing_type, value in instanceInfo["timing"].items():
            if timing_type not in ("AppStartTime", "AppEndTime"):
                addTextNode(document, timing_node, timing_type, value)

        # runs
        run_node = document.createElement("output_runs")
        instance_node.appendChild(run_node)
        for run in instanceInfo["output_runs"]:
            addTextNode(document, run_node, "value", run)
        
        # skipped events
        skipped_node = document.createElement("skipped_events")
        instance_node.appendChild(skipped_node)
        for run, event in instanceInfo["skipped_events"]:
            skipped_event_node = document.createElement("skipped")
            skipped_node.appendChild(skipped_event_node)
            skipped_event_node.setAttribute("run", str(run))
            skipped_event_node.setAttribute("event", str(event))

    return


def addTextNode(document, parent, name, value):
    """
    _addTextNode_
    
    Add a text node with name and value to the parent node within the document
    
    """
    node = document.createElement(name)
    parent.appendChild(node)
    if value != None:
        text = document.createTextNode(str(value))
        node.appendChild(text)
    return


def HTTPpost(params, url):
    """
    Do a http post with params to url
    
    params is a list of tuples of key,value pairs
    """
    
    logging.debug("contacting %s" % url)    
    
    data = urllib.urlencode(params)
    #put who we are in headers
    headers = { 'User-Agent' : USER_AGENT }
    req = urllib2.Request(url, data, headers)
    
    #logging.debug("with request:\n%s" % str(req))
    
    response = urllib2.urlopen(req, data)
        
    logging.debug("received http code: %s, message: %s, response: %s" \
         % (response.code, response.msg, str(response.read())))
            
    return