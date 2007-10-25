#!/usr/bin/env python

"""
_WorkflowConstraints_

"""

from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec
from ProdCommon.Database import Session
import os,re
import logging

from ProdAgentCore.Configuration import loadProdAgentConfiguration
PAcfg = loadProdAgentConfiguration()


def getCMSSoft(work,reverse=False):
    """
    opens the workflowfile and gets the CMSSoft version
    if reverse, returns a map between CMSSoft version and real workflowname
    """

    new_work={}
    workflowSpec = WorkflowSpec()
    for fil in work:
        try:
            workflowSpec.load(fil)
            cmssw=workflowSpec.payload.application['Version']
            name=workflowSpec.parameters['WorkflowName']
            if reverse:
                if not new_work.has_key(cmssw):
                    new_work[cmssw]=[]
                    new_work[cmssw].append(name)
            else:
                new_work[name]=cmssw
        except:
            """
            something went wrong

            """
            msg="WorkflowConstraints getCMSSoft: something went wrong while handling file "+fil
            print(msg)

    return new_work

def getCMSSoftFromWE(workmap,reverse=False):
    """ 
    returns a map between Worklow and CMSSoftware release
    for all WFs not allready in workmap and found in we_Workflow 
    """

    sqlStr='select workflow_spec_file from we_Workflow;'
    Session.execute(sqlStr)
    rows=Session.fetchall()
    all_files=[]
    for i in rows:
        if os.path.isfile(i[0]):
            all_files.append(i[0])


    newwf=getCMSSoft(all_files,reverse)
    #print "getCMSSoftFromRequestInjector "+str(newwf)

    for wf,cmssw in newwf.items():
        if not workmap.has_key(wf):
            workmap[wf]=cmssw

    return workmap


def getCMSSoftFromRequestInjector(workmap,reverse=False):
    """
    returns a map between Worklow and CMSSoftware release
    for all WFs not allready in workmap and found in RequestInjector/WorkflowCache directory 
    """
    RIcfg = PAcfg.get("RequestInjector")
    RIDir = RIcfg['ComponentDir']
    WFCache = os.path.join(RIDir,'WorkflowCache')
    all_files=[]
    reg_wf=re.compile(r"-workflow.xml$")
    for fi in os.listdir(WFCache):
        if reg_wf.search(fi):
            all_files.append(os.path.join(WFCache,fi))

    newwf=getCMSSoft(all_files,reverse)
    #print "getCMSSoftFromRequestInjector "+str(newwf)

    for wf,cmssw in newwf.items():
        if not workmap.has_key(wf):
            workmap[wf]=cmssw

    return workmap


def getCMSSoftFromProdMon(reverse=False):
    """
    Returns a map between Worklow and CMSSoftware release
    if reverse, return the reverse
    """
    sqlStr='select workflow_name,app_version from prodmon_Workflow;'
    Session.execute(sqlStr)
    rows=Session.fetchall()


    work={}
    for i in rows:
        if reverse:
            if not work.has_key(i[1]):
                work[i[1]]=[]
            work[i[1]].append(i[0])
        else:
            work[i[0]]=i[1]

    return work

def getWorkflow2CMSSW():
    """
    return a dictionary with workflows and corresponding cmssoft release
    """

    """
    make an initial map based on prodmon

    will cover only WFs that are already known to ProdMon (so not the freshly injected ones)

    """
    workmap=getCMSSoftFromProdMon()

    """
    collect more possible workflows
    """
    workmap=getCMSSoftFromWE(workmap)

    workmap=getCMSSoftFromRequestInjector(workmap)



    """
    don't process worklfows that have no entry in merge_dataset

    """
    sqlStr='select distinct workflow from merge_dataset;'
    Session.execute(sqlStr)
    rows=Session.fetchall()
    wfs=[]
    for x in rows:
        wfs.append(x[0])
    for wf in workmap.keys():
        if not wf in wfs:
            logging.info("Removing workflow %s from list: not found in merge_dataset" % wf)
            del workmap[wf]

    return workmap

def workmapWFName2ID(workmap):
    """
    do the name possible ID translation for a dictionary {WFname:cmssw_version}

    use merge_dataset
    WFs that have no merge_dataset entry, use full name
    --> should not be processing

    """

    sqlStr='select workflow,id from merge_dataset;'
    Session.execute(sqlStr)
    rows=Session.fetchall()

    w2i={}
    for i in rows:
        w2i[str(i[0])]=str(i[1])


    logging.info("workmapWFName2ID "+str(w2i))

    workmap2={}
    for wf,cmssw in workmap.items():
        if w2i.has_key(wf):
            workmap2[w2i[wf]]=cmssw
        else:
            workmap2[wf]=cmssw

    return workmap2


def constraintID2WFname(constr):
    """
    do the (possible) ID 2 WFname translation for a workflowconstraint (ie a commaseparated list of WFs)

    use merge_dataset
    WFs that have no merge_dataset entry, use full name
    --> but these should not be processing !!!

    """

    ## empty constr = None
    if not constr:
        return None

    sqlStr='select id,workflow from merge_dataset;'
    Session.execute(sqlStr)
    rows=Session.fetchall()

    i2w={}
    for i in rows:
        i2w[str(i[0])]=str(i[1])


    listt=[]
    for wf in constr.split(','):
        if i2w.has_key(wf):
            listt.append(i2w[wf])
        else:
            listt.append(wf)

    listt.sort()
    return ','.join(listt)



def gtkToNotWorkflows(infosite,short=False):
    """
    take a dictionary with {gtk:[cmssoft]} and return a dictionary {gtk:[not_workflow]}
    only will work if new workflows ALWAYS trigger RM

    if short, attempt a translationof WF names to ids in various tables
    """


    #workmap=getWorkflow2CMSSW()
    workmap=workmapWFName2ID(getWorkflow2CMSSW())
    logging.info("gtkToNotWorkflows: workflow/software "+str(workmap))

    all_gtk={}
    for gtk in infosite.keys():
        all_gtk[gtk]=[]

    for workflow,version in workmap.items():
        for gtk,val in infosite.items():
            if not version in val['software']: 
                all_gtk[gtk].append(workflow)

    return all_gtk	

