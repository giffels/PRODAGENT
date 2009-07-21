#!/usr/bin/env python

from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdCommon.Database import Session
from ProdAgent.ResourceControl.ResourceControlDB import ResourceControlDB


import sys, getopt


valid = [
    'new' , 'edit', 'drop', 'list',  # modes
    'site=',                         # one site or all sites
    
    'ce-name=', 'se-name=',  # new site args

    'set-threshold=', 'value=',  # edit threshold
    'set-attribute=',            # edit attribute
    'activate', 'deactivate',
    
    'processing-threshold=',                     # standard thresholds
    'merge-threshold=',                          # standard thresholds
    'cleanup-threshold=',                        # standard thresholds
    'logcollect-threshold=',                        # standard thresholds
    'skim-threshold=',                        # standard thresholds
    'express-threshold=',                        # standard thresholds
    'repack-threshold=',                        # standard thresholds
    'harvesting-threshold=',                        # standard thresholds
    'processing-throttle=',                      # standard throttles
    'merge-throttle=',                           # standard throttles
    'min-submit=', 'max-submit=',                # for new site mode
    'processing-throttle=', 'merge-throttle='   # new standard throttles 
    
    ]

mode = None

site = None
ceName = None
seName = None
activate = None
deactivate = None

setThreshold = None
setAttribute = None
setThisValue = None


procThreshold = 100
mergeThreshold = 10
cleanThreshold = 10
logcollectThreshold = 10
expressThreshold = 10
harvestingThreshold = 10
skimThreshold = 10
repThreshold = 100
minSubmit = 1
maxSubmit = 50

procThrottle = 10000000
skimThrottle = 10000000
mergeThrottle = 10000000

usage = """
resourceControl.py --<MODE>     # Mode is one of: new, edit, drop, list

            --site=<SITENAME>   # Name of the site (new, edit, drop)
            --ce-name=<CE NAME> # Compute Element name for site (new)
            --se-name=<SE NAME> # Storage Element name for site (new)

            --activate          # set the site as active (new, edit)
            --deactivate        # set the site as inactive (new, edit)
                                # default is that new sites are active
                                
            --processing-threshold=<INT> (new, edit)
            --merge-threshold=<INT>      (new, edit)
            --skim-threshold=<INT>      (new, edit)
            --express-threshold=<INT>      (new, edit)
            --logcollect-threshold=<INT>      (new, edit)
            --cleanup-threshold=<INT>    (new, edit)
            --repack-threshold=<INT>
                                # Thresholds for triggering new submission
                                # for merge or processing jobs respectively
            --processing-throttle=<INT>  (new, edit)
            --merge-throttle=<INT> (new, edit)
                                # Throttles:  when the number of running jobs
                                # exceed these, more jobs are not released
                                # from the jobQueue (this does not prevent
                                # released idle jobs from running however)
                                # set these with --set-threshold
            --min-submit=<INT>  # Minimum number of processing jobs to submit
                                # in a single attempt for bulk ops. (new, edit)
            --max-submit=<INT>  # Maximum number of processing jobs to submit
                                # in a single attempt for bulk ops. (new, edit)

            --set-threshold=<thresholdName> # set/add threshold name with value
            --set-attribute=<attributeName> # set/add attribute name with value
            --value=<Value of threshold/attribute>  # (edit)


"""

try:
    opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
    print usage
    print str(ex)
    sys.exit(1)


for opt, arg in opts:
    if opt == "--new":
        mode = "new"
    if opt == "--edit":
        mode = "edit"
    if opt == "--drop":
        mode = "drop"
    if opt == "--list":
        mode = "list"


    if opt == "--site":
        site = arg
    if opt == "--ce-name":
        ceName = arg
        
    if opt == "--se-name":
        seName = arg
    if opt == "--activate":
        activate = True

    if opt == "--deactivate":
        deactivate = True
        

    if opt == "--processing-threshold":
        procThreshold = int(arg)
        
    if opt == "--merge-threshold":
        mergeThreshold = int(arg)

    if opt == "--cleanup-threshold":
        cleanThreshold = int(arg)
    
    if opt == "--logcollect-threshold":
        logcollectThreshold = int(arg)
    
    if opt == "--express-threshold":
        expressThreshold = int(arg)
    
    if opt == "--skim-threshold":
        skimThreshold = int(arg)
    
    if opt == "--harvesting-threshold":
        expressThreshold = int(arg)
    
    if opt == "--logcollect-threshold":
        logcollectThreshold = int(arg)
    
    if opt == "--repack-threshold":
        repThreshold = int(arg)

    if opt == "--processing-throttle":
        procThrottle = int(arg)

    if opt == "--merge-throttle":
        mergeThrottle = int(arg)
        
    if opt == "--skim-throttle":
        skimThrottle = int(arg)
        
    if opt == "--min-submit":
        minSubmit = int(arg)
    if opt == "--max-submit":
        maxSubmit = int(arg)

    if opt == "--set-threshold":
        setThreshold = arg
    if opt == "--set-attribute":
        setAttribute = arg
    if opt == "--value":
        setThisValue = arg


modeHelp = \
"""
You must select a mode from:
 --new : Adding a new site to the DB
 --edit : Edit details for an exiting site
 --drop : Remove an existing site
 --list : Print details of sites
"""
if mode == None:
    msg = "No --mode provided\n"
    msg += modeHelp
    raise RuntimeError, msg



def newMode():
    """
    _newMode_

    Add a new site with some standard default thresholds

    """
    if site == None:
        msg = "--site option not provided"
        raise RuntimeError, msg

    if ceName == None:
        msg = "--ce-name option not provided"
        raise RuntimeError, msg
    if seName == None:
        msg = "--se-name option not provided"
        raise RuntimeError, msg


    msg = "Adding New Site named: %s\n" % site

    Session.set_database(dbConfig)
    Session.connect()
    Session.start_transaction()

    active = True
    if deactivate != None:
        active = False
    
    resCon = ResourceControlDB()
    try:
        siteIndex = resCon.newSite(site, seName, ceName, active)
    except Exception, ex:
        msg += "Error adding new site:\n%s\n" % str(ex)
        Session.rollback()
        Session.close_all()
        print msg
        sys.exit(1)
    msg += "Site Added with: \n"
    msg += " SE = %s\n" % seName
    msg += " CE = %s\n" % ceName
    msg += " Active = %s\n" % active
    msg += " Site Index Assigned to site: %s\n" % siteIndex

    
    resCon.updateThresholds(siteIndex, processingThreshold = procThreshold,
                            skimThreshold = skimThreshold,
                            mergeThreshold = mergeThreshold,
                            cleanupThreshold = cleanThreshold,
                            logcollectThreshold = logcollectThreshold,
                            repackThreshold = repThreshold,
                            expressThreshold = expressThreshold,
                            harvestingThreshold = harvestingThreshold,
                            processingRunningThrottle = procThrottle,
                            mergeRunningThrottle = mergeThrottle,
                            skimRunningThrottle = skimThrottle,
                            minimumSubmission = minSubmit,
                            maximumSubmission = maxSubmit)

    msg += " Initial Thresholds for site set to:\n"
    msg += " Processing Threshold: %s\n" % procThreshold
    msg += " Skim Threshold: %s\n" % skimThreshold
    msg += " Merge Threshold: %s\n" % mergeThreshold
    msg += " Cleanup Threshold: %s\n" % cleanThreshold
    msg += " LogCollect Threshold: %s\n" % logcollectThreshold
    msg += " Repack Threshold: %s\n" % repThreshold
    msg += " Express Threshold: %s\n" % expressThreshold
    msg += " DQM Harvesting Threshold: %s\n" % harvestingThreshold
    msg += " Processing Running Throttle: %s\n" % procThrottle
    msg += " Skim Running Throttle: %s\n" % skimThrottle
    msg += " Merge Running Throttle: %s\n" % mergeThrottle
    msg += " Minimum Submission: %s\n" % minSubmit
    msg += " Maximum Submission: %s\n" % maxSubmit

    Session.commit_all()
    Session.close_all()

    print msg
    sys.exit(0)


def editMode():
    """
    _editMode_

    Edit settings for an existing site

    """
    if site == None:
        msg = "--site option not provided"
        raise RuntimeError, msg
    msg = "Editing Site named: %s\n" % site

    
    global setThisValue

    Session.set_database(dbConfig)
    Session.connect()
    Session.start_transaction()

    resCon = ResourceControlDB()
    try:
        siteData = resCon.getSiteData(site)
    except Exception, ex:
        msg += "Unable to retrieve data for site: %s\n" % site
        msg += "Site may not be registered in ResourceControlDB\n"
        print msg
        sys.exit(1)

    msg += "Site retrieved with index: %s\n" % siteData['SiteIndex']

    if setThreshold != None:
        msg += "Setting Threshold %s...\n" % setThreshold
        if setThisValue == None:
            msg += "ERROR: --value not set, must be provided with"
            msg += " --set-threshold...\n"
            raise RuntimeError, msg
        try:
            setThisValue = int(setThisValue)
        except ValueError:
            msg += "ERROR: --value not an Integer: Must be an int provided"
            msg += " with --set-threshold...\n"
            raise RuntimeError, msg
        msg += "Setting To Value: %s " %  setThisValue
        
        try:
            args = {setThreshold : setThisValue}
            resCon.updateThresholds(siteData['SiteIndex'], **args)            
        except Exception, ex:
            msg += "Error updating threshold:\n"
            msg += str(ex)
            
            print msg
            sys.exit(1)
            
    elif setAttribute != None:
        msg += "Setting Attribute %s...\n" % setAttribute
        if setThisValue == None:
            msg += "ERROR: --value not set, must be provided with"
            msg += " --set-attribute...\n"
            raise RuntimeError, msg
        msg += "Setting To Value: %s " %  setThisValue
        
        try:
            args = {setAttribute : setThisValue}
            resCon.updateAttributes(siteData['SiteIndex'], **args)
        except Exception, ex:
            msg += "Error updating threshold:\n"
            msg += str(ex)
        
            print msg
            sys.exit(1)

    else:
        if (activate == None) and (deactivate == None):
            msg += "No edit command provided...\n"
            msg += "You should provide either:\n"
            msg += "--deactivate OR --activate\n"
            msg += "OR\n"
            msg += "--set-threshold=<ThreshName> --value=<INT>\n"
            msg += "OR\n"
            msg += "--set-attribute=<AttrName> --value=<STRING>\n"
            msg += "in --edit mode..."
            print msg
            sys.exit(1)
        if activate == True:
            msg += "Activating site..."
            try:
                resCon.activate(site)
            except Exception, ex:
                msg += "Error activating site:\n"
                msg += str(ex)
        
                print msg
                sys.exit(1)
                
        if deactivate == True:
            msg += "Deactivating site..."
            
            try:
                resCon.deactivate(site)          
            except Exception, ex:
                msg += "Error deactivating site:\n"
                msg += str(ex)
          
                print msg
                sys.exit(1)

    Session.commit_all()
    Session.close_all()
    print msg
    sys.exit(0)

def listMode():
    """
    _listMode_

    List details for the site provided by --site.
    If --site not provided, list data for all sites

    """
    Session.set_database(dbConfig)
    Session.connect()
    Session.start_transaction()
    resCon = ResourceControlDB()
    sites = []
    if site != None:
        sites.append(site)

    else:
        sites = resCon.siteNames()

    for item in sites:
        try:
            siteData = resCon.getSiteData(item)
        except Exception:
            msg = "No site information found for: %s\n" % item
            print msg
            continue
        msg = "Site Information for: %s\n" % item
        msg += "=======================================\n"
        for key, val in siteData.items():
            msg += " => %s = %s\n" %( key, val)

        thresholds = resCon.siteThresholds(siteData['SiteIndex'])
        msg += "Thresholds:\n"
        for threshName, threshValue in thresholds.items():
            msg += " => %s = %s\n" % (threshName, threshValue )
        msg += "Attributes:\n"
        attributes = resCon.siteAttributes(siteData['SiteIndex'])
        for attrName, attrValue in attributes.items():
            msg += " => %s = %s\n" % ( attrName, attrValue)
        
        print msg
    sys.exit(0)
    

def dropMode():
    """
    _dropMode_

    Remove a site from the ResourceControlDB

    """
    if site == None:
        msg = "--site option not provided"
        raise RuntimeError, msg
    msg = "Dropping Site named: %s\n" % site
    Session.set_database(dbConfig)
    Session.connect()
    Session.start_transaction()

    resCon = ResourceControlDB()
    try:
        resCon.dropSite(site)
        Session.commit_all()
        Session.close_all()
    except Exception, ex:
        msg += "Error dropping site:\n"
        msg += str(ex)
        Session.rollback()
        Session.close_all()
        print msg
        sys.exit(1)
    sys.exit(0)
                
if mode == "new":
    newMode()

elif mode == "edit":
    editMode()

elif mode == "drop":
    dropMode()

elif mode == "list":
    listMode()

    
    
        
