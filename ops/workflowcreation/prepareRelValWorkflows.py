#!/usr/bin/env python

import sys
import os
import getopt
import popen2

def main(argv) :
    """
    
    prepareRelValworkflows
    
    prepare workflows for chained processing of RelVal samples

    - parse file holding cmsDriver commands for 1st and 2nd steps
    - prepare workflows
    - prepare WorkflowInjector:Input script
    - prepare ForceMerge script
    - prepare DBSMigrationToGlobal script
    - prepare PhEDExInjection script
    - prepare local DBS query script
    
    required parameters
    --samples <textfile>           : list of RelVal sample parameter-sets in plain text file, one sample per line, # marks comment
    --version <processing version> : processing version (v1, v2, ... )
    --DBSURL <URL>                 : URL of the local DBS (http://cmsdbsprod.cern.ch/cms_dbs_prod_local_07/servlet/DBSServlet, http://cmssrv46.fnal.gov:8080/DBS126/servlet/DBSServlet)
    
    optional parameters            :
    --lumi <number>                : initial run for generation (default: 666666), set it to 777777 for high statistics samples
    --event <number>               : initial event number
    --help (-h)                    : help
    --debug (-d)                   : debug statements
    
    
    """
    
    # default
    try:
        version = os.environ.get("CMSSW_VERSION")
    except:
        print ''
        print 'CMSSW version cannot be determined from $CMSSW_VERSION'
        sys.exit(2)

    try:
        architecture = os.environ.get("SCRAM_ARCH")
    except:
        print ''
        print 'CMSSW architecture cannot be determined from $SCRAM_ARCH'
        sys.exit(2)

    samples            = None
    processing_version = None
    initial_run        = "666666"
    initial_event      = None
    debug              = 0
    DBSURL             = None

    try:
        opts, args = getopt.getopt(argv, "", ["help", "debug", "samples=", "version=", "DBSURL=", "event=", "lumi="])
    except getopt.GetoptError:
        print main.__doc__
        sys.exit(2)

    # check command line parameter
    for opt, arg in opts :
        if opt == "--help" :
            print main.__doc__
            sys.exit()
        elif opt == "--debug" :
            debug = 1
        elif opt == "--samples" :
            samples = arg
        elif opt == "--version" :
            processing_version = arg
#	elif opt == "--run" :
        elif opt == "--lumi" :
            initial_run = arg
	elif opt == "--event" :
            initial_event = arg
	elif opt == "--DBSURL" :
	    DBSURL = arg

    if initial_event == None :
	print ""
	print "Warning: Initial Event Number is not set, output workflow will not have this block."
	print ""

    if samples == None or processing_version == None or DBSURL == None :
        print main.__doc__
        sys.exit(2)

    onestep = []
    step1 = []
    step2 = {}
    parsedConditions = []

    try:
        file = open(samples)
    except IOError:
        print 'file with list of parameter-sets cannot be opened!'
        sys.exit(1)
    for line in file.readlines():
        if line != '' and line != '\n' and line.find("#") != 0 and line.find('//') != 0 :
            # parse
            primary = 'RelVal'+line.split('@@@')[0].strip()
            array = line.split('@@@')[1].strip().split()
            command = line.split('@@@')[1].strip()
            if '--conditions' in array:
                conditions = array[array.index('--conditions')+1].split(',')[1].split('::')[0].strip()
                if conditions not in parsedConditions: parsedConditions.append(conditions)
            else:
                conditions = 'SpecialConditions'
            if '--relval' in array :
                totalEvents = array[array.index('--relval')+1].split(',')[0].strip()
                eventsPerJob = array[array.index('--relval')+1].split(',')[1].strip()
            outputname = primary + '_' + conditions + '.py'

            # add command options
            if command.find('no_exec') < 0:
                command += ' --no_exec'
            if command.find('python_filename') < 0:
                command += ' --python_filename ' + outputname

            # distinguish two-step and one-step processes (one step always has RECO in process list)
            if command.find('RECO') >= 0 :
                if primary.find('RECO') >= 0 :
                    dict = {}
                    dict['command'] = command
                    dict['outputname'] = outputname
                    step2[conditions] = dict
                elif primary.find('ALCA') >= 0 :
                    # do nothing for now
                    a=1
                else :
                    dict = {}
                    dict['command'] = command
                    dict['primary'] = primary
                    dict['conditions'] = conditions
                    dict['totalEvents'] = totalEvents
                    dict['eventsPerJob'] = eventsPerJob
                    dict['outputname'] = outputname
                    onestep.append(dict)
            else :
                dict = {}
                dict['command'] = command
                dict['primary'] = primary
                dict['conditions'] = conditions
                dict['totalEvents'] = totalEvents
                dict['eventsPerJob'] = eventsPerJob
                dict['outputname'] = outputname
                step1.append(dict)

            if debug == 1:
                print 'parsing'
                print 'primary:',primary
                print 'command:',command
                print 'conditions:',conditions
                if '--relval' in array :
                    print 'totalEvents:',totalEvents
                    print 'eventsPerJob:',eventsPerJob
                print ''

    for conditions in parsedConditions:
        if conditions not in step2.keys():
            print 'Step 2 cmsDriver command for conditions: ',conditions,'not included in sample file:',samples
            sys.exit(1)

    if debug == 1:
        print 'collected information step 1'
        for sample in step1:
            print 'primary:',sample['primary']
            print 'command:',sample['command']
            print 'conditions:',sample['conditions']
            print 'totalEvents:',sample['totalEvents']
            print 'eventsPerJob:',sample['eventsPerJob']
            print 'outputname:',sample['outputname']
            print ''
        print 'collected information step 2'
        for condition in step2.keys() :
            print 'step 2 condition:',condition,'command:',step2[condition]['command'],'outputname:',step2[condition]['outputname']
            print ''
        print 'collected information onestep'
        for sample in onestep:
            print 'primary:',sample['primary']
            print 'command:',sample['command']
            print 'conditions:',sample['conditions']
            print 'totalEvents:',sample['totalEvents']
            print 'eventsPerJob:',sample['eventsPerJob']
            print 'outputname:',sample['outputname']
            print ''

    # execute cmsDriver commands
    print ''
    print 'Executing cmsDriver commands for step 1 configurations'
    print ''
    for sample in step1:
        proc = popen2.Popen3(sample['command'])
        proc.wait()
        exitCode = proc.fromchild.close()
        if exitCode == None :
            exitValue = 0
            print 'cmsDriver command for step 1 to produce:',sample['outputname'],'exited with ExitCode:',exitValue
        else :
            exitValue = exitCode
            print 'cmsDriver command for step 1 to produce:',sample['outputname'],'failed with ExitCode:',exitValue
            sys.exit(1)


    print ''
    print 'Executing cmsDriver commands for step 2 configurations'
    print ''
    for condition in step2.keys() :
        proc = popen2.Popen3(step2[condition]['command'])
        proc.wait()
        exitCode = proc.fromchild.close()
        if exitCode == None :
            exitValue = 0
            print 'cmsDriver command for step 2 to produce:',step2[condition]['outputname'],'exited with ExitCode:',exitValue
        else :
            exitValue = exitCode
            print 'cmsDriver command for step 2 to produce:',step2[condition]['outputname'],'failed with ExitCode:',exitValue
            sys.exit(1)

    print ''
    print 'Executing cmsDriver commands for single step configurations'
    print ''
    for sample in onestep:
        proc = popen2.Popen3(sample['command'])
        proc.wait()
        exitCode = proc.fromchild.close()
        if exitCode == None :
            exitValue = 0
            print 'cmsDriver command for onestep to produce:',sample['outputname'],'exited with ExitCode:',exitValue
        else :
            exitValue = exitCode
            print 'cmsDriver command for onestep to produce:',sample['outputname'],'failed with ExitCode:',exitValue
            sys.exit(1)

    print ''
    print 'Workflow creation'
    print ''

    unmergedDatasets = []
    mergedDatasets = []
    workflows = []

    # create workflows
    for sample in step1:
        command  = 'python2.4 createProductionWorkflow_CSA08Hack.py --channel=' + sample['primary'] + ' \\\n'
        command += '--py-cfg=' + sample['outputname'] + ' \\\n'
        command += '--version=' + version + ' \\\n'
        command += '--py-cfg=' + step2[sample['conditions']]['outputname']+ ' \\\n'
        command += '--stageout-intermediates=true \\\n'
        command += '--group=RelVal \\\n'
        command += '--category=relval \\\n'
        command += '--activity=RelVal \\\n'
        command += '--acquisition_era=' + version + ' \\\n'
        command += '--conditions=' + sample['conditions'] + ' \\\n'
        command += '--processing_version=' + processing_version + ' \\\n'
        command += '--only-sites=srm.cern.ch \\\n'
        command += '--starting-run=' + initial_run + ' \\\n'
	if initial_event != None :
		command += '--starting-event=' + initial_event + ' \\\n'
        command += '--totalevents=' + sample['totalEvents']+ ' \\\n'
        command += '--eventsperjob=' + sample['eventsPerJob']

        if debug == 1 :
            print command
            print ''
        
        proc = popen2.Popen3(command)
        proc.wait()
        output = proc.fromchild.readlines()

        if debug == 1 :
            print output
            print ''

        exitCode = proc.fromchild.close()
        if exitCode == None :
            exitValue = 0
            # parse output
            tmp = []
            tmp.append(output[-2].strip())
            tmp.append(output[-1].strip())
            tmp.sort()
            unmergedDatasets.append(tmp)
            workflow = output[-5].strip().split()[1]
            workflows.append(workflow)
            print 'workflow creation command for workflow:',workflow,'exited with ExitCode:',exitValue
        else :
            exitValue = exitCode
            print 'workflow creation command:'
            print command
            print 'failed'
            sys.exit(1)

    # create workflows
    for sample in onestep:
        command  = 'python2.4 createProductionWorkflow_CSA08Hack.py --channel=' + sample['primary'] + ' \\\n'
        command += '--version=' + version + ' \\\n'
        command += '--py-cfg=' + sample['outputname'] + ' \\\n'
        command += '--group=RelVal \\\n'
        command += '--category=relval \\\n'
        command += '--activity=RelVal \\\n'
        command += '--acquisition_era=' + version + ' \\\n'
        command += '--conditions=' + sample['conditions'] + ' \\\n'
        command += '--processing_version=' + processing_version + ' \\\n'
        command += '--only-sites=srm.cern.ch \\\n'
        command += '--starting-run=' + initial_run + ' \\\n'
	if initial_event != None :
		command += '--starting-event=' + initial_event + ' \\\n'
        command += '--totalevents=' + sample['totalEvents']+ ' \\\n'
        command += '--eventsperjob=' + sample['eventsPerJob']

        if debug == 1 :
            print command
            print ''
        
        proc = popen2.Popen3(command)
        proc.wait()
        output = proc.fromchild.readlines()

        if debug == 1 :
            print output
            print ''
        
        exitCode = proc.fromchild.close()
        if exitCode == None :
            exitValue = 0
            # parse output
            tmp = [output[-1].strip()]
            unmergedDatasets.append(tmp)
            workflow = output[-4].strip().split()[1]
            workflows.append(workflow)
            print 'workflow creation command for workflow:',workflow,'exited with ExitCode:',exitValue
        else :
            exitValue = exitCode
            print 'workflow creation command:'
            print command
            print 'failed'
            sys.exit(1)


    # extract merged datasets
    for sample in unmergedDatasets:
        tmp = []
        for dataset in sample:
            tmp.append(dataset.replace('-unmerged',''))
        mergedDatasets.append(tmp)

    print ''
    print 'Write helper scripts'
    print

    # WorkflowInjector:Input script
    inputScript = open('input.sh','w')
    inputScript.write('#!/bin/bash\n')
    inputScript.write('python2.4 $PRODAGENT_ROOT/util/publish.py WorkflowInjector:SetPlugin RequestFeeder\n')
    for workflow in workflows:
        inputScript.write('python2.4 $PRODAGENT_ROOT/util/publish.py WorkflowInjector:Input ' + os.path.join(os.getcwd(), workflow) + '\n')
    inputScript.close()
    os.chmod('input.sh',0755)
    print 'Wrote WorkflowInjector:Input script to:',os.path.join(os.getcwd(),'input.sh') 

    # ForceMerge
    forceMergeScript = open('forceMerge.sh','w')
    forceMergeScript.write('#!/bin/bash\n')
    for sample in unmergedDatasets :
        for dataset in sample :
            forceMergeScript.write('python2.4 $PRODAGENT_ROOT/util/publish.py ForceMerge ' + dataset + '\n')
    forceMergeScript.close()
    os.chmod('forceMerge.sh',0755)
    print 'Wrote ForceMerge script to:',os.path.join(os.getcwd(),'forceMerge.sh')

    # MigrateDatasetToGlobal
    migrateScript = open('migrateToGlobal.sh','w')
    migrateScript.write('#!/bin/bash\n')
    for sample in mergedDatasets :
        if len(sample) > 1 :
            migrateScript.write('python2.4 $DBS_CLIENT_ROOT/Clients/Python/DBSAPI/UserExamples/dbsRemap.py ' + sample[0] + ' ' + sample[1] + '\n')
        else :
            migrateScript.write('python2.4 $DBS_CLIENT_ROOT/Clients/Python/DBSAPI/UserExamples/dbsRemapOnelevel.py ' + sample[0] + '\n')
    for sample in mergedDatasets :
        for dataset in sample :
            migrateScript.write('python2.4 $PRODAGENT_ROOT/util/publish.py DBSInterface:MigrateDatasetToGlobal ' + dataset + '\n')
    migrateScript.close()
    os.chmod('migrateToGlobal.sh',0755)
    print 'Wrote DBSInterface:MigrateDatasetToGlobal script to:',os.path.join(os.getcwd(),'migrateToGlobal.sh')

    # PhEDExInjectDataset
    phedexScript = open('injectIntoPhEDEx.sh','w')
    phedexScript.write('#!/bin/bash\n')
    for sample in mergedDatasets :
        for dataset in sample :
            phedexScript.write('python2.4 $PRODAGENT_ROOT/util/publish.py PhEDExInjectDataset ' + dataset + '\n')
    phedexScript.close()
    os.chmod('injectIntoPhEDEx.sh',0755)
    print 'Wrote PhEDExInjectDataset script to:',os.path.join(os.getcwd(),'injectIntoPhEDEx.sh')

    # DBS: query unmerged datasets
    queryUnmergedScript = open('queryUnmerged.sh','w')
    queryUnmergedScript.write('#!/bin/bash\n')
    for sample in unmergedDatasets :
        queryUnmergedScript.write('python2.4 $PRODAGENT_ROOT/util/InspectDBS2.py --DBSURL=' + DBSURL  + ' --datasetPath=' + sample[0] + ' | grep total\n')
    queryUnmergedScript.close()
    os.chmod('queryUnmerged.sh',0755)
    print 'Wrote DBS query script for unmerged datasets to:',os.path.join(os.getcwd(),'queryUnmerged.sh')

    # DBS: query merged datasets
    queryMergedScript = open('queryMerged.sh','w')
    queryMergedScript.write('#!/bin/bash\n')
    for sample in mergedDatasets :
        queryMergedScript.write('python2.4 $PRODAGENT_ROOT/util/InspectDBS2.py --DBSURL=' + DBSURL + ' --datasetPath=' + sample[0] + ' | grep total\n')
    queryMergedScript.close()
    os.chmod('queryMerged.sh',0755)
    print 'Wrote DBS query script for merged datasets to:',os.path.join(os.getcwd(),'queryMerged.sh')

    print ''

if __name__ == '__main__' :
    main(sys.argv[1:])
