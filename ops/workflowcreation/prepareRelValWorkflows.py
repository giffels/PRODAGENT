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
    --samples <textfile>            : list of RelVal sample parameter-sets in plain text file, one sample per line, # marks comment
    --version <processing version>  : processing version (v1, v2, ... )
    --DBSURL <URL>                  : URL of the local DBS (http://cmsdbsprod.cern.ch/cms_dbs_prod_local_07/servlet/DBSServlet | http://cmssrv46.fnal.gov:8080/DBSFNALT1206/servlet/DBSServlet)
    --only-sites                    : Site where dataset is going to be processed or where the input dataset is taken from. Usually srm-cms.cern.ch and cmssrm.fnal.gov
    
    optional parameters
    --pileupdataset                 : input pileup dataset. It must be provided if the <samples> txt file contains PilepUp samples
    --lumi <number>                 : initial run for generation (default: 666666), set it to 777777 for high statistics samples
    --event <number>                : initial event number
    --store-fail <True|False>       : store output files for failed jobs in chain processing.
    --read-dbs                      : DBS URL used for obtaining the list of available blocks for real data. Default: http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet
    --scripts-dir                   : Path to workflow creation scripts (default: $PUTIL)
    --help (-h)                     : help
    --debug (-d)                    : debug statements
    
    
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

    try:
        from ProdCommon.DataMgmt.DBS.DBSReader import DBSReader
    except ImportError, ex:
        print ex
        print 'Please load prodAgent libraries (point $PYTHONPATH to the right path).'
        sys.exit(2)

    samples             = None
    processing_version  = None
    initial_run         = "666666"
    initial_event       = None
    debug               = 0
    DBSURL              = None
    pileup_dataset      = None
    storeFail           = False
    readDBS             = 'http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet'
    onlySites           = None
    scriptsDir          = '$PUTIL'

    try:
        opts, args = getopt.getopt(argv, "", ["help", "debug", "samples=", "version=", 
                                                "DBSURL=", "event=", "lumi=", "pileupdataset=", 
                                                "store-fail=", "read-dbs=", "only-sites=", 
                                                "scripts-dir="])
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
        elif opt == "--lumi" :
            initial_run = arg
        elif opt == "--event" :
            initial_event = arg
    	elif opt == "--DBSURL" :
            DBSURL = arg
        elif opt == "--pileupdataset" :
            pileup_dataset = arg
            print arg
        elif opt == '--store-fail':
            if arg.lower() in ("true", "yes"):
                storeFail = True
            else:
                storeFail = False
        elif opt == '--read-dbs':
            readDBS = arg
        elif opt == '--only-sites':
            onlySites = arg
        elif opt == '--scripts-dir':
            if arg.endswith('/') :
                scriptsDir = arg[:-1]
            else:
                scriptsDir = arg
            scriptsDirTemp = scriptsDir
            if scriptsDir.startswith('$') :
                scriptsDirTemp = os.environ.get(scriptsDir[1:],None)
            if scriptsDirTemp != None:
                if not os.path.exists(scriptsDirTemp):
                    print "--scripts-dir argument does not exist, please verify."
                    sys.exit(6)
            else:
                print "--scripts-dir argument does not exist, please verify."
                sys.exit(6)

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
    step3 = {}
    parsedConditions = []
    parsedRECOTags = []
    parsedALCATags = []

    try:
        file = open(samples)
    except IOError:
        print 'file with list of parameter-sets cannot be opened!'
        sys.exit(1)
    n_line = 0
    for line in file.readlines():
        n_line += 1
        isRealData = False
        if line != '' and line != '\n' and line.find("#") != 0 and line.find('//') != 0 :
            # parse
            primary = 'RelVal' + line.split('@@@')[0].split('++')[1].strip()
            if line.find('REALDATA') > -1 :
                command = ''
                array = []
                isRealData = True
                conditions = None
                pileUp = False
                totalEvents = None
                eventsPerJob = None
                outputname = ""
                SimType = ""
            else:
                command = line.split('@@@')[1].strip()
                if command.count('=') > 0 : command=command.replace('=',' ')
                array = command.split()
                if '--conditions' in array:
                    conditions = array[array.index('--conditions')+1].split(',')[1].split('::')[0].strip()
                    if conditions not in parsedConditions: parsedConditions.append(conditions)
                else:
                    conditions = 'SpecialConditions'
                if '--relval' in array :
                    totalEvents = array[array.index('--relval')+1].split(',')[0].strip()
                    eventsPerJob = array[array.index('--relval')+1].split(',')[1].strip()
                SimType = ''
                pileUp = False
                if '--pileup' in array :
                    if array[array.index('--pileup')+1].lower().strip() != 'nopileup' :
                        SimType = '_' + array[array.index('--pileup')+1].strip()
                        pileUp = True
                        if pileup_dataset == None :
                            print "Hey! You have to provide a pileup dataset."
                            print "Usually it is a MinBias (RAW)."
                            print "Use option --pileupdataset"
                            sys.exit(5)
                if command.find('FASTSIM') != -1 : SimType = '_FastSim'
                outputname = primary + '_' + conditions + SimType + '.py'

                # add command options
                if command.find('no_exec') < 0:
                    command += ' --no_exec'
                if command.find('python_filename') < 0:
                    command += ' --python_filename ' + outputname

            if len(line.split("@@@")[0].split("++")) > 3 and not isRealData:
                print "Sorry, but you need to you edit the samples file."
                print "Only two \"++\" should be in the line %d:\n%s" % (n_line,line.split("@@@")[0])
                print "Syntax should be like this for MC samples:"
                print "## ++ SampleName ++ RECOTag, ALCATag @@@ cmsDriver... or"
                print "## ++ SampleName ++ none @@@ cmsDriver... in case there is no chained processing."
                print ""
                sys.exit(4)

            inputData = {}
            inputBlocks = ""
            if isRealData:
                for parameter in line.split("@@@")[0].split('++')[3].split(','):
                    inputData[parameter.split(':')[0].strip()] = parameter.split(':')[1].strip()
                #  //
                # // Looking up the blocks for a given Dataset and a given run
                #//
                reader = DBSReader(readDBS)
                inputFiles = reader.dbs.listFiles(path=inputData['REALDATA'],runNumber=inputData['RUN'])
                blocks = {}
                for inputFile in inputFiles:
                    blocks[inputFile['Block']['Name']] = None
                inputBlocks = ",".join(blocks.keys())
            
            # Is a first step command?
            if len(line.split('@@@')[0].split('++')) > 2 : #Yes
                chain = line.split('@@@')[0].split('++')[2].split(',')
                ALCAtag = RECOtag = None
                if len(chain) >= 1 :
                    RECOtag = chain[0].strip()
                if len(chain) >= 2 :
                    ALCAtag = chain[1].strip()

            # distinguish two-step and one-step processes (one step always has RECO in process list)
            if command.find('RECO') >= 0 :
                if primary.find('RECO') >= 0 :
                    dict = {}
                    dict['command'] = command
                    dict['outputname'] = outputname
                    dict['conditions'] = conditions
                    step2[line.split('@@@')[0].split('++')[1].strip()] = dict
                elif primary.find('ALCA') >= 0 :
                    dict = {}
                    dict['command'] = command
                    dict['outputname'] = outputname
                    step3[line.split('@@@')[0].split('++')[1].strip()] = dict
                else :
                    dict = {}
                    dict['command'] = command
                    dict['primary'] = primary
                    dict['conditions'] = conditions
                    dict['totalEvents'] = totalEvents
                    dict['eventsPerJob'] = eventsPerJob
                    dict['outputname'] = outputname
                    dict['version'] = ""
                    if SimType != '' :
                        dict['version'] = SimType.strip('_') + "_"
                    onestep.append(dict)
            else :
                dict = {}
                if len(chain) >= 2 :
                    dict['steps'] = 3
                    dict['ALCAtag'] = ALCAtag
                else :
                    dict['steps'] = 2
                dict['RECOtag'] = RECOtag
                dict['command'] = command
                dict['isRealData'] = isRealData
                dict['inputData'] = inputData
                dict['inputBlocks'] = inputBlocks
                dict['primary'] = primary
                dict['conditions'] = conditions
                dict['totalEvents'] = totalEvents
                dict['eventsPerJob'] = eventsPerJob
                dict['outputname'] = outputname
                dict['version'] = ""
                if SimType != '' :
                    dict['version'] = SimType.strip('_') + "_"
                dict['pileUp'] = pileUp
                step1.append(dict)

            if debug == 1:
                print 'parsing'
                print 'primary:',primary
                print 'command:',command
                print 'conditions:',conditions
                if '--relval' in array :
                    print 'totalEvents:',totalEvents
                    print 'eventsPerJob:',eventsPerJob
                print 'RECOtag:',RECOtag
                print 'ALCAtag:',ALCAtag
                print 'Steps:',chain
                print 'PileUp:',pileUp
                print ''

    for tag in parsedRECOTags:
        if tag not in step2.keys():
            print 'Step 2 cmsDriver command for: ',tag,' not included in sample file:',samples
            sys.exit(1)
    for tag in parsedALCATags:
        if tag not in step2.keys():
            print 'Step 2 cmsDriver command for: ',tag,' not included in sample file:',samples
            sys.exit(1)

    if debug == 1:
        print 'collected information step 1'
        for sample in step1:
            print 'primary:',sample['primary']
            print 'command:',sample['command']
            print 'isRealData:',sample['isRealData']
            print 'inputData:',sample['inputData']
            print 'inputBlocks',sample['inputBlocks']
            print 'conditions:',sample['conditions']
            print 'totalEvents:',sample['totalEvents']
            print 'eventsPerJob:',sample['eventsPerJob']
            print 'outputname:',sample['outputname']
            print ''
        print 'collected information step 2'
        for tag in step2.keys() :
            print 'step 2 condition:',tag,' command:',step2[tag]['command'],' outputname:',step2[tag]['outputname']
            print ''
        print 'collected information step 3'
        for tag in step3.keys() :
            print 'step 3 condition:',tag,' command:',step3[tag]['command'],' outputname:',step3[tag]['outputname']
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
        if not sample['isRealData'] :
            proc = popen2.Popen3(sample['command'])
            exitCode = proc.wait()
            if exitCode == 0 :
                print 'cmsDriver command for step 1 to produce:',sample['outputname'],'exited with ExitCode:',exitCode
            else :
                print 'cmsDriver command for step 1 to produce:',sample['outputname'],'failed with ExitCode:',exitCode
                sys.exit(1)
        else :
            msg = 'Real Data:\n'
            msg += 'Input dataset: %s\n' % (sample['inputData']['REALDATA'])
            msg += 'Run: %s\n' % (sample['inputData']['RUN'])
            msg += 'Input blocks: %s' % (sample['inputBlocks'])
            print msg

    print ''
    print 'Executing cmsDriver commands for step 2 configurations'
    print ''
    for condition in step2.keys() :
        proc = popen2.Popen3(step2[condition]['command'])
        exitCode = proc.wait()
        if exitCode == 0 :
            print 'cmsDriver command for step 2 to produce:',step2[condition]['outputname'],'exited with ExitCode:',exitCode
        else :
            print 'cmsDriver command for step 2 to produce:',step2[condition]['outputname'],'failed with ExitCode:',exitCode
            sys.exit(1)

    print ''
    print 'Executing cmsDriver commands for step 3 configurations'
    print ''
    for condition in step3.keys() :
        proc = popen2.Popen3(step3[condition]['command'])
        exitCode = proc.wait()
        if exitCode == 0 :
            print 'cmsDriver command for step 3 to produce:',step3[condition]['outputname'],'exited with ExitCode:',exitCode
        else :
            print 'cmsDriver command for step 3 to produce:',step3[condition]['outputname'],'failed with ExitCode:',exitCode
            sys.exit(1)

    print ''
    print 'Executing cmsDriver commands for single step configurations'
    print ''
    for sample in onestep:
        proc = popen2.Popen3(sample['command'])
        exitCode = proc.wait()
        if exitCode == 0 :
            print 'cmsDriver command for onestep to produce:',sample['outputname'],'exited with ExitCode:',exitCode
        else :
            print 'cmsDriver command for onestep to produce:',sample['outputname'],'failed with ExitCode:',exitCode
            sys.exit(1)

    print ''
    print 'Workflow creation'
    print ''

    unmergedDatasets = []
    mergedDatasets = []
    workflows = {}


    # create workflows
    for sample in step1:
        if sample['isRealData'] :
            if sample['steps'] == 2 :
                command  = 'python2.4 ' + scriptsDir + '/createProcessingWorkflow.py \\\n'
                command += '--override-channel=' + sample['primary'] + ' \\\n'
                command += '--version=' + version + ' \\\n'
                command += '--py-cfg=' + step2[sample['RECOtag']]['outputname'] + ' \\\n'
                command += '--group=RelVal \\\n'
                command += '--category=relval \\\n'
                command += '--activity=RelVal \\\n'
                command += '--acquisition_era=' + version + ' \\\n'
                command += '--conditions=' + step2[sample['RECOtag']]['conditions'] + ' \\\n'
                command += '--processing_version=' + sample['version'] + processing_version + ' \\\n'
                command += '--only-sites=' + onlySites + ' \\\n'
                command += '--dataset=' + sample['inputData']['REALDATA'] + ' \\\n'
                command += '--dbs-url=' + readDBS + ' \\\n'
                command += '--split-type=file \\\n'
                command += '--split-size=1 \\\n'
                command += '--only-blocks=' + sample['inputBlocks']
            else :
                command  = 'python2.4 ' + scriptsDir + '/createProcessingWorkflow.py \\\n'
                command += '--override-channel=' + sample['primary'] + ' \\\n'
                command += '--version=' + version + ' \\\n'
                command += '--py-cfg=' + step2[sample['RECOtag']]['outputname'] + ' \\\n'
                #  //
                # // Currently not supported by prodAgent!
                #//
                #command += '--version=' + version + ' \\\n'
                #command += '--py-cfg=' + step2[sample['ALCAtag']]['outputname'] + ' \\\n'
                #command += '--stageout-intermediates=true \\\n'
                command += '--group=RelVal \\\n'
                command += '--category=relval \\\n'
                command += '--activity=RelVal \\\n'
                command += '--acquisition_era=' + version + ' \\\n'
                command += '--conditions=' + step2[sample['RECOtag']]['conditions'] + ' \\\n'
                command += '--processing_version=' + sample['version'] + processing_version + ' \\\n'
                command += '--only-sites=' + onlySites + ' \\\n'
                command += '--dataset=' + sample['inputData']['REALDATA'] + ' \\\n'
                command += '--dbs-url=' + readDBS + ' \\\n'
                command += '--split-type=file \\\n'
                command += '--split-size=1 \\\n'
                command += '--only-blocks=' + sample['inputBlocks']
        else :
            if sample['steps'] == 2 :
                command  = 'python2.4 ' + scriptsDir + '/createProductionWorkflow.py --channel=' + sample['primary'] + ' \\\n'
                command += '--version=' + version + ' \\\n'
                command += '--py-cfg=' + sample['outputname'] + ' \\\n'
                command += '--version=' + version + ' \\\n'
                command += '--py-cfg=' + step2[sample['RECOtag']]['outputname'] + ' \\\n'
                command += '--stageout-intermediates=true \\\n'
                command += '--group=RelVal \\\n'
                command += '--category=relval \\\n'
                command += '--activity=RelVal \\\n'
                command += '--acquisition_era=' + version + ' \\\n'
                command += '--conditions=' + sample['conditions'] + ' \\\n'
                command += '--processing_version=' + sample['version'] + processing_version + ' \\\n'
                command += '--only-sites=' + onlySites + ' \\\n'
                command += '--starting-run=' + initial_run + ' \\\n'
                if initial_event != None :
                    command += '--starting-event=' + initial_event + ' \\\n'
                command += '--totalevents=' + sample['totalEvents']+ ' \\\n'
                command += '--eventsperjob=' + sample['eventsPerJob']
                if sample['pileUp'] :
                    command += ' \\\n--pileup-dataset=' + pileup_dataset
                if storeFail :
                    command += ' \\\n--store-fail=True' 
            else :
                command  = 'python2.4 ' + scriptsDir + '/createProductionWorkflow.py --channel=' + sample['primary'] + ' \\\n'
                command += '--version=' + version + ' \\\n'
                command += '--py-cfg=' + sample['outputname'] + ' \\\n'
                command += '--version=' + version + ' \\\n'
                command += '--py-cfg=' + step2[sample['RECOtag']]['outputname']+ ' \\\n'
                command += '--stageout-intermediates=true \\\n'
                command += '--version=' + version + ' \\\n'
                command += '--py-cfg=' + step3[sample['ALCAtag']]['outputname']+ ' \\\n'
                command += '--stageout-intermediates=true \\\n'
                command += '--group=RelVal \\\n'
                command += '--category=relval \\\n'
                command += '--activity=RelVal \\\n'
                command += '--acquisition_era=' + version + ' \\\n'
                command += '--conditions=' + sample['conditions'] + ' \\\n'
                command += '--processing_version=' + sample['version'] + processing_version + ' \\\n'
                command += '--only-sites=' + onlySites + ' \\\n'
                command += '--starting-run=' + initial_run + ' \\\n'
                if initial_event != None :
                    command += '--starting-event=' + initial_event + ' \\\n'
                command += '--totalevents=' + sample['totalEvents']+ ' \\\n'
                command += '--eventsperjob=' + sample['eventsPerJob']
                if sample['pileUp'] :
                    command += ' \\\n--pileup-dataset=' + pileup_dataset
                if storeFail :
                    command += ' \\\n--store-fail=True'

        if debug == 1 :
            print command
            print ''
        
        proc = popen2.Popen3(command)
        ExitCode = proc.wait()
        output = proc.fromchild.readlines()

        if debug == 1 :
            print output
            print ''

        if exitCode == 0 :
            # parse output
            tmp = []
            index = FindIndex(output,'Output Datasets')
            for dataset in output[index+1:] : tmp.append(dataset.strip())
            unmergedDatasets.append(tmp)
            index = FindIndex(output,'Created')
            if index == -1 :
                print "No workflow was created by create*workflow.py"
                sys.exit(1)
            workflow = output[index].split()[1].strip()
            workflows[workflow] = sample['isRealData']
            print 'workflow creation command for workflow:',workflow,'exited with ExitCode:',exitCode
        else :
            print 'workflow creation command:'
            print command
            print 'failed'
            sys.exit(1)

    # create workflows
    for sample in onestep:
        command  = 'python2.4 ' + scriptsDir + '/createProductionWorkflow.py --channel=' + sample['primary'] + ' \\\n'
        command += '--version=' + version + ' \\\n'
        command += '--py-cfg=' + sample['outputname'] + ' \\\n'
        command += '--group=RelVal \\\n'
        command += '--category=relval \\\n'
        command += '--activity=RelVal \\\n'
        command += '--acquisition_era=' + version + ' \\\n'
        command += '--conditions=' + sample['conditions'] + ' \\\n'
        command += '--processing_version=' + sample['version'] + processing_version + ' \\\n'
        command += '--only-sites=' + onlySites + ' \\\n'
        command += '--starting-run=' + initial_run + ' \\\n'
        if initial_event != None :
            command += '--starting-event=' + initial_event + ' \\\n'
        command += '--totalevents=' + sample['totalEvents']+ ' \\\n'
        command += '--eventsperjob=' + sample['eventsPerJob']

        if debug == 1 :
            print command
            print ''
        
        proc = popen2.Popen3(command)
        exitCode = proc.wait()
        output = proc.fromchild.readlines()

        if debug == 1 :
            print output
            print ''
        
        if exitCode == 0 :
            # parse output
            tmp = []
            index = FindIndex(output,'Output Datasets')
            for dataset in output[index+1:] : tmp.append(dataset.strip())
            unmergedDatasets.append(tmp)
            index = FindIndex(output,'Created')
            if index == -1 :
                print "No workflow was created by createProductionWorkflow_CSAi08Hack.py"
                sys.exit(1)
            workflow = output[index].split()[1].strip()
            workflows[workflow] = False 
            print 'workflow creation command for workflow:',workflow,'exited with ExitCode:',exitCode
        else :
            exitValue = exitCode
            print 'workflow creation command:'
            print command
            print 'failed'
            sys.exit(1)
    
    if debug == 1 :
        print 'Created workflows:'
        print workflows.keys()	
        print ''
        print "Unmerged datasets:"
        print unmergedDatasets

    # extract merged datasets
    for sample in unmergedDatasets:
        tmp = []
        for dataset in sample:
            tmp.append(dataset.replace('-unmerged',''))
        mergedDatasets.append(tmp)

    print ''
    print 'Write helper scripts'
    print ''

    # WorkflowInjector:Input script
    inputScript = open('input.sh','w')
    inputScript.write('#!/bin/bash\n')
    feeder = 'None'
    for workflow in workflows.keys():
        if workflows[workflow]:
            if feeder.find('ReReco') < 0:
                inputScript.write('python2.4 $PRODAGENT_ROOT/util/publish.py WorkflowInjector:SetPlugin BlockFeeder\n')
                feeder = 'ReReco'
        else :
            if feeder.find('Request') < 0:
                inputScript.write('python2.4 $PRODAGENT_ROOT/util/publish.py WorkflowInjector:SetPlugin RequestFeeder\n')
                feeder = 'Request'
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
        for dataset in sample :
            #if dataset.find('-RECO') == -1 or len(sample) == 1 :
            queryUnmergedScript.write('python2.4 $PRODAGENT_ROOT/util/InspectDBS2.py --DBSURL=' + DBSURL  + ' --datasetPath=' + dataset + ' | grep total\n')
    queryUnmergedScript.close()
    os.chmod('queryUnmerged.sh',0755)
    print 'Wrote DBS query script for unmerged datasets to:',os.path.join(os.getcwd(),'queryUnmerged.sh')

    # DBS: query merged datasets
    queryMergedScript = open('queryMerged.sh','w')
    queryMergedScript.write('#!/bin/bash\n')
    for sample in mergedDatasets :
        for dataset in sample :
            #if dataset.find('-RECO') == -1 or len(sample) == 1 :
            queryMergedScript.write('python2.4 $PRODAGENT_ROOT/util/InspectDBS2.py --DBSURL=' + DBSURL  + ' --datasetPath=' + dataset + ' | grep total\n')
    queryMergedScript.close()
    os.chmod('queryMerged.sh',0755)
    print 'Wrote DBS query script for merged datasets to:',os.path.join(os.getcwd(),'queryMerged.sh')

    # DQMHarvesting
    DQMinputScript = open('DQMinput.sh','w')
    DQMinputScript.write("#!/bin/bash\n")
    for sample in mergedDatasets :
        for dataset in sample :
            if dataset.find('RECO') != -1 :
                primary = dataset.split("/")[1]
                processed = dataset.split("/")[2]
                tier = dataset.split("/")[3]
                DQMinputScript.write('python2.4 $PRODAGENT_ROOT/util/harvestDQM.py  --run=1 --primary=' + primary  + ' --processed=' + processed + ' --tier=' + tier + '\n' )
    os.chmod('DQMinput.sh',0755)
    print 'Wrote DQMHarvesting script for merged datasets to:', os.path.join(os.getcwd(),'DQMinput.sh')

    # Output datasets list
    outputList = open('outputDatasets.txt','w')
    for sample in mergedDatasets :
        for dataset in sample :
            outputList.write(dataset + "\n")
    print 'Wrote output datasets list to:', os.path.join(os.getcwd(),'outputDatasets.txt')

    print ''

def FindIndex(output,string) :
    index = -1
    counter = 0
    for field in output :
        if field.find(string) != -1 : index = counter
        counter += 1
    return index
 
if __name__ == '__main__' :
    main(sys.argv[1:])
