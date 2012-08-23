#!/usr/bin/env python
"""
_RuntimeOfflineDQM_

Harvester script for DQM Histograms.

Will do one/both of the following:
1. Copy the DQM Histogram file to the local SE, generating an LFN name for it
2. Post the DQM Histogram file to a Siteconf discovered DQM Server URL

"""
import os
import sys
import httplib
import urllib2
import traceback
import re

from cStringIO import StringIO
from mimetypes import guess_type
from gzip import GzipFile

# Compatibility with python2.3 or earlier
HTTPS = httplib.HTTPS
if sys.version_info[:3] >= (2, 4, 0):
    HTTPS = httplib.HTTPSConnection

# Compatibility with python2.4 or earlier
try:
    from hashlib import md5
except ImportError:
    from md5 import md5

from ProdCommon.FwkJobRep.TaskState import TaskState
from ProdCommon.FwkJobRep.FwkJobReport import FwkJobReport
from ProdCommon.FwkJobRep.MergeReports import updateReport
from ProdCommon.MCPayloads.UUID import makeUUID
from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec

from StageOut.StageOutMgr import StageOutMgr
from StageOut.StageOutError import StageOutInitError
import StageOut.Impl

# Default parameters
_DoHTTPPost = True
_DoStageOut = False
_DoCERNCopy  = False
_HTTPPostURL = 'https://cmsweb.cern.ch/dqm/dev' #test instance
#_HTTPPostURL = 'https://cmsweb.cern.ch/dqm/offline' # prod instance

#  //
# // Parameters used to control the stage out of DQM histograms back to CERN
#//
CERNStageOut = {
    "command" : "srmv2",
    "option" : [],
    "se-name" : "srm.cern.ch",
    "lfn-prefix" : "srm://srm-cms.cern.ch:8443/srm/managerv2?SFN=/castor/cern.ch/cms/",
    }

__revision__ = "$Id: RuntimeOfflineDQM.py,v 1.27 2011/05/04 00:51:10 hufnagel Exp $"
__version__ = "$Revision: 1.27 $"


class HarvesterImpl:
    """
    _HarvesterImpl_

    Implementation class for the upload to facilitate easier testing outside of an actual
    harvesting job environment

    """
    def __init__(self):
        self.proxyLocation = None
        self.uploadUrl = None
        self.workflowSpecId = None
        self.jobSpecId = None
        self.inputDataset = None
        self.thisSite = None
        self.mssNames = {}
        self.doStageOut = _DoStageOut
        self.doHttpPost = _DoHTTPPost
        self.doCernCopy = _DoCERNCopy
        self.copyCommandParameters = CERNStageOut

    def __call__(self, aFile):
        """
        _operator(analysisFile)_

        Do the upload for the file provided

        """
        # only process DQM files
        if aFile['FileClass'] != "DQM":
            return

        if aFile['FileName'].startswith("./"):
            aFile['FileName'] = aFile['FileName'].replace("./", "")

        msg = "==========Handling Analysis File=========\n"
        for key, value in aFile.items():
            msg += " => %s: %s\n" % (key, value)
        print msg

        if self.doStageOut:
            try:
                self.stageOut(aFile)
            except Exception, ex:
                msg = "Failure processing stage out:\n"
                msg += "For File: %s\n" % aFile['FileName']
                msg += "%s\n" % str(ex)
                msg += "\n".join([
                    str(x) for x in traceback.format_exception(*sys.exc_info())
                    ])
                print msg
                raise RuntimeError, msg
        else:
            print "Info: doStageOut flag is set to False, not staging out.\n"
        
        if self.doHttpPost:
            try:
                self.httpPost(aFile)
            except Exception, ex:
                msg = "Failure processing Http Post:\n"
                msg += "For File: %s\n" % aFile['FileName']
                msg += "%s\n" % str(ex)
                msg += "\n".join([
                    str(x) for x in traceback.format_exception(*sys.exc_info())
                    ])
                print msg
                raise RuntimeError, msg
        else :
            print "Info: doHttpPost flag is set to False, not posting.\n"
        
        if self.doCernCopy:
            try:
                self.cernStageOut(aFile)
            except Exception, ex:
                msg = "Failure to do copy to CERN\n"
                msg += "For File: %s\n" % aFile['FileName']
                msg += "%s\n" % str(ex)
                msg += "\n".join([
                    str(x) for x in traceback.format_exception(*sys.exc_info())
                    ])
                print msg
                raise RuntimeError, msg
        else :
            print "Info: doCernCopy flag is set to False, not copying files to CERN.\n"


    def buildLFN(self, analysisFile):
        """
        _buildLFN_

        This method creates the LFN which the DQM files will be stage out with
        The LFN will have the ofllowing structure:

        /store/unmerged/dqm/[acq_era]/[sample_name]/[TIER]/[processing_string-processing_version]/[run_padding]/[analysis_file].root

        The different parts of the LFN will be figure out from the analysis
        file name which should match the following re:

        ^(DQM)_V\d+(_[A-Za-z]+)?_R(\d+)(__.*)?\.root

        where thelast group corresponds to the dataset name.
        """

        lfn_prefix = '/store/unmerged/dqm/'


        file_name = analysisFile['FileName']
        filebasename = os.path.basename(file_name)

        m = re.match(r'^(DQM)_V\d+(_[A-Za-z]+)?_R(\d+)(__.*)?\.root', 
                     filebasename)

        if not m:
            msg = "Unable to stage out DQM file %s: " \
                  "It's name does not match the expected " \
                  "convention." % filebasename
            print msg
            raise RuntimeError, msg

        run_number = int(m.group(3))
        #run_padding0 = str(run_number % 1000).zfill(3)
        run_padding1 = str(run_number // 1000).zfill(3)

        dataset_name = m.group(4).replace("__", "/")

        if re.match(r'^(/[-A-Za-z0-9_]+){3}$', dataset_name) is None:
            msg = "Unable to stage out DQM file %s: " \
                  "Dataset %s It does not match the expected " \
                  "convention." % (filebasename, dataset_name)
            print msg
            raise RuntimeError, msg

        m1 = re.match(r'^/([-A-Za-z0-9_]+)/?([A-Za-z0-9_]+)-([-A-Za-z0-9_]+)/([-A-Za-z0-9_]+)',
                      dataset_name)

        acq_era = m1.group(2)
        primary_ds = m1.group(1)
        tier = m1.group(4)
        proc_string = m1.group(3)

        lfn = os.path.join(lfn_prefix, acq_era, primary_ds, tier, proc_string,
                           run_padding1, filebasename)
                           #run_padding1, run_padding0, filebasename)

        return lfn


    def stageOut(self, analysisFile):
        """
        _stageOut_

        stage out the DQM Histogram to local storage

        """
        filename = analysisFile['FileName']
        try:
            stager = StageOutMgr()
        except Exception, ex:
            msg = "Unable to stage out DQM File:\n"
            msg += str(ex)
            raise RuntimeError, msg

        filebasename = os.path.basename(filename)

        fileInfo = {
            'LFN' : self.buildLFN(analysisFile),
            'PFN' : os.path.join(os.getcwd(), filename),
            'SEName' : None,
            'GUID' : filebasename.replace(".root", ""),
            }

        try:
            stager(**fileInfo)
        except Exception, ex:
            msg = "Unable to stage out DQM File:\n"
            msg += str(ex)
            raise RuntimeError, msg

        storagePFN = stager.searchTFC(fileInfo['LFN'])
        self.mssNames[filename] = storagePFN
        analysisFile['StoragePFN'] = storagePFN
        return


    def cernStageOut(self, analysisFile):
        """
        _stageOut_

        stage out the DQM Histogram to local storage

        """
        filename = analysisFile['FileName']

        #  //
        # // In case the site is T1_US_FNAL, special options are added to the 
        #// srmv2 command
        #\\
        if self.thisSite.lower().find('t1_us_fnal') > -1 :
            self.copyCommandParameters['option'] = \
            '-use_urlcopy_script -urlcopy=/opt/d-cache/srm/sbin/url-copy.sh'
            
        try:
            stager = StageOutMgr(**self.copyCommandParameters)
        except Exception, ex:
            msg = "Unable to stage out DQM File to CERN:\n"
            msg += str(ex)
            raise RuntimeError, msg


        filebasename = os.path.basename(filename)
        filebasename = filebasename.replace(".root", "")

        if self.thisSite != None:
            lfn = "/store/unmerged/dqm/%s/%s/%s/%s" % (
                self.thisSite,
                self.workflowSpecId,
                self.jobSpecId,
                filebasename)
        else:
            lfn = "/store/unmerged/dqm/%s/%s/%s" % (
                self.workflowSpecId,
                self.jobSpecId,
                filebasename)

        fileInfo = {
            'LFN' : lfn,
            'PFN' : os.path.join(os.getcwd(), filename),
            'SEName' : None,
            'GUID' : filebasename,
            }

        try:
            stager(**fileInfo)
        except Exception, ex:
            msg = "Unable to stage out DQM File To CERN:\n"
            msg += str(ex)
            raise RuntimeError, msg

        analysisFile['CERNLFN'] = fileInfo['LFN']
        analysisFile['CERNPFN'] = fileInfo['PFN']
        return


    def httpPost(self, analysisFile):
        """
        _httpPost_

        perform an HTTP POST operation to a webserver

        """
        filename = analysisFile['FileName']

        args = {}

        # Preparing a checksum
        blockSize = 0x10000
        def upd(m, data):
            m.update(data)
            return m
        fd = open(filename, 'rb')
        try:
            contents = iter(lambda: fd.read(blockSize), '')
            m = reduce(upd, contents, md5())
        finally:
            fd.close()
            
        args['checksum'] = 'md5:%s' % m.hexdigest()
        #args['checksum'] = 'md5:%s' % md5.new(filename).read()).hexdigest()
        args['size'] = os.path.getsize(filename)

        msg = "HTTP Upload is about to start:\n"
        msg += " => URL: %s\n" % self.uploadUrl
        msg += " => Filename: %s\n" % filename
        print msg

        try:
            (headers, data) = self.upload(self.uploadUrl, args, filename)
            print 'HTTP upload finished succesfully with response:'
            print 'Status code: ', headers.get("Dqm-Status-Code", "None")
            print 'Message: ', headers.get("Dqm-Status-Message", "None")
            print 'Detail: ', headers.get("Dqm-Status-Detail", "None")
            print 'Data: ', str(data)
        except urllib2.HTTPError, ex:
            print 'HTTP upload failed with response:'
            print 'Status code: ', ex.hdrs.get("Dqm-Status-Code", "None")
            print 'Message: ', ex.hdrs.get("Dqm-Status-Message", "None")
            print 'Detail: ', ex.hdrs.get("Dqm-Status-Detail", "None")
            print 'Error: ', str(ex)
            raise RuntimeError, ex
        except Exception, ex:
            print 'HTTP upload failed with response:'
            print 'Problem unknown.'
            print 'Error: ', str(ex)
            raise RuntimeError, ex

        return

    def filetype(self, filename):
        return guess_type(filename)[0] or 'application/octet-stream'

    def encode(self, args, files):
        """
        Encode form (name, value) and (name, filename, type) elements into
        multi-part/form-data. We don't actually need to know what we are
        uploading here, so just claim it's all text/plain.
        """
        boundary = '----------=_DQM_FILE_BOUNDARY_=-----------'
        (body, crlf) = ('', '\r\n')
        for (key, value) in args.items():
            payload = str(value)
            body += '--' + boundary + crlf
            body += ('Content-Disposition: form-data; name="%s"' % key) + crlf
            body += crlf + payload + crlf
        for (key, filename) in files.items():
            body += '--' + boundary + crlf
            body += ('Content-Disposition: form-data; name="%s"; filename="%s"'
                     % (key, os.path.basename(filename))) + crlf
            body += ('Content-Type: %s' % self.filetype(filename)) + crlf
            body += ('Content-Length: %d' % os.path.getsize(filename)) + crlf
            body += crlf + open(filename, "r").read() + crlf
            body += '--' + boundary + '--' + crlf + crlf
        return ('multipart/form-data; boundary=' + boundary, body)

    def marshall(self, args, files, request):
        """
        Marshalls the arguments to the CGI script as multi-part/form-data,
        not the default application/x-www-form-url-encoded. This improves
        the transfer of the large inputs and eases command line invocation
        of the CGI script.
        """
        (type, body) = self.encode(args, files)
        request.add_header('Content-Type', type)
        request.add_header('Content-Length', str(len(body)))
        request.add_data(body)
        return

    def upload(self, url, args, filename):
        """
        _upload_

        Perform a file upload to the dqm server using HTTPS auth with the
        service proxy provided

        """
        uploadProxy = self.proxyLocation

        class HTTPSCertAuth(HTTPS):
            def __init__(self, host, *args, **kwargs):
                HTTPS.__init__(self, host,
                               key_file = uploadProxy,
                               cert_file = uploadProxy,
                               **kwargs)

        class HTTPSCertAuthenticate(urllib2.AbstractHTTPHandler):
            def default_open(self, req):
                return self.do_open(HTTPSCertAuth, req)

        ident = "ProdAgent python/%d.%d.%d" % sys.version_info[:3]

        msg = "HTTP POST upload arguments:\n"
        for arg in args:
            msg += "  ==> %s: %s\n" % (arg, args[arg])
        print msg

        datareq = urllib2.Request(url + '/data/put')
        datareq.add_header('Accept-encoding', 'gzip')
        datareq.add_header('User-agent', ident)
        self.marshall(args, {'file' : filename}, datareq)
        if 'https://' in url:
            result = urllib2.build_opener(HTTPSCertAuthenticate()).open(datareq)
        else:
            result = urllib2.build_opener(urllib2.ProxyHandler({})).open(datareq)

        data = result.read()
        if result.headers.get('Content-encoding', '') == 'gzip':
            data = GzipFile(fileobj=StringIO(data)).read ()
        return (result.headers, data)


class OfflineDQMHarvester:
    """
    _OfflineDQMHarvester_

    Util to trawl through a Framework Job Report to find analysis
    files and copy them to some DQM server

    """
    def __init__(self):
        self.state = TaskState(os.getcwd())
        self.state.loadRunResDB()
        self.config = self.state.configurationDict()
        self.workflowSpecId = self.config['WorkflowSpecID'][0]
        self.jobSpecId = self.config['JobSpecID'][0]
        self.toplevelReport = os.path.join(os.environ['PRODAGENT_JOB_DIR'],
                                  "FrameworkJobReport.xml")


        try:
            self.state.loadJobReport()
        except Exception, ex:
            print "Error Reading JobReport:"
            print str(ex)
            self.state._JobReport = None

        self.state.loadJobSpecNode()

        self.uploadUrl = _HTTPPostURL
        doHttpPost = _DoHTTPPost
        workflow = WorkflowSpec()
        workflow.load(os.environ['PRODAGENT_WORKFLOW_SPEC'])
        if workflow.parameters.has_key("DQMServer"):
            self.uploadUrl = workflow.parameters['DQMServer']
        else :
            doHttpPost = False

        cernStageOut = False
        if workflow.parameters.has_key("DQMCopyToCERN"):
            if str(workflow.parameters['DQMCopyToCERN']).lower() == "true":
                cernStageOut = True

        doStageOut = False
        if workflow.parameters.has_key("DoStageOut"):
            if str(workflow.parameters['DoStageOut']).lower() == "true":
                doStageOut = True


        #  //
        # // Lookup proxy, first from workflow for explicit path
        #//  Fallback to X509_PROXY as defined in grid jobs
        self.proxyLocation = workflow.parameters.get('proxyLocation', None)
        if doHttpPost : 
            possibleProxyVars = [
                "X509_PROXY",
                "X509_USER_PROXY",
                ]
            if self.proxyLocation == None:
                for ppv in possibleProxyVars:
                    value = os.environ.get(ppv, None)
                    if value is None:
                        continue
                    if not os.path.exists(value):
                        continue
                    self.proxyLocation = value
                    break
            if self.proxyLocation is None:
                msg = "===PROXY FAIL===\n"
                msg += "Unable to find proxy for HTTPS Upload\n"
                msg += "Cannot determine location of proxy"
                print msg

            if not os.path.exists(self.proxyLocation):
                msg = "===PROXY FAIL===\n"
                msg += "Proxy file does not exist:\n"
                msg += "%s\n" % self.proxyLocation
                msg += "Cannot proceed with HTTPS Upload without proxy\n"
                print msg
                self.proxyLocation = None


        jobSpecNode = self.state.jobSpecNode
        inputDataset = jobSpecNode._InputDatasets[0]

        siteConf = self.state.getSiteConfig()
        siteName = siteConf.siteName

        self.impl = HarvesterImpl()
        self.impl.inputDataset = inputDataset.name()
        self.impl.proxyLocation = self.proxyLocation
        self.impl.uploadUrl = self.uploadUrl
        self.impl.workflowSpecId = self.workflowSpecId
        self.impl.jobSpecId = self.jobSpecId
        self.impl.thisSite = siteName
        self.impl.doCernCopy = cernStageOut
        self.impl.doHttpPost = doHttpPost
        self.impl.doStageOut = doStageOut



    def __call__(self):
        """
        _operator()_

        Invoke this object to find files and do stage out

        """
        print "\n==> Preparing upload of analysis files to the DQM Server.\n"

        if self.state._JobReport is None:
            msg = "No Job Report available or could not be read.\n"
            msg += "==> Unable to process analysis files for offline DQM\n"
            print msg
            print "Creating JobReport by hand..."
            self.state._JobReport = jobRep = FwkJobReport()
            jobRep.name = self.state.taskAttrs['Name']
            jobRep.workflowSpecId = self.state.taskAttrs['WorkflowSpecID']
            jobRep.jobSpecId = self.state.jobSpec.parameters['JobName']
            jobRep.jobType = self.state.taskAttrs['JobType']
            error = jobRep.addError(50115, "DQMProxyError")
            error['Description'] = msg
            jobRep.status = "Failed"
            jobRep.exitCode = 50115
            self.state.saveJobReport()
            updateReport(self.toplevelReport, jobRep)
            return 1

        jobRep = self.state._JobReport

        if self.proxyLocation is None:
            msg = "Proxy file does not exist or could not be found." \
                  "\n ==> Unable to process analysis files for offline DQM\n"
            print msg
            error = jobRep.addError(60311, "DQMProxyError")
            error['Description'] = msg
            jobRep.status = "Failed"
            jobRep.exitCode = 60311
            self.state.saveJobReport()
            updateReport(self.toplevelReport, jobRep)
            return 1

        # Do not upload files if the job failed
        if not jobRep.wasSuccess():
            msg = "FrameworkJobReport says the job has failed."
            msg = "\n==> Not doing anything."
            return 1

        for aFile in jobRep.analysisFiles:
            try:
                self.impl(aFile)
            except Exception, ex:
                msg = " ==> Failure while processing analysis file %s" % aFile
                msg += "\n%s" % str(ex)
                print msg
                error = jobRep.addError(60311, "DQMUploadError")
                error['Description'] = msg
                jobRep.status = "Failed"
                jobRep.exitCode = 60311
                self.state.saveJobReport()
                updateReport(self.toplevelReport, jobRep)
                return 1
        return 0



if __name__ == '__main__':
    harvester = OfflineDQMHarvester()
    status = harvester()
    sys.exit(status)
