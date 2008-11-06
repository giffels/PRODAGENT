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
from mimetypes import guess_type
from gzip      import GzipFile
from cStringIO import StringIO
from md5       import md5
import traceback
import httplib
import urllib2

from ProdCommon.FwkJobRep.TaskState import TaskState
from ProdCommon.MCPayloads.UUID import makeUUID
from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec

from StageOut.StageOutMgr import StageOutMgr
from StageOut.StageOutError import StageOutInitError
import StageOut.Impl

_DoHTTPPost = True
_DoStageOut = True
_HTTPPostURL = 'https://vocms33.cern.ch/dqm/dev' #test instance
#_HTTPPostURL = 'https://cmsweb.cern.ch/dqm/tier-0/data/put' # prod instance
#_HTTPPostURL = 'https://cmsweb.cern.ch/dqm/relval/data/put' # RelVal instance


__revision__ = "$Id: RuntimeOfflineDQM.py,v 1.12 2008/11/05 22:11:14 direyes Exp $"
__version__ = "$Revision: 1.12 $"


HTTPS = httplib.HTTPS
if sys.version_info[:3] >= (2, 4, 0):
    HTTPS = httplib.HTTPSConnection


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
        self.mssNames = {}
        self.doStageOut = _DoStageOut
        self.doHttpPost = _DoHTTPPost


    def __call__(self, aFile):
        """
        _operator(analysisFile)_

        Do the upload for the file provided

        """
        if aFile['FileName'].startswith("./"):
            aFile['FileName'] = aFile['FileName'].replace("./", "")

        msg = "==========Handling Analysis File=========\n"
        for key, value in aFile.items():
            msg += " => %s: %s\n" % (key, value)
        print msg
        try:
            self.stageOut(aFile)
        except Exception, ex:
            msg = "Failure processing stage out:\n"
            msg += "For File: %s\n" % aFile['FileName']
            msg += "%s\n" % str(ex)
            print msg
            return 1
        if self.doHttpPost:
            try:
                self.httpPost(aFile)
            except Exception, ex:
                msg = "Failure processing Http Post:\n"
                msg += "For File: %s\n" % aFile['FileName']
                msg += "%s\n" % str(ex)
                print msg
                return 2
        return 0


    def stageOut(self, analysisFile):
        """
        _stageOut_

        stage out the DQM Histogram to local storage

        """
        filename = analysisFile['FileName']
        try:
            stager = StageOutMgr()
        except Exception, ex:
            msg = "Unable to stage out log archive:\n"
            msg += str(ex)
            raise RuntimeError, msg

        filebasename = os.path.basename(filename)
        filebasename = filebasename.replace(".root", "")

        fileInfo = {
            'LFN' : "/store/unmerged/dqm/%s/%s/%s" % (self.workflowSpecId,
                                                      self.jobSpecId,
                                                      filebasename),
            'PFN' : os.path.join(os.getcwd(), filename),
            'SEName' : None,
            'GUID' : filebasename,
            }
        if self.doStageOut:
            try:
                stager(**fileInfo)
            except Exception, ex:
                msg = "Unable to stage out DQM File:\n"
                msg += str(ex)
                raise RuntimeError, msg
        else:
            msg = "Stage Out is disabled"
            print msg

        storagePFN = stager.searchTFC(fileInfo['LFN'])
        self.mssNames[filename] = storagePFN
        analysisFile['StoragePFN'] = storagePFN
        return


    def httpPost(self, analysisFile):
        """
        _httpPost_

        perform an HTTP POST operation to a webserver

        """
        filename = analysisFile['FileName']

        args = {}
        args['producer'] = 'ProdSys'
        args['step'] = 'Pass-1'
        args['url'] = self.uploadUrl
        args['workflow'] = self.inputDataset
        args['mssname'] = self.mssNames[filename]

        msg = "HTTP Upload of file commencing with args:\n"
        msg += " => Filename: %s\n" % filename
        for key, val in args.items():
            msg += " => %s: %s\n" % (key, val)
        print msg

        try:
            (headers, data) = self.upload(args, filename)
            print 'Status code: ', headers.get("Dqm-Status-Code", "None")
            print 'Message:     ', headers.get("Dqm-Status-Message", "None")
            print 'Detail:      ', headers.get("Dqm-Status-Detail", "None")
            print data
        except urllib2.HTTPError, e:
            print 'Automated upload of %s failed' % filename
            print "ERROR", e
            print 'Status code: ', e.hdrs.get("Dqm-Status-Code", "None")
            print 'Message:     ', e.hdrs.get("Dqm-Status-Message", "None")
            print 'Detail:      ', e.hdrs.get("Dqm-Status-Detail", "None")
        except Exception, ex:
            print 'Automated upload of %s failed' % filename
            print 'problem unknown'
            print ex


    def encode(self, args, files):
        """
        Encode form (name, value) and (name, filename, type) elements into
        multi-part/form-data. We don't actually need to know what we are
        uploading here, so just claim it's all text/plain.
        """
        boundary = '----------=_DQM_FILE_BOUNDARY_=-----------'
        (body, crlf) = ('', '\r\n')
        for (key, value) in args.items():
            body += '--' + boundary + crlf
            body += ('Content-disposition: form-data; name="%s"' % key) + crlf
            body += crlf + str(value) + crlf
        for (key, filename) in files.items():
            filetype = guess_type(filename)[0] or 'application/octet-stream'
            body += '--' + boundary + crlf
            body += ('Content-Disposition: form-data; name="%s"; filename="%s"'
                     % (key, os.path.basename(filename))) + crlf
            body += ('Content-Type: %s' % filetype) + crlf
            body += crlf + open(filename, "r").read() + crlf
        body += '--' + boundary + '--' + crlf + crlf
        return ('multipart/form-data; boundary=' + boundary, body)


    def upload(self, args, file):
        """
        _upload_

        Perform a file upload to the dqm server using HTTPS auth with the service proxy provided

        """
        #preparing a checksum
        blockSize = 0x10000
        def upd(m, data):
            m.update(data)
            return m
        fd = open(file, 'rb')
        try:
            contents = iter(lambda: fd.read(blockSize), '')
            m = reduce(upd, contents,md5())
        finally:
            fd.close()

        args['checksum'] = 'md5:' + m.hexdigest()
        args['size']     = str(os.stat(file)[6])
        proxyLoc = self.proxyLocation

        class HTTPSCertAuth(HTTPS):
            def __init__(self, host):
                HTTPS.__init__(self, host,
                               key_file = proxyLoc,
                               cert_file = proxyLoc)

        class HTTPSCertAuthenticate(urllib2.AbstractHTTPHandler):
            def default_open(self, req):
                return self.do_open(HTTPSCertAuth, req)

        #
        # HTTPS see : http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/VisMonitoring/DQMServer/scripts/visDQMUpload?r1=1.1&r2=1.2
        #
        #
        # Add user identification, ProdAgent or something like that
        # Eg: ProdAgent python version, this modules __version__ attr
        #

        ident = "ProdAgent Python %s.%s.%s %s" % (sys.version_info[0] ,
                                                  sys.version_info[1] ,
                                                  sys.version_info[2] ,
                                                  __version__)

        authreq = urllib2.Request(args['url'] + '/digest')
        authreq.add_header('User-agent', ident)
        result = urllib2.build_opener(HTTPSCertAuthenticate()).open(authreq)
        cookie = result.headers.get('Set-Cookie')
        if not cookie:
            msg = "Unable to authenticate to DQM Server:\n"
            msg += "%s\n" % self.args['url']
            msg += "With Proxy from:\n"
            msg += "%s\n" % self.proxyLocation
            print msg
            raise RuntimeError, msg
        cookie = cookie.split(";")[0]


        # open a connection and upload the file
        url = args.pop('url') + "/data/put"
        request = urllib2.Request(url)
        (type, body) = self.encode(args, {'file': file})
        request.add_header('Accept-encoding', 'gzip')
        request.add_header('User-agent', ident)
        request.add_header('Cookie', cookie)
        request.add_header('Content-type',    type)
        request.add_header('Content-length',  str(len(body)))
        request.add_data(body)
        result = urllib2.build_opener().open(request)
        data   = result.read()
        if result.headers.get('Content-encoding', '') == 'gzip':
            data = GzipFile(fileobj=StringIO(data)).read()
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

        try:
            self.state.loadJobReport()
        except Exception, ex:
            print "Error Reading JobReport:"
            print str(ex)
            self.state._JobReport = None

        self.state.loadJobSpecNode()

        self.uploadUrl = _HTTPPostURL
        workflow = WorkflowSpec()
        workflow.load(os.environ['PRODAGENT_WORKFLOW_SPEC'])
        if workflow.parameters.has_key("DQMServer"):
            self.uploadUrl = workflow.parameters['DQMServer']

        #  //
        # // Lookup proxy, first from workflow for explicit path
        #//  Fallback to X509_PROXY as defined in grid jobs
        self.proxyLocation = workflow.parameters.get('proxyLocation', None)
        possibleProxyVars = [
            "X509_PROXY",
            "X509_USER_PROXY",
            ]
        if self.proxyLocation == None:
            for ppv in possibleProxyVars:
                value = os.environ.get(ppv, None)
                if value == None:
                    continue
                if not os.path.exists(value):
                    continue
                self.proxyLocation = value
                break
        if self.proxyLocation == None:
            msg = "===PROXY FAIL===\n"
            msg += "Unable to find proxy for HTTPS Upload\n"
            msg += "Cannot determine location of proxy"
            raise RuntimeError, msg

        if not os.path.exists(self.proxyLocation):
            msg = "===PROXY FAIL===\n"
            msg += "Proxy file does not exist:\n"
            msg += "%\n" % self.proxyLocation
            msg += "Cannot proceed with HTTPS Upload without proxy\n"
            raise RuntimeError, msg




        jobSpecNode = self.state.jobSpecNode
        inputDataset = jobSpecNode._InputDatasets[0]

        self.impl = HarvesterImpl()
        self.impl.inputDataset = inputDataset.name()
        self.impl.proxyLocation = self.proxyLocation
        self.impl.uploadUrl = self.uploadUrl
        self.impl.workflowSpecId = self.workflowSpecId
        self.impl.jobSpecId = self.jobSpecId

        # map local to storage PFN
        #self.mssNames = {}


    def __call__(self):
        """
        _operator()_

        Invoke this object to find files and do stage out

        """
        if self.state._JobReport == None:
            msg = "No Job Report available\n"
            msg += "Unable to process analysis files for offline DQM\n"
            print msg
            return 1

        jobRep = self.state._JobReport

        for aFile in jobRep.analysisFiles:
            self.impl(aFile)
        return 0



if __name__ == '__main__':


    harvester = OfflineDQMHarvester()
    status = harvester()

    sys.exit(status)
