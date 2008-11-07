#!/usr/bin/env python

import os
from ProdAgentCore.Configuration import loadProdAgentConfiguration
from ProdAgentCore.DaemonDetails import DaemonDetails


class ComponentStatus:

    def __init__(self, compLog = None, postMorten = None) :
        self.complog = compLog
        self.postmorten = postMorten

    def index(self):
        config = loadProdAgentConfiguration()
        components = config.listComponents()
        html = """<head>\n"""
        html += "<meta http-equiv=\"refresh\" content=\"60\" />\n\n"
        html += """</head>"""
        html += """<html><body><h2>ProdAgent Component Status</h2>\n """
        html += "<table>\n"
        html += "<tr><th>Component</th><th>Status</th><th>Logs</th><th>PostMortens</th></tr>\n"
        for component in components :
            componentDir = config.getConfig(component)['ComponentDir']
            componentDir = os.path.expandvars(componentDir)
            daemonXML = os.path.join(componentDir, "Daemon.xml")
            daemon = DaemonDetails(daemonXML)
            if not daemon.isAlive():
                html += "<tr><td>%s</td><td><font color=\"FF0000\">Not Running</font></td>" % component
            else :
                html += "<tr><td>%s</td><td>PID : %s</td>" % (component, daemon['ProcessID'])
            html += "<td><a href=\"%s?componentName=%s\">Show</a></td>\n" % (self.complog, component)
            html += "<td><a href=\"%s?componentName=%s\">Show</a></td></tr>\n" % (self.postmorten, component)
        html += "</table>\n"
        html += """</body></html>"""
        return html
    index.exposed = True



class PostMorten:

    def __init__(self, writeLog = None) :
        self.writelog = writeLog

    def index(self, componentName) :
        config = loadProdAgentConfiguration()
        componentDir = config.getConfig(componentName)['ComponentDir']
        componentDir = os.path.expandvars(componentDir)
        postMortens = []
        for file in os.listdir(componentDir) :
            if file.find('PostMortem') > -1 :
                postMortens.append(file)
        postMortens.sort()
        html = """<html><body><h2>List of Available PostMortens for Component %s </h2>\n """ % componentName
        if len(postMortens) == 0 :
            html = """<html><body><h2>No Available PostMortens for Component %s! </h2>\n """ % componentName
        else :
            html += "<ul>\n"
            for postMorten in postMortens:
                postMortenFile = os.path.join(componentName,postMorten)
                html += "<li><a href=\"%s?logFile=%s\">%s</a></li>\n" % (
                    self.writelog, postMortenFile, postMorten)
            html += "</ul>\n"
        html += """</body></html>"""
        return html
    index.exposed = True 



class ComponentLogs :

    def __init__(self, writeLog = None) :
        self.writelog = writeLog

    def index(self, componentName) :
        config = loadProdAgentConfiguration()
        componentDir = config.getConfig(componentName)['ComponentDir']
        componentDir = os.path.expandvars(componentDir)
        logs = []
        for file in os.listdir(componentDir) :
            if file.find('ComponentLog') > -1 :
                logs.append(file)
        logs.sort()
        html = """<html><body><h2>List of Available Logs for Component %s </h2>\n """ % componentName
        if len(logs) == 0 :
            html = """<html><body><h2>No Available Logs for Component %s! </h2>\n """ % componentName
        else :
            html += "<ul>\n"
            for log in logs:
                logFile = os.path.join(componentName,log) 
                html += "<li><a href=\"%s?logFile=%s\">%s</a></li>\n" % (
                    self.writelog, logFile, log)
            html += "</ul>\n"
        html += """</body></html>"""
        return html
    index.exposed = True 


class WriteLog :
    
    def index(self,logFile) :
        config = loadProdAgentConfiguration()
        componentName = logFile.split("/")[0].strip()
        componentDir = config.getConfig(componentName)['ComponentDir']
        componentDir = os.path.expandvars(componentDir)
        file = logFile.split("/")[1].strip()
        html = """<html><body><h2>%s for %s </h2>\n """ % (file, componentName)
        componentLog = open(componentDir+'/'+file).read().replace('\n','<br>')
        html += componentLog
        html += """</body></html>"""
        return html
    index.exposed = True

