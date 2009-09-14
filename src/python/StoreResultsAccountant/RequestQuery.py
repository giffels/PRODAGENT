#!/usr/bin/env python
"""
RequestQuery

Provides an interface between the StoreResultsAccountant
and the Savannah Request Interface. Responsible for querying
the Request Interface, creation of the JSON steering file and
providing information for the bookeeping database

"""
import os
import re
import logging

from mechanize import Browser
import simplejson as json

class RequestQuery:

    def __init__(self,config):
        self.br=Browser()

        self.config = config

        self.isLoggedIn = self.login2Savannah()

    def closeRequest(self,task):
        if self.isLoggedIn:
            self.selectQueryForm()

            for link in self.br.links(text_regex="#[0-9]+"):
            ## remove leading &nbsp and # from name
                id = link.text.replace('#','').decode('utf-8').strip()

                if int(id)==int(task):
                    response = self.br.follow_link(link)

                    self.br.select_form(name="item_form")
                    
                    control = self.br.find_control("status_id",type="select")
                    TicketStatusByLabelDict = self.getValueByLabelDict(control)
                
                    control.value = [TicketStatusByLabelDict["Closed"]]

                    self.br.submit()
                    return
                
    def createValueDicts(self):       
        if self.isLoggedIn:
            self.br.select_form(name="bug_form")
            
            control = self.br.find_control("custom_sb1",type="select")
            self.SiteByValueDict = self.getLabelByValueDict(control)
            control = self.br.find_control("custom_sb2",type="select")
            self.ReleaseByValueDict = self.getLabelByValueDict(control)
            control = self.br.find_control("custom_sb3",type="select")
            self.GroupByValueDict = self.getLabelByValueDict(control)
            control = self.br.find_control("resolution_id",type="select")
            self.StatusByValueDict = self.getLabelByValueDict(control)
            
    def getLabelByValueDict(self, control):
        d = {}
        for item in control.items:
            value = item.attrs['value']
            label = item.attrs['label']
            d[value] = label
                
        return d

    def getRequests(self):
        requests = []
        
        if self.isLoggedIn:
            self.selectQueryForm()
            self.createValueDicts()
        
            self.br.select_form(name="bug_form")
            response = self.br.submit()

            for link in self.br.links(text_regex="#[0-9]+"):
                response = self.br.follow_link(link)
    
                ## Get Information
                self.br.select_form(name="item_form")

                ## Get old dataset name
                control = self.br.find_control("custom_tf1",type="text")
                old_dataset = control.value.split('/')

                ## Get new dataset name
                control = self.br.find_control("custom_tf2",type="text")
                new_dataset = control.value.split('/')

                ## Get DBS URL
                control = self.br.find_control("custom_tf4",type="text")
                dbs_url = control.value

                ## Get Site
                control = self.br.find_control("custom_sb1",type="select")
                site_id =  control.value

                ## Get Release
                control = self.br.find_control("custom_sb2",type="select")
                release_id = control.value

                ## Get Physics Group
                control = self.br.find_control("custom_sb3",type="select")
                group_id = control.value
                group_squad = 'cms-storeresults-'+self.GroupByValueDict[group_id[0]].replace("-","_")
    
                ## Get current status
                control = self.br.find_control("resolution_id",type="select")
                status_id = control.value

                ## Get assigned to
                control = self.br.find_control("assigned_to",type="select")
                AssignedToByValueDict = self.getLabelByValueDict(control)
                assignedTo_id = control.value

                ##Assign task to the physics group squad
                if AssignedToByValueDict[assignedTo_id[0]]!=group_squad:
                    control.value = [self.getValueByLabelDict(control)[group_squad]]
                    self.br.submit()
    
                self.br.back()

                ## remove leading &nbsp and # from name
                task = link.text.replace('#','').decode('utf-8').strip()

                infoDict = {}
                
                infoDict["primaryDataset"] = old_dataset[1]
                infoDict["processedDataset"] = old_dataset[2]
                infoDict["outputDataset"] = new_dataset[2]
                infoDict["physicsGroup"] = self.GroupByValueDict[group_id[0]]
                infoDict["inputDBSURL"] = dbs_url
                infoDict["cmsswRelease"] = self.ReleaseByValueDict[release_id[0]]
                infoDict["destinationSite"] = self.SiteByValueDict[site_id[0]]
                            
                ##Fill json file, if status is done
                if self.StatusByValueDict[status_id[0]]=='Done':
                    self.writeJSONFile('Ticket_'+task+'.json', infoDict)

                ##Not part of the bookeeping database
                del infoDict["destinationSite"]

                infoDict["task"] = int(task)
                infoDict["status"] = self.StatusByValueDict[status_id[0]]
                infoDict["assignedTo"] = AssignedToByValueDict[assignedTo_id[0]]
                
                requests.append(infoDict)
    
            return requests


    def getValueByLabelDict(self, control):
        d = {}
        for item in control.items:
            value = item.attrs['value']
            label = item.attrs['label']
            d[label] = value

        return d

    def login2Savannah(self):
        login_page='https://savannah.cern.ch/account/login.php?uri=%2F'
        savannah_page='https://savannah.cern.ch/task/?group=cms-storeresults'
        
        self.br.open(login_page)

        ## 'Search' form is form 0
        ## login form is form 1
        self.br.select_form(nr=1)

        username = self.config["SavannahUser"]
    
        self.br['form_loginname']=username
        self.br['form_pw']=self.config["SavannahPasswd"]
        
        self.br.submit()
        
        response = self.br.open(savannah_page)
        
        # Check to see if login was successful
        if not re.search('Logged in as ' + username, response.read()):
            logging.error('login unsuccessful, please check your username and password')
            return False
        else:
            return True

    def selectQueryForm(self):       
        if self.isLoggedIn:
            self.br.select_form(name="bug_form")

            ## Use right query form labelled Test
            control = self.br.find_control("report_id",type="select")

            for item in control.items:
                if item.attrs['label'] == "Test":
                    control.value = [item.attrs['value']]
            self.br.submit()
     
    def writeJSONFile(self, name, infoDict):
        ##check if file already exists
        filename = self.config["ComponentDir"]+'/'+name.strip()
        if not os.access(filename,os.F_OK):
            jsonfile = open(filename,'w')
            jsonfile.write(json.dumps(infoDict,sort_keys=True, indent=4))
            jsonfile.close
