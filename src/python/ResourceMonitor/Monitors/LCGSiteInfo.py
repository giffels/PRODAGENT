#!/usr/bin/env python

"""
_LCGInfo_

"""

"""

Tool to collect site information.
Sources include BDII, FCR.
Still need to be added: SAM

Also contain tools to produce necessary information for the ResourceControlDB 

What is expected in the RCDB:
--> site: LCG BDII name
--> ce-name: CE gatekeeper 
"""

import re, os
from subprocess import Popen,PIPE
from urllib import urlopen
from xml.dom.minidom import parse                                                            
import urllib2
import logging
from ProdCommon.SiteDB import SiteDB

"""
Collect site info
"""

"""
FCR parsing
"""

def getFcr(urls):
    in_fcr_cms=[]

    ## FCR ldif regexp
    reg_fcr_cms=re.compile(r"dn\s*:\s*GlueCEUniqueID=(?P<queue>.*?),.*?,mds-vo-name=local,o=grid\s*\n\s*changetype\s*:\s*modify\s*\n\s*delete\s*:\s*GlueCEAccessControlBaseRule\s*\n\s*GlueCEAccessControlBaseRule:\s*VO\s*:\s*cms")

    for fcr_url in urls:
        try:
            f=urlopen(fcr_url).read()
        except:
            ## if something goes wrong, assume empty list
            f=''

        for i in reg_fcr_cms.finditer(f):
            #print i.group('queue'),i.group('site')
            in_fcr_cms.append(i.group('queue'))

    return in_fcr_cms


def samCheck(sites, url):
    """
    check status of last sam tests for sites
        Returns list of ce's with failures
    """

    if url == None:
      return sites

    ces = [x.split(':')[0] for x in sites.keys()]
    cms_sites = set()
    for ce in ces:
        details = SiteDB.getJSON("CEtoCMSName", name=ce)
        if details.has_key('0') and details['0'].has_key('name'):
            cms_sites.add(details['0']['name'])

    # dashboard url takes test id's not names
    #TODO: move to config file at some point
    # 6 = CE-cms-mc
    # 133 = CE-cms-basic
    tests = ["133", "6"]

    #only return info for failed ce's
    query_url = url + "?services=CE&exitStatus=error&siteSelect3=All%20Sites&serviceTypeSelect3=vo"
    query_url += "&sites=" + "&sites=".join(cms_sites)
    query_url += "&tests=" + "&tests=".join(tests)    

    logging.debug("sam query using url: %s" % query_url)

    try:
        logging.debug("Starting sam check")

        request = urllib2.Request(query_url)
        #request xml data
        request.add_header('Accept', 'text/xml')
        opener = urllib2.build_opener()
        results = parse(opener.open(request))

        # each site under item tag (under data)
        data = results.getElementsByTagName('data')[0].getElementsByTagName('item')
        for sitedata in data:

            # check if any sites names - if not skip
            if (len(sitedata.getElementsByTagName('SamName')) == 0):
                continue

            site = sitedata.getElementsByTagName('SamName')[0].firstChild.nodeValue
            logging.debug("checking site %s" % site)

            # now loop over ce's
            ces = sitedata.getElementsByTagName('ServiceNames')
            for ce in ces:
                try:

                    if ce.getElementsByTagName('ServiceName') == None:
                        continue;

                    cename = ce.getElementsByTagName('ServiceName')[0].firstChild.nodeValue
                    logging.error("ce = %s" % cename)

                    #go through tests
                    tests = ce.getElementsByTagName('Tests')[0]

                    # each test is a seperate item tag
                    items = tests.getElementsByTagName('item')
                    for test in items:

                        # now ignore age - this is status of last test so 
                        # on error put in failure mode
                        # --- old --- only trust if got last result
                        #if test.getElementsByTagName("Age")[0].firstChild == None or \
                        #       test.getElementsByTagName("Age")[0].firstChild.nodeValue != "0":
                        #    continue

                        statusNode = test.getElementsByTagName("Status")[0].firstChild
                        #statusNode not filled for non-error
                        #TODO: Change to fail only on error - look up syntax
                        if statusNode and statusNode.nodeValue != "ok":
                            for ce, details in sites.values():
                                if ce.split(':')[0] == cename:
                                    details['SAMfail'] = True
                except Exception, ex:
                    logging.error("prob with sam query of %s: %s" % (site, str(ex)))
                    # on errror just ignore ce


    except Exception, ex:
        logging.error("Error on sam query: %s" % str(ex))

    return sites


"""
LDAP parsing using ldapsearch. Assumes ldapsearch is available
"""

def getLdap(base,attrs,ldaphost,ret_dn=False):
    all_res=[]

    cmd_base='ldapsearch -LLL -x -H ldap://'+ldaphost

    #TODO: Does not work with this - why?
    #command=cmd_base+' -b "'+base+'" \'(GlueCEAccessControlBaseRule=VO:cms)\' '+' '.join(attrs)
    command=cmd_base+' -b "'+base+'" '+' '.join(attrs)

    FNULL = open('/dev/null', 'w')
    try:
        data=Popen(command,stdout=PIPE,stderr=FNULL,shell=True).communicate()[0]    
    except:
        ## we don't really care
        data=''
        logging.error("Error contacting ldap - got: %s" % data)
    FNULL.close()

    ## reparse the ldapsearch data: any '\n \S' sequence means actually '\S'
    ## see http://www.openldap.org/lists/openldap-software/200303/msg00509.html
    reg_fix_ldapout=re.compile(r"\n (\S)")
    data=reg_fix_ldapout.sub(r"\1",data)

    ## split different answers
    reg_split=re.compile(r"\n\s*\n")
    blocks=reg_split.split(data)

    for attr in attrs:
        res=[]
        for b in blocks:
            ## get the block with source info
            reg_block=re.compile(r"^\s*dn\s*:\s*(?P<dn>.*?)$(?P<real_data>.*?^\s*"+attr+"\s*:.*)\Z",re.M | re.S)
            blockres=reg_block.search(b)
            source = None
            tmp_res=[]
            if blockres:
                source=blockres.group('dn')
                reg_attr=re.compile(r"^\s*"+attr+"\s*:\s*(?P<value>\S.*?)\s*$",re.M)

                for val in reg_attr.finditer(blockres.group('real_data')):
                    tmp_res.append(val.group('value'))

                if ret_dn:
                    if source:
                        res.append([source,tmp_res])
                else:
                    res=res+tmp_res

        all_res.append(res)

    return all_res

"""
Search the necessary info from BDII
"""
def getBdii(names,ldaphost):
    the_list={}

    reg_cmssoft=re.compile("^VO-cms-(.*)$")
    reg_acbr_cms=re.compile("VO\s*:\s*cms\s*$")

    reg_mdsvoname=re.compile(r"mds-vo-name=(?P<site>.*)")

    for site in names:
        try:
            site_base='mds-vo-name='+site+',mds-vo-name=local,o=grid'
            [clusters]=getLdap(site_base,['GlueClusterUniqueID'],ldaphost,True)
            for source,[cluster] in clusters:
                ## to fix GRIF, we need to redefine the site base
                tmparr=source.split(',')
                tmparr.pop(0)
                site_base=','.join(tmparr)
                ## look for additional sitename
                tmp_sitename=''
                if reg_mdsvoname.search(tmparr[0]):
                    tmp_sitename=reg_mdsvoname.search(tmparr[0]).group('site')
                    if tmp_sitename == site:
                        tmp_sitename=''
                    else:
                        ## use a whitespace character. it forbidden in BDII names. 
                        ## split(' ')[0] should give the original name 
                        tmp_sitename=' '+tmp_sitename

                cluster_base='GlueClusterUniqueID='+cluster+','+site_base
                subclusters,ceUIDs=getLdap(cluster_base,['GlueSubClusterUniqueID','GlueClusterService'],ldaphost)

                cmssoft=[]
                for subcluster in subclusters:
                    subcluster_base='GlueSubClusterUniqueID='+subcluster+','+cluster_base
                    [softs]=getLdap(subcluster_base,['GlueHostApplicationSoftwareRunTimeEnvironment'],ldaphost)
                    for soft in softs:
                        if reg_cmssoft.search(soft):
                            cmssw=reg_cmssoft.search(soft).group(1)
                            if not cmssw in cmssoft:
                                cmssoft.append(cmssw)

                for ceuid in ceUIDs:
                    ceuid_base='GlueCEUniqueID='+ceuid+','+site_base
                    info=['GlueCEInfoTotalCPUs','GlueCEStateFreeCPUs','GlueCEStateStatus','GlueCEPolicyMaxCPUTime','GlueCEAccessControlBaseRule']
                    max_slots,free,state,maxct,ACBRs=getLdap(ceuid_base,info,ldaphost)

                    ## ACBR: VO:cms
                    support_cms=False
                    for acbr in ACBRs:
                        if reg_acbr_cms.search(acbr):
                            support_cms=True

                            ## some more info
                            localvo_ceuid_base='GlueVOViewLocalID=cms,'+ceuid_base
                            #TODO: restrict this to only cmsprod jobs (what about other cmsprod users?)\
                            info=['GlueCEStateEstimatedResponseTime','GlueCEStateRunningJobs','GlueCEStateTotalJobs','GlueCEStateWaitingJobs']
                            estRT,runn,tot,wait=getLdap(localvo_ceuid_base,info,ldaphost)
                            if support_cms:
                                #print ceuid
                                ceseuid_base='GlueCESEBindGroupCEUniqueID='+ceuid+','+site_base
                                [ses]=getLdap(ceseuid_base,['GlueCESEBindGroupSEUniqueID'],ldaphost)
                                #print ses

                                the_list[ceuid]={'site_name':site+tmp_sitename,
                                                    'SEs':ses,
                                                    'estimated_response_time':estRT[0],
                                                    'free_slots':free[0],
                                                    'max_slots':max_slots[0],
                                                    'jobs_running':runn[0],
                                                    'jobs_total':tot[0],
                                                    'jobs_waiting':wait[0],
                                                    'state':state[0],
                                                    'max_cpu_time':maxct[0],
                                                    'in_fcr':False,
                                                    'software':cmssoft,
                                                    'SAMfail' : False
                                                    }
        except Exception, e:
            logging.error("Error with site %s bdii query:\t%s" % (site, str(e)))


    return the_list


'''

Tools to generate the RCDB entries.

'''

'''
Parse the Phedex DBParam file
'''

def getPhedexConnect(paramfile,section):
    all_sections=file(paramfile).read()

    """
    remove commented lines
    """
    reg_comment=re.compile(r'^\s*#.*$',re.M)
    all_sections=reg_comment.sub("",all_sections)

    """
    if there are multiple sections, we expect a newline between them
    """
    reg_sec=re.compile(r"(Section\s+(\S+).*?)((\n\s*\n)|$)",re.S)
    reg_field=re.compile(r"^\s*(\S+)\s+(\S+)$",re.M)

    dbp={}
    for sec in reg_sec.finditer(all_sections):
        if sec.group(2) == section:
            ## make dictionary with all entries
            for line in reg_field.finditer(sec.group(1)):
                dbp[line.group(1)]=line.group(2)

    conn_param=['AuthDBUsername','AuthRolePassword','Database']
    missing_param=False
    for par in conn_param:
        if not dbp.has_key(par):
            missing_param=True
    if missing_param:
        return None
    else:
        connect_string=dbp['AuthDBUsername']+'/'+dbp['AuthDBPassword']+'@'+dbp['Database']
        return connect_string

'''
Make a Phedex_node name -> SE map
'''
def getPhedexSEmap(paramfile,section):
    ma={}
    conn=getPhedexConnect(paramfile,section)
    if not conn:
        return ma

    import cx_Oracle
    con = cx_Oracle.connect(conn)
    cur=con.cursor()

    sql="""
        select node.name node_name, node.se_name
        from t_adm_node node
        """
    #print sql
    cur.prepare(sql)
    cur.execute(sql)
    res=cur.fetchall()
    for r in res:
        ma[r[0]]=r[1]

    #print ma
    return ma

def getSiteInfoFromBase(names,urls,ldaphost,dbp=None, samUrl=None):
    """
    Collect all site info starting from LCG BDII Site names. Returns dictionary.
    """
    
    use_phed=True
    try:
        if not dbp:
            from ProdAgentCore.Configuration import loadProdAgentConfiguration
            PAcfg = loadProdAgentConfiguration()


            PhedCfg = PAcfg['PhEDExConfig']
            dbp = PhedCfg['DBPARAM']
        DBParamFile,Section = dbp.split(':')
        if not os.path.isfile(DBParamFile):
            logging.error("Configured DBParamFile %s not found" % DBParamFile)
            use_phed=False
    except:
        print "Something went wrong with the Phedex config. Not using TMDB."
        use_phed=False
    
    #strip of text after "#", and remove site duplicates 
    bdiiNames = list(set([ x.split("#")[0] for x in names]))
    tmp_list=getBdii(bdiiNames,ldaphost)
    
    #fcr info
    ceuids_in_fcr=getFcr(urls)
    for ceuid in tmp_list.keys():
        if ceuid in ceuids_in_fcr:
            tmp_list[ceuid]['in_fcr']=True

    #sam test info
    tmp_list = samCheck(tmp_list, samUrl)

    if use_phed:
        Phmap=getPhedexSEmap(DBParamFile,Section)

        for ceuid in tmp_list.keys():
            se_in_phedex=None
            for se in tmp_list[ceuid]['SEs']:
                if se in Phmap.values():
                    ## this can happen more than once,
                    ## but should not cause problems                
                    se_in_phedex=se
            if se_in_phedex:
                tmp_list[ceuid]['SEs']=[se]
            else:
                print "None of the published SEs can be found in Phedex."
                print " This is probably a bug somewhere."
                print "Currnet SEs: %s, CEUID: %s" % (
                    str(tmp_list[ceuid]['SEs']),ceuid)

    return tmp_list
