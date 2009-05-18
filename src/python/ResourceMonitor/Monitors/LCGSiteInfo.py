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
from ProdCommon.SiteDB.CmsSiteMapper import CECmsMap

"""
Collect site info
"""


def getVOMSfqan():
    """
    Get voms fqan string
      Some sites only allow certain roles to run there,
        need to find which role we have and use that in bdii queries
    """
    # default that matches plain cms access rules
    result = 'VO\s*:\s*cms\s*$'
    reg_search_fqan = re.compile('^\/cms\/Role=(\w*)\/')
    
    command = 'voms-proxy-info -fqan'
    try:
        out, err = Popen(command,stdout=PIPE,stderr=PIPE,shell=True).communicate()
    except:
        raise RuntimeError, 'Error with voms fqan: %s, %s' % (out,err)
    try:
        role = reg_search_fqan.search(out.split('\n')[0])
        if role:
            role = role.group(1)
            if role != 'NULL':
                # new rule match voms rule *or* the default cms access
                result="("+result+')|('+'VOMS\s*:\s*\/cms\/Role='+role+'\s*$'+')'
    except Exception, e:
        raise RuntimeError, 'Error with voms fqan: %s' % str(e)
    return re.compile(result)
            

"""
FCR parsing
"""

def getFcr(fcr_url, sites):
    in_fcr_cms=[]

    ## FCR ldif regexp
    reg_fcr_cms=re.compile(r"dn\s*:\s*GlueCEUniqueID=(?P<queue>.*?),.*?,mds-vo-name=local,o=grid\s*\n\s*changetype\s*:\s*modify\s*\n\s*delete\s*:\s*GlueCEAccessControlBaseRule\s*\n\s*GlueCEAccessControlBaseRule:\s*VO\s*:\s*cms")

    try:
        f=urlopen(fcr_url).read()
    except:
        ## if something goes wrong, assume empty list
        f=''

    for i in reg_fcr_cms.finditer(f):
        in_fcr_cms.append(i.group('queue'))
        for site in sites:
            for ce, ceinfo in site.items():
                if ce == i.group('queue'):
                    ceinfo['in_fcr'] = True
    return sites


def samCheck(sites, url):
    """
    check status of last sam tests for sites
        Returns list of ce's with failures
    """

    if url == None:
      return sites

    site_mapper = CECmsMap()
    cms_sites = []
    for site in sites.values():
        cms_sites.extend([site_mapper[ce.split(":")[0]] for ce in site])
#        set([site_mapper[ce.split(':')[0]] for ce in site for site in sites ])
    cms_sites = set(cms_sites)

    # dashboard url takes test id's not names
    #TODO: move to config file at some point
    # 6 = CE-cms-mc
    # 133 = CE-cms-basic
    tests = ("133", "6")

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

            try:

                # check if any sites names - if not skip
                if (len(sitedata.getElementsByTagName('SamName')) == 0):
                    continue
    
                site = sitedata.getElementsByTagName('SamName')[0].firstChild.nodeValue
                logging.debug("checking site %s" % site)
    
                # now loop over ce's
                ces = sitedata.getElementsByTagName('ServiceNames')
                for ce in ces:

                    if ce.getElementsByTagName('ServiceName') == None:
                        continue;

                    cename = ce.getElementsByTagName('ServiceName')[0].firstChild.nodeValue

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
                        if statusNode and statusNode.nodeValue not in ("ok", "info", "warning"):
                            for ce, details in sites.values():
                                if ce.split(':')[0] == cename:
                                    details['SAMfail'] = True
                                    #details['SAMfail'].append(ce)
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
def getBdii(sites, ldaphost):
    the_list={}

    reg_cmssoft=re.compile("^VO-cms-(.*)$")
    reg_acbr_cms = getVOMSfqan()

    reg_mdsvoname=re.compile(r"mds-vo-name=(?P<site>.*)")

    #
    # TODO: Change to going from SE to CE - ignore CE's not in GlueCESEBindGroupCEUniqueID
    #
    # Loop over clusters, find closeSE, filter for given se
    #    only add if one ce close to se - return standard stuff but keyed off site name not ce
    #         return list of sites
    for site in sites:
        try:
            name = site['SiteName']
            se = site['SEName']
            ce = site['CEName']
        
            site_base='mds-vo-name='+name+',mds-vo-name=local,o=grid'
            [clusters]=getLdap(site_base,['GlueClusterUniqueID'],ldaphost,True)
            
            if not clusters:
                logging.warning("No clusters for site %s found in information system." % name)
            
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

                if ce and ce not in ceUIDs:
                    continue

                #
                # TODO: Drop acbr check - confusion between prod and non prod role - leave to operator???
                #
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
                                
                                # CE must be listed as close to the specified SE
                                if se not in ses:
                                    logging.info("Ignore CE %s - not close to SE %s at %s" % (ce, se, name))
                                    support_cms = False
                                
                    the_list.setdefault(name, {})
                    #if the site has a CE specified we only care about that one
                    if support_cms:
                        if ce and ce != ceuid:
                            continue

                        #the_list[ceuid]={'site_name':site+tmp_sitename,
                        the_list[name][ceuid] = {'CE' : ceuid,
                                    'SE': se,
                                    'estimated_response_time':estRT[0],
                                    'free_slots':free[0],
                                    'max_slots':max_slots[0],
                                    'jobs_running':runn[0],
                                    'jobs_total':tot[0],
                                    'jobs_waiting':wait[0],
                                    'state':state[0],
                                    'max_cpu_time':maxct[0],
                                    'in_fcr': False,
                                    'software':cmssoft,
                                    'SAMfail' : False
                                    }

            if ce and ce not in the_list.get(name, {}):
                    logging.warning("Specified CE %s not in info system (or you don't have the necessary role) for site %s." % (ce, name))
                    continue

        except Exception, e:
            logging.error("Error with site %s bdii query:\t%s" % (name, str(e)))


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

def getSiteInfoFromBase(sites, urls, ldaphost, dbp=None, samUrl=None):
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
    #bdiiNames = list(set([x['SiteName'].split("#")[0] for x in sites]))
    
    tmp_list = getBdii(sites, ldaphost)
    #tmp_list=getBdii(bdiiNames,ldaphost)

    #fcr info
    #ceuids_in_fcr=getFcr(urls)
#    for ceuid in tmp_list.keys():
#        if ceuid in ceuids_in_fcr:
#            tmp_list[ceuid]['in_fcr']=True      
    #tmp_list = [x for x in tmp_list if x['CEName'] in ceuids_in_fcr]
    tmp_list = getFcr(urls, tmp_list)

    #sam test info
    tmp_list = samCheck(tmp_list, samUrl)

#    if use_phed:
#        #TODO: Out of date, Do we want to track links here -
#        #        esp if we are going to auto-create subscriptions
#        Phmap=getPhedexSEmap(DBParamFile,Section)
#
#        for site in tmp_list:
#            #se_in_phedex=None
#            for se in site.values(): #['SEs'][:]: #tmp_list[ceuid]['SEs']:
#                se = se['SE']
#                if not se in Phmap.values():
#                    ## this can happen more than once,
#                    ## but should not cause problems                
#                    #se_in_phedex=se
#                    site['SEs'].remove(se)
#            else:
#                print "None of the published SEs can be found in Phedex."
#                print " This is probably a bug somewhere."
#                print "Currnet SEs: %s, CEUID: %s" % (
#                    str(tmp_list[ceuid]['SEs']),ceuid)

    return tmp_list
