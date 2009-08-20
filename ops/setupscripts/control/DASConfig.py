from WMCore.Configuration import Configuration
from os import environ

config = Configuration()
config.component_('Webtools')
config.Webtools.application = 'Tier0Monitoring'
config.Webtools.port = 8888
config.Webtools.host = 'whatever.cern.ch'

config.component_('Tier0Monitoring')
config.Tier0Monitoring.templates = environ['WTBASE'] + '/templates/WMCore/WebTools'
config.Tier0Monitoring.admin = 'president@whitehouse.gov'
config.Tier0Monitoring.title = 'TEST Tier0 Monitoring'
config.Tier0Monitoring.description = 'Monitoring of a Tier0 instance'
config.Tier0Monitoring.section_('views')

# These are all the active pages that Root.py should instantiate
active = config.Tier0Monitoring.views.section_('active')
tier0 = active.section_('tier0')

# The class to load for this view/page
tier0.object = 'WMCore.WebTools.RESTApi'
tier0.templates = environ['WTBASE'] + '/templates/WMCore/WebTools/'
tier0.database = 'oracle://account:passwd@tnsname'
tier0.section_('model')
tier0.model.object = 'T0.DAS.Tier0RESTModel'
tier0.section_('formatter')
tier0.formatter.object = 'WMCore.WebTools.DASRESTFormatter'


