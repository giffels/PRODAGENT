{
'procname': 'MERGE'
, 'main_input': {
'@classname': ('string', 'tracked', 'PoolSource') }
 # end of main_input
, 'looper': {
} # end of looper
, 'psets': {
} #end of psets
, 'modules': {
#--------------------
'Merged': {'@classname': ('string', 'tracked', 'PoolOutputModule') }
} #end of modules
# es_modules
, 'es_modules': {
} #end of es_modules
# es_sources
, 'es_sources': {
} #end of es_sources
# es_prefers
, 'es_prefers': {
} #end of es_prefers
# output modules (names)
, 'output_modules': [ 'Merged' ]
# sequences
, 'sequences': { 
}
# paths
, 'paths': { 
}
# endpaths
, 'endpaths': { 
'outpath' : 'Merged'
}
# services
, 'services': {
'MessageLogger': {'@classname':('string', 'tracked', 'MessageLogger')
, 'fwkJobReports': ('vstring', 'untracked', ['"FrameworkJobReport.xml"']) }
, 'AdaptorConfig': {'@classname': ('string', 'tracked', 'AdaptorConfig')}
} #end of es_modules
# schedule
, 'schedule': 'outpath'
}
