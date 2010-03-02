cnaf()
{
    version='CMSSW_3_3_5_patch4'
    site='storm-fe-cms.cr.cnaf.infn.it'
    dataset='/MinimumBias/BeamCommissioning09-v1/RAW'
    cfg="/afs/cern.ch/user/c/cmst1/public/cfgs/rereco_FirstCollisions_33x.py"
}


fnal()
{
    site='cmssrm.fnal.gov'
    version='CMSSW_3_3_5_patch4'
    dataset='/ZeroBias/BeamCommissioning09-v1/RAW'
    cfg="/afs/cern.ch/user/c/cmst1/public/cfgs/rereco_FirstCollisions_33x.py"
    #onlyblocks='/MinimumBias/BeamCommissioning09-v1/RAW#98a86729-082e-4166-88f0-fbe20e7e5f53,/MinimumBias/BeamCommissioning09-v1/RAW#ce6e58f5-05a6-4758-abc0-15382a1b431a,/MinimumBias/BeamCommissioning09-v1/RAW#6d5ccf33-5bd0-4fed-85e8-4bfad549aa14,/MinimumBias/BeamCommissioning09-v1/RAW#76523809-a4c6-4b8f-a5a7-9e780139a4ac,/MinimumBias/BeamCommissioning09-v1/RAW#94ceb0ae-36e2-4946-abee-f791168ae4f4,/MinimumBias/BeamCommissioning09-v1/RAW#7fe69db7-a651-46b6-91ed-d4edb2fa6d98,/MinimumBias/BeamCommissioning09-v1/RAW#fa572324-06e8-4dcd-be65-537e7d6e895b,/MinimumBias/BeamCommissioning09-v1/RAW#061a9c17-b164-41a2-b642-e35816feb645,/MinimumBias/BeamCommissioning09-v1/RAW#b59ed338-79fc-4d5c-a597-70e1a3dfe0d3,/MinimumBias/BeamCommissioning09-v1/RAW#dfa13f2f-252d-471e-941f-3686aaf1a986,/MinimumBias/BeamCommissioning09-v1/RAW#572ac860-5556-4fdf-9ac4-d0e33aee7c1e,/MinimumBias/BeamCommissioning09-v1/RAW#18ec0796-5607-4d43-a4fb-47335435dfe8,/MinimumBias/BeamCommissioning09-v1/RAW#a765b608-a076-424d-808e-28100687cbed,/MinimumBias/BeamCommissioning09-v1/RAW#34659acb-6138-4eec-a7f7-c0cbb21b7b19,/MinimumBias/BeamCommissioning09-v1/RAW#b77c5138-1656-481b-ab2c-7b511c1fd4c2,/MinimumBias/BeamCommissioning09-v1/RAW#5a3ccc00-1b9d-44ad-ae1a-c8cdaebd6836,/MinimumBias/BeamCommissioning09-v1/RAW#6cc3a294-abaf-448a-873e-1311c4acbd83,/MinimumBias/BeamCommissioning09-v1/RAW#771bd10a-4350-4c8e-856e-ca43e4d80eb9,/MinimumBias/BeamCommissioning09-v1/RAW#1e1e7d38-5e03-4772-b8ab-f788ee8cfa6e,/MinimumBias/BeamCommissioning09-v1/RAW#cc6c84f6-8724-41dc-a1f8-e1efed1afedf,/MinimumBias/BeamCommissioning09-v1/RAW#74d226bf-05d7-4884-8f86-947788779074,/MinimumBias/BeamCommissioning09-v1/RAW#7dcdcd8d-09d0-47e5-95ff-2fb0a8b80035,/MinimumBias/BeamCommissioning09-v1/RAW#91f83df5-b51f-4ea8-9396-af4eedcd8586,/MinimumBias/BeamCommissioning09-v1/RAW#24b70515-5ee3-4b17-a08f-e9440b1578af,/MinimumBias/BeamCommissioning09-v1/RAW#d5c56078-3152-412c-b7f2-d48e1d889fec,/MinimumBias/BeamCommissioning09-v1/RAW#286e5399-05e4-4818-98a8-8243f795c3a4,/MinimumBias/BeamCommissioning09-v1/RAW#6f194912-b32f-4c00-9b18-cac1ed7c1974,/MinimumBias/BeamCommissioning09-v1/RAW#b3c1dd26-3551-4b93-b586-922cfda205c6,/MinimumBias/BeamCommissioning09-v1/RAW#8e28c0c6-926b-4d74-b925-b456ccb0e08e,/MinimumBias/BeamCommissioning09-v1/RAW#9eca9b43-7f7f-4cb4-85fd-322ce8aefe2e,/MinimumBias/BeamCommissioning09-v1/RAW#8a548ea4-669e-49cc-bdc0-a5d093fe4a22,/MinimumBias/BeamCommissioning09-v1/RAW#81a9e501-85fb-42f6-ab5d-3d3f813ea21a,/MinimumBias/BeamCommissioning09-v1/RAW#1f583c4c-caaf-4396-9472-8685f36e4a4a,/MinimumBias/BeamCommissioning09-v1/RAW#601810bf-d5fa-4ec4-b607-0d1124482042,/MinimumBias/BeamCommissioning09-v1/RAW#a69575d2-a254-44ee-81bb-9d5827e8e78d,/MinimumBias/BeamCommissioning09-v1/RAW#48c22939-c425-42fb-8d9a-77f8064404d9,/MinimumBias/BeamCommissioning09-v1/RAW#6b1a571c-e0d4-47a5-9f30-e8cd77d8d43b,/MinimumBias/BeamCommissioning09-v1/RAW#08aef87c-6bd5-4690-b137-ffc4dc62cafb,/MinimumBias/BeamCommissioning09-v1/RAW#30c85e0a-93c2-4461-8c0b-6d469019ca9a,/MinimumBias/BeamCommissioning09-v1/RAW#9c98a0a0-0732-4963-9a3d-d160b9fff962,/MinimumBias/BeamCommissioning09-v1/RAW#bf62b853-6a63-4f7f-aeb6-9c3f9c43c606,/MinimumBias/BeamCommissioning09-v1/RAW#f43283f3-d3ea-47bd-9b3d-5298e5fb1048,/MinimumBias/BeamCommissioning09-v1/RAW#5929e226-fc18-4e1a-9668-2e76840fb933,/MinimumBias/BeamCommissioning09-v1/RAW#76ae8ee6-17be-4297-bb48-333ce1198511,/MinimumBias/BeamCommissioning09-v1/RAW#597859e0-1d21-4d63-a443-c3af04a2e59b,/MinimumBias/BeamCommissioning09-v1/RAW#2f4f6deb-fa24-4cdb-9d1f-bd70327fcf2f,/MinimumBias/BeamCommissioning09-v1/RAW#481fcdb9-386b-4a58-a16e-9df954095018,/MinimumBias/BeamCommissioning09-v1/RAW#bf72a35f-069b-46cb-86ad-9348b377da37,/MinimumBias/BeamCommissioning09-v1/RAW#7684326d-ab90-4f4e-851d-11688a1e8c60,/MinimumBias/BeamCommissioning09-v1/RAW#3c947ec3-a6d6-4bca-847d-735acc6649da,/MinimumBias/BeamCommissioning09-v1/RAW#feb4c718-62cd-4a70-adae-6158737b5174,/MinimumBias/BeamCommissioning09-v1/RAW#735f33c3-74a9-43ec-8505-8adb5601a398,/MinimumBias/BeamCommissioning09-v1/RAW#2ce10d62-9f05-4de0-8f69-fc6e14db2bed,/MinimumBias/BeamCommissioning09-v1/RAW#5bb1622c-63da-4c10-9f98-7654fda7ad13,/MinimumBias/BeamCommissioning09-v1/RAW#146a2ff9-3a36-4f1a-988f-5d67a00e15e3,/MinimumBias/BeamCommissioning09-v1/RAW#7cbd1247-16e3-4b81-b1a1-b7e6d56f00ed,/MinimumBias/BeamCommissioning09-v1/RAW#00d33465-1439-46fe-aa3b-d1705b6c4e78,/MinimumBias/BeamCommissioning09-v1/RAW#ad6688bb-9588-424a-a342-09cfa14d98fd'
}

in2p3()
{
    version='CMSSW_3_3_5_patch4'
    site='ccsrm.in2p3.fr'
    dataset='/MinimumBias/BeamCommissioning09-v1/RAW'
    cfg='/afs/cern.ch/user/c/cmst1/public/cfgs/rereco_FirstCollisions_33x.py'
}

ral()
{
    version='CMSSW_3_3_5_patch4'
    site='srm-cms.gridpp.rl.ac.uk'
    dataset='/Cosmics/BeamCommissioning09-v1/RAW'
    cfg='/afs/cern.ch/user/c/cmst1/public/cfgs/rereco_FirstCollisions_33x.py'
}


asgc()
{
    version='CMSSW_3_3_5_patch4'
    site='srm2.grid.sinica.edu.tw'
    dataset='/Cosmics/Commissioning08-CRUZET4_v1/RAW'
    cfg='/afs/cern.ch/user/c/cmst1/public/cfgs/rereco_FirstCollisions_33x.py'
}


kit()
{
    version='CMSSW_3_3_5_patch4'
    site='gridka-dCache.fzk.de'
    dataset='/ZeroBiasB/BeamCommissioning09-v1/RAW'
    cfg='/afs/cern.ch/user/c/cmst1/public/cfgs/rereco_FirstCollisions_33x.py'
}

pic()
{
    version='CMSSW_3_3_5_patch4'
    site='srmcms.pic.es'
    dataset='/MinimumBias/BeamCommissioning09-v1/RAW'
    cfg='/afs/cern.ch/user/c/cmst1/public/cfgs/rereco_FirstCollisions_33x.py'
}
