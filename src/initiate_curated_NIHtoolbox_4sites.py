# this program is more of a history of all work that has already been done by hand (Cindy) and in python
# Basically, Cindy's files were fixed of known ID issues (e.g. UMN duplicate IDs per protocol violations and others) (python),
# them concatenated with new stuff (post 12/31/2018) from the endpoint machine
# before the machine went down early July, 2019 and sites had to start creating local backups.
# This will initiate the 8 site/study specific files of curated
# information for Box, so that RAs can begin fixing their own data as issues become apparent. The 8 files together will represent the 'curated' database of Toolbox data until a better solution can be designed that
# can handle the scale of data at hand and the need for protocol expertise when fixing rows.
# New data should be sent to these folders as individual files for me to automatically append?
#
# There are two parts to this program, corresponding to the two types of data that are being curated.  The first is the simple scores data...
# the second is the so-called Raw data.  Raw data is the item level data captured on the Ipad, from which scores are generated.  They are exported as separate files.
# Both are slight misnomers in the data cleaning sense because when they come from the endpoint machine, they are both untouched (e.g. raw).  After this program both types of data could be considered Curated (opposite of raw).
# for clarity sake, this program will only use Raw to mean data that is used to generate a given score.
# the steps below were already run - for example the cleanest data already has the FirstDate4Pin field
# They will be repeated here (even though there is no new data) so that
# everything from start to finish is in one place.


# previously (just before endpoint machine went offline) I rsynced all the 2019 endpoint data to cache, and then to my local machine for catting to Cindy's latest via some version of this command
# rsync petra@nrg-toolbox.nrg.mir:/var/www_nih_app_endpoint/html/projects/lifespan/*2019* /data/intradb/tmp/box2nda_cache/endpointmachine/lifespan.
# cache no longer exists, though, and cant get at the endpoint machine, (nrg-toolbox.nrg.mir) so to reproduce
# the programs I already ran to create what is currently stored under BDAS as of 7/24/2019, I have to rsync it back from PC
# e.g. from dev machine... mkdir /data/intradb/tmp/box2nda_cache/endpointmachine/lifespan
# and from local machine ...scp -r /home/petra/endpointmachine/lifespan/*
# hcpinternal@hcpi-dev-petra1.nrg.mir:/data/intradb/tmp/box2nda_cache/endpointmachine/lifespan/.

# the cache now contains everything we might need to cat to cindy's work...
# use python to cat only those with 2019 in their filenames because these represent correctly rescored data .
# Note: can't cat with unix...columns not the same for all files
# I.e. NO -- cat endpointmachine/lifespan/*Assessment\ Scores.csv_* | grep
# -v PIN > endpointmachine/AssessmentScores.csv

import datetime
##########################################################################
####initiate data that is required for both scores and raw data types#####
import os

import pandas as pd

from download import redcap
from download.box import LifespanBox

verbose = True
snapshotdate = datetime.datetime.today().strftime('%m_%d_%Y')
root_cache = '/data/intradb/tmp/box2nda_cache/'
# dont delete cache at the end of this program until endpoint machine is
# back up and running
cache_space = os.path.join(root_cache, 'endpointmachine/lifespan')
box = LifespanBox(cache=cache_space)

root_store = '/home/shared/HCP/hcpinternal/ccf-nda-behavioral/store/'
# this will be the place to save any snapshots on the nrg servers
store_space = os.path.join(root_store, 'toolbox')
try:
    os.mkdir(store_space)  # look for store space before creating it here
except BaseException:
    print("store already exists")


# prep basic redcap data ####################3
# need this so sites and studies can be assigned to curated data rows (if missing).
# Data cant be split by sites unless this info included somehow.
moredata = redcap.getfullredcapdata()
# set these vars to missing because they belong to child of parent and could conflict
# (different QC pgrograms look for this type of inconsistency between REDCap Databases)
moredata.loc[moredata.study == 'hcpdparent', 'gender'] = ''
moredata.loc[moredata.study == 'hcpdparent', 'dob'] = ''
# moredata has duplicate records where parent in redcap more than once for diff children
# just keep first of this record and note that interview date is first v1
# date observed in data
moredata = moredata.drop_duplicates(subset='subject')
moredata = moredata.rename(columns={'interview_date': 'v1_interview_date'})
moredata = moredata.drop(columns=['dob', 'subject_id', 'parent_id'])
# simplify study names
moredata.loc[moredata.study == 'hcpa', 'studyslim'] = 'HCA'
moredata.loc[moredata.study == 'hcpd18', 'studyslim'] = 'HCD'
moredata.loc[moredata.study == 'hcpdchild', 'studyslim'] = 'HCD'
moredata.loc[moredata.study == 'hcpdparent', 'studyslim'] = 'HCD'
moredata = moredata.drop(columns='study')
moredata = moredata.rename(columns={'studyslim': 'study'})

# get list of files in the endpoint machine to separate out by type
# note that this dir has been whittled down to the files with 2019 in
# their file name to keep it smaller - rsync again and get everything
files2cat = pd.DataFrame(os.listdir(cache_space), columns=['fname'])


# NOW Assemble Complete Raw and Scores datasets ##########################
##########################################################################
# BEGIN with Scores Data
# STEP 1: get cleanest data so far and merge with Redcap to get site and
# study info.  Replace UMN ids, where applicable, and correct any other
# known (historical) issues.
cleanest_scores = 476857277893
scores_path = box.downloadFile(cleanest_scores)
cleanestscores = pd.read_csv(
    scores_path,
    header=0,
    low_memory=False,
    encoding='ISO-8859-1')
# Extend first occurrence of date finished for a given PIN (if needed)
# to records without date, then pull in redcap vars and merge with catscoresnew
cleanestscores = extend_date(
    cleanestscores.drop(
        columns='FirstDate4PIN'),
    'DateFinished').copy()

# replace the bad UMN ids before merging with redcap - otherwise wont find
# a match
replacements = pd.read_csv(
    os.path.join(
        store_space,
        'UMN_Subject_ID_replacement_list.csv'),
    header=0)
IDmap = dict(zip(list(replacements.Old_ID), list(replacements.New_ID)))
for (oldid, newid) in IDmap.items():
    cleanestscores.loc[cleanestscores.subject == oldid, 'subject'] = newid

# replace ids for parent HCD ids with HCA id where applicable - to be consistent - these are parent about self batteries otherwise they would have been entered under childs ID
# change study to 'both' for easy flagging
# careful as there are some ids in the duplicates file that are duplicate hcds i.e. parent entered as two different hcdids for each child
# newid contains the id that is used in REDCAP.  In this case Both is changed to 'HCD', below
# may need to revisit this issue to align with redcap where such
# duplicates were merged into one id.
replacements2 = pd.read_csv(
    os.path.join(
        store_space,
        'HCD_to_HCA_multipleIDs.csv'),
    header=0)
cleanestscoresbak = cleanestscores.copy()
cleanestscores = cleanestscoresbak.copy()

IDmap2 = dict(zip(list(replacements2.Old_ID), list(replacements2.New_ID)))
count = 0
for (oldid, newid) in IDmap2.items():
    cleanestscores.loc[cleanestscores.subject == newid, 'study'] = 'BOTH'
    cleanestscores.loc[cleanestscores.subject == oldid, 'subject'] = newid
    if 'HCD' in newid:
        count = count + 1
        # two folks for whom the multiple IDs was not an HCA to HCD issue
        cleanestscores.loc[cleanestscores.subject == newid, 'study'] = 'HCD'

print(count)  # should be 2

# also remove deletions and fix typosfrom last round of QC.
replacements3 = pd.read_csv(
    os.path.join(
        store_space,
        'HCA_HCD_whoami_06_27_2019_fixed.csv'),
    header=0)
droplist = replacements3.loc[replacements3.delete == 1, 'subject']
df = pd.merge(
    cleanestscores,
    droplist,
    on=['subject'],
    how="outer",
    indicator=True)
cleanestscores = df.loc[df['_merge'] == 'left_only'].copy()
keeplist = replacements3.loc[replacements3.replaceid == 1]
IDmap3 = dict(zip(list(keeplist.subject), list(keeplist.correction)))
for (oldid, newid) in IDmap3.items():
    cleanestscores.loc[cleanestscores.subject == oldid, 'subject'] = newid

# looks good now get the redcapvars (remove the ones that were already there as they should
# always represent the latest redcap data for this id.
slimcurated = cleanestscores.drop(
    columns=[
        'flagged',
        'gender',
        'v1_interview_date',
        'site']).copy()
# drop study var from redcap data because dont want that level of info now
cleanestscoresmore = pd.merge(
    moredata.drop(
        columns='study'),
    slimcurated,
    how='right',
    on='subject')
# drop empty columns that were produced by excel during hand manipulations
cleanestscoresmore = cleanestscoresmore.drop(
    columns=[
        'Unnamed: 53',
        'Unnamed: 54',
        'Unnamed: 55',
        'Unnamed: 56',
        '_merge'])
# drop anything that has endpointmachine as its source -- endpoint machine
# data will be reincorporated in step2
cleanestscoresmore = cleanestscoresmore.loc[cleanestscoresmore.source.str.contains(
    "endpointmachine/") == False].copy()

# STEP2: get endpoint machine data, cat IT with output of step1
# get filenames with Scores in their titles
scores2cat = files2cat.loc[files2cat.fname.str.contains('Scores')]
# read and cat them in to dataframe
catscores = catfromlocal(cache_space, scores2cat)
# create subject variable
catscores['subject'] = catscores.PIN.str.split("_", expand=True)[0]

# Extend first occurrence of date finished for a given PIN - note that PINs are supposed to have visit number in them
# ...dates will extend beyond given visit if ID naming convention not followed - NEED TO DEAL WITH THIS VIA QC
# to records without date for later filtering
catscores = extend_date(catscores, 'DateFinished').copy()

# merge with basic redcap data so you can track down site, study, and v1 interview date
# this will only add records that have ID in REDcap
# to find missing IDs, need to look at catscores without the inner merge
# with moredata
catscoresmore = pd.merge(moredata, catscores, how='inner', on='subject')

# subset to data collected after 12/31/2018 (all sites except WU) and after 4/23/2019 for WU
# note that next time around, this can all be put together (all sites
# collected from same catfromdate)
subset4scoresWU = catscoresmore.loc[catscoresmore.site == '4']
subset4scoresWU = subset4scoresWU.loc[subset4scoresWU.FirstDate4PIN > '2019-04-22']

# still might be old records in here even though filtered local copy of
# directory by date in filename...need to filter out.
subset4scoresOthersites = catscoresmore.loc[~(catscoresmore.site == '4')]
subset4scoresOthersites = subset4scoresOthersites.loc[
    subset4scoresOthersites.FirstDate4PIN > '2018-12-31']

# put them together and rename a few variables in prep for catting with
# the cleanest box data
catscoresnew = pd.concat([subset4scoresWU, subset4scoresOthersites], axis=0)
catscoresnew = catscoresnew.rename(columns={'filename': 'source'})
# drop empty columns from old Taste Test batteries
catscoresnew = catscoresnew.drop(
    columns=[
        'Age-Corrected Standard Scores Quinine Whole',
        'Age-Corrected Standard Scores Salt Whole',
        'Uncorrected Standard Scores Quinine Whole',
        'Uncorrected Standard Scores Salt Whole',
        'Whole Mouth Quinine',
        'Whole Mouth Salt',
        'Fully-Corrected T-scores Quinine Whole',
        'Fully-Corrected T-scores Salt Whole'])


# STEP 3: merge the data from steps1 and steps2 and then split the result into site/subject csv files and upload to Box-site folders in box.
# merge with new data from endpoint
# confirm that columns are same because they should be the same
catscoresnew.columns.sort_values().values == cleanestscoresmore.columns.sort_values(
).values  # should all be True - otherwise dirty data...please investigate
curatedwithcat = pd.concat(
    [cleanestscoresmore, catscoresnew], axis=0, sort=False, verify_integrity=False)
# reorder columns
cols = curatedwithcat.columns.tolist()
cols = cols[-1:] + cols[:-1]
curatedwithcatreordered = curatedwithcat[cols]
# check source
# this QC start not fleshed out but will come in handy when looking for duplicate blocks of records...
# extrarecords=curatedwithcatreordered.groupby('source').count()
# extrarecords.FirstDate4PIN

# lights on
# get the ones that dont have site info (indicating they still couldnt be
# mapped to redcap for one reason or another.
whoami = pd.DataFrame(
    curatedwithcat.loc[curatedwithcat.site.isnull(), 'subject'].unique())  # 74 of these
whoami.to_csv(
    os.path.join(
        store_space,
        'Homeless_IDs_Toolbox_Scored_Combined_' +
        snapshotdate +
        '.csv'))
# to do:

# split by study and site - remember that hcd parents who are also hca are
# listed under BOTH for study and their hca ID.  Make sure filename
# reflects this.

HCAorBoth = curatedwithcatreordered.loc[~(
    curatedwithcatreordered.study == 'HCD')]  # NOT (notice ~) HCD data
# HCD data
HCDonly = curatedwithcatreordered.loc[curatedwithcatreordered.study == 'HCD']

WashU_HCAorBoth = HCAorBoth.loc[HCAorBoth.site == '4'].copy()
WashU_HCAorBoth_storefile = os.path.join(
    store_space,
    'WashU_HCAorBoth_Toolbox_Scored_Combined_' +
    snapshotdate +
    '.csv')
WashU_HCAorBoth.to_csv(WashU_HCAorBoth_storefile, index=False)
box.upload_file(WashU_HCAorBoth_storefile, 82804729845)

WashU_HCDonly = HCDonly.loc[HCDonly.site == '4'].copy()
WashU_HCDonly_storefile = os.path.join(
    store_space,
    'WashU_HCDonly_Toolbox_Scored_Combined_' +
    snapshotdate +
    '.csv')
WashU_HCDonly.to_csv(WashU_HCDonly_storefile, index=False)
box.upload_file(WashU_HCDonly_storefile, 82804015457)

UCLA_HCAorBoth = HCAorBoth.loc[HCAorBoth.site == '2'].copy()
UCLA_HCAorBoth_storefile = os.path.join(
    store_space,
    'UCLA_HCAorBoth_Toolbox_Scored_Combined_' +
    snapshotdate +
    '.csv')
UCLA_HCAorBoth.to_csv(UCLA_HCAorBoth_storefile, index=False)
box.upload_file(UCLA_HCAorBoth_storefile, 82807223120)

UCLA_HCDonly = HCDonly.loc[HCDonly.site == '2'].copy()
UCLA_HCDonly_storefile = os.path.join(
    store_space,
    'UCLA_HCDonly_Toolbox_Scored_Combined_' +
    snapshotdate +
    '.csv')
UCLA_HCDonly.to_csv(UCLA_HCDonly_storefile, index=False)
box.upload_file(UCLA_HCDonly_storefile, 82805124019)

UMN_HCAorBoth = HCAorBoth.loc[HCAorBoth.site == '3'].copy()
UMN_HCAorBoth_storefile = os.path.join(
    store_space,
    'UMN_HCAorBoth_Toolbox_Scored_Combined_' +
    snapshotdate +
    '.csv')
UMN_HCAorBoth.to_csv(UMN_HCAorBoth_storefile, index=False)
box.upload_file(UMN_HCAorBoth_storefile, 82803665867)

UMN_HCDonly = HCDonly.loc[HCDonly.site == '3'].copy()
UMN_HCDonly_storefile = os.path.join(
    store_space,
    'UMN_HCDonly_Toolbox_Scored_Combined_' +
    snapshotdate +
    '.csv')
UMN_HCDonly.to_csv(UMN_HCDonly_storefile, index=False)
box.upload_file(UMN_HCDonly_storefile, 82805151056)

MGH_HCAorBoth = HCAorBoth.loc[HCAorBoth.site == '1'].copy()
MGH_HCAorBoth_storefile = os.path.join(
    store_space,
    'MGH_HCAorBoth_Toolbox_Scored_Combined_' +
    snapshotdate +
    '.csv')
MGH_HCAorBoth.to_csv(MGH_HCAorBoth_storefile, index=False)
box.upload_file(MGH_HCAorBoth_storefile, 82761770877)

Harvard_HCDonly = HCDonly.loc[HCDonly.site == '1'].copy()
Harvard_HCDonly_storefile = os.path.join(
    store_space,
    'Harvard_HCDonly_Toolbox_Scored_Combined_' +
    snapshotdate +
    '.csv')
Harvard_HCDonly.to_csv(Harvard_HCDonly_storefile, index=False)
box.upload_file(Harvard_HCDonly_storefile, 82803734267)

#['WashU_HCAorBoth', 'WashU_HCD', 'UCLA_HCAorBoth', 'UCLA_HCD', 'UMN_HCAorBoth', 'UMN_HCD',   'MGH_HCAorBoth', 'Harvard_HCD']
#[82804729845,        82804015457, 82807223120,       82805124019,82803665867,   82805151056, 82761770877,      82803734267]


############################################
#############################################
# Repeat process for Raw Data
# STEP 1: get cleanest Raw data so far and merge with Redcap to get site and study info.  Replace UMN ids, where applicable, and correct any other known (historical) issues.
# call this raw data
cleanest_raw = 476857675439  # ..as of 7/24/2019.
data_path = box.downloadFile(cleanest_raw)
cleanestraw = pd.read_csv(
    data_path,
    header=0,
    low_memory=False,
    encoding='ISO-8859-1')
# no need to extend date because none missing, as in the scores file

# replace the bad UMN ids before merging with redcap - otherwise wont find
# a match
replacements = pd.read_csv(
    os.path.join(
        store_space,
        'UMN_Subject_ID_replacement_list.csv'),
    header=0)
IDmap = dict(zip(list(replacements.Old_ID), list(replacements.New_ID)))
for (oldid, newid) in IDmap.items():
    cleanestraw.loc[cleanestraw.subject == oldid, 'subject'] = newid

# replace ids for parent HCD ids with HCA id where applicable - to be consistent - these are parent about self batteries otherwise they would have been entered under childs ID
# change study to 'both' for easy flagging but careful as there are some ids in the duplicates file that are duplicate hcds i.e. parent entered as two different hcdids for each child
# newid contains the id that is used in REDCAP.  In this case Both is
# changed to 'HCD', below
replacements2 = pd.read_csv(
    os.path.join(
        store_space,
        'HCD_to_HCA_multipleIDs.csv'),
    header=0)
cleanestrawbak = cleanestraw.copy()
# cleanestraw=cleanestrawbak.copy()
IDmap2 = dict(zip(list(replacements2.Old_ID), list(replacements2.New_ID)))
count = 0
for (oldid, newid) in IDmap2.items():
    cleanestraw.loc[cleanestraw.subject == newid, 'study'] = 'BOTH'
    cleanestraw.loc[cleanestraw.subject == oldid, 'subject'] = newid
    if 'HCD' in newid:
        count = count + 1
        # two folks for whom the multiple IDs was not an HCA to HCD issue
        cleanestraw.loc[cleanestraw.subject == newid, 'study'] = 'HCD'

print(count)  # should be 2

# also remove deletions and fix typosfrom last round of QC.
replacements3 = pd.read_csv(
    os.path.join(
        store_space,
        'HCA_HCD_whoami_06_27_2019_fixed.csv'),
    header=0)
droplist = replacements3.loc[replacements3.delete == 1, 'subject']
df = pd.merge(
    cleanestraw,
    droplist,
    on=['subject'],
    how="outer",
    indicator=True)
cleanestraw = df.loc[df['_merge'] == 'left_only'].copy()
keeplist = replacements3.loc[replacements3.replaceid == 1]
IDmap3 = dict(zip(list(keeplist.subject), list(keeplist.correction)))
for (oldid, newid) in IDmap3.items():
    cleanestraw.loc[cleanestraw.subject == oldid, 'subject'] = newid


# looks good now get the redcapvars (remove the ones that were already there as they should
# always represent the latest redcap data for this id.
slimcurated = cleanestraw.copy()
# slimcurated=cleanestraw.drop(columns=['flagged','gender','v1_interview_date','site']).copy()
# drop study var from redcap data because dont want that level of info now
cleanestrawmore = pd.merge(
    moredata.drop(
        columns='study'),
    slimcurated,
    how='right',
    on='subject')
# drop empty columns that were produced by excel during hand manipulations
cleanestrawmore = cleanestrawmore.drop(columns=['_merge'])
# drop anything that has endpointmachine as its source -- endpoint machine
# data will be reincorporated in step2
cleanestrawmore = cleanestrawmore.loc[cleanestrawmore.Source.str.contains(
    "endpointmachine/") == False].copy()

##################################
# STEP2: get endpoint machine data, cat IT with output of step1
# get filenames with Data in their titles and read files into dataframe
rawdata2cat = files2cat.loc[files2cat.fname.str.contains(
    'Assessment') & ~(files2cat.fname.str.contains('Scores'))]
catrawdata = catfromlocal(cache_space, rawdata2cat)
catrawdatabak = catrawdata.copy()
catrawdata['subject'] = catrawdata.PIN.str.split("_", expand=True)[0]

# no need to extend first occurrence of data...all necessary dates are included except 14 rows of empty export.
# merge with basic redcap data so you can track down site, study, and v1 interview date
# this will only add records that have ID in REDcap
# to find missing IDs, need to look at catraw without the inner merge with
# moredata
catrawmore = pd.merge(moredata, catrawdata, how='inner', on='subject')

# subset to data collected after 12/31/2018 (all sites except WU) and after 4/23/2019 for WU
# note that next time around, this can all be put together (all sites
# collected from same catfromdate)
subset4rawWU = catrawmore.loc[catrawmore.site == '4']
subset4rawWU = subset4rawWU.loc[subset4rawWU.DateCreated > '2019-04-22']

# still might be old records in here even though filtered local copy of
# directory by date in filename...need to filter out.
subset4rawOthersites = catrawmore.loc[~(catrawmore.site == '4')]
subset4rawOthersites = subset4rawOthersites.loc[subset4rawOthersites.DateCreated > '2018-12-31']

# put them together and rename a few variables in prep for catting with
# the cleanest box data
catrawnew = pd.concat([subset4rawWU, subset4rawOthersites], axis=0)
catrawnew = catrawnew.rename(columns={'filename': 'source'})

#########################################
# STEP 3: : merge the data from steps1 and steps2 and then split the result into site/subject csv files and upload to Box-site folders in box.
# merge with new data from endpoint
# after renaming a couple of things, confirm that columns are same because
# they should be the same
cleanestrawmoretest = cleanestrawmore.rename(columns={'Source': 'source'})

catrawnew.columns.sort_values().values == cleanestrawmore.columns.sort_values(
).values  # should all be True - otherwise dirty data...please investigate
curatedwithcat = pd.concat(
    [cleanestrawmore, catrawnew], axis=0, sort=False, verify_integrity=False)
# reorder columns
cols = curatedwithcat.columns.tolist()
cols = cols[-1:] + cols[:-1]
curatedwithcatreorderedraw = curatedwithcat[cols]
# check source
# this QC start not fleshed out but will come in handy when looking for duplicate blocks of records...
# extrarecords=curatedwithcatreordered.groupby('source').count()
# extrarecords.FirstDate4PIN
# split by study and site - remember that hcd parents who are also hca are
# listed under BOTH for study and their hca ID.  Make sure filename
# reflects this.

RawHCAorBoth = curatedwithcatreorderedraw.loc[~(
    curatedwithcatreorderedraw.study == 'HCD')]  # NOT (notice ~) HCD data
# HCD data
RawHCDonly = curatedwithcatreorderedraw.loc[curatedwithcatreorderedraw.study == 'HCD']

WashU_HCAorBoth = RawHCAorBoth.loc[RawHCAorBoth.site == '4'].copy()
WashU_HCAorBoth_storefile = os.path.join(
    store_space,
    'WashU_HCAorBoth_Toolbox_Raw_Combined_' +
    snapshotdate +
    '.csv')
WashU_HCAorBoth.to_csv(WashU_HCAorBoth_storefile, index=False)
box.upload_file(WashU_HCAorBoth_storefile, 82804729845)

WashU_HCDonly = RawHCDonly.loc[RawHCDonly.site == '4'].copy()
WashU_HCDonly_storefile = os.path.join(
    store_space,
    'WashU_HCDonly_Toolbox_Raw_Combined_' +
    snapshotdate +
    '.csv')
WashU_HCDonly.to_csv(WashU_HCDonly_storefile, index=False)
box.upload_file(WashU_HCDonly_storefile, 82804015457)

UCLA_HCAorBoth = RawHCAorBoth.loc[RawHCAorBoth.site == '2'].copy()
UCLA_HCAorBoth_storefile = os.path.join(
    store_space,
    'UCLA_HCAorBoth_Toolbox_Raw_Combined_' +
    snapshotdate +
    '.csv')
UCLA_HCAorBoth.to_csv(UCLA_HCAorBoth_storefile, index=False)
box.upload_file(UCLA_HCAorBoth_storefile, 82807223120)

UCLA_HCDonly = RawHCDonly.loc[RawHCDonly.site == '2'].copy()
UCLA_HCDonly_storefile = os.path.join(
    store_space,
    'UCLA_HCDonly_Toolbox_Raw_Combined_' +
    snapshotdate +
    '.csv')
UCLA_HCDonly.to_csv(UCLA_HCDonly_storefile, index=False)
box.upload_file(UCLA_HCDonly_storefile, 82805124019)

UMN_HCAorBoth = RawHCAorBoth.loc[RawHCAorBoth.site == '3'].copy()
UMN_HCAorBoth_storefile = os.path.join(
    store_space,
    'UMN_HCAorBoth_Toolbox_Raw_Combined_' +
    snapshotdate +
    '.csv')
UMN_HCAorBoth.to_csv(UMN_HCAorBoth_storefile, index=False)
box.upload_file(UMN_HCAorBoth_storefile, 82803665867)

UMN_HCDonly = RawHCDonly.loc[RawHCDonly.site == '3'].copy()
UMN_HCDonly_storefile = os.path.join(
    store_space,
    'UMN_HCDonly_Toolbox_Raw_Combined_' +
    snapshotdate +
    '.csv')
UMN_HCDonly.to_csv(UMN_HCDonly_storefile, index=False)
box.upload_file(UMN_HCDonly_storefile, 82805151056)

MGH_HCAorBoth = RawHCAorBoth.loc[RawHCAorBoth.site == '1'].copy()
MGH_HCAorBoth_storefile = os.path.join(
    store_space,
    'MGH_HCAorBoth_Toolbox_Raw_Combined_' +
    snapshotdate +
    '.csv')
MGH_HCAorBoth.to_csv(MGH_HCAorBoth_storefile, index=False)
box.upload_file(MGH_HCAorBoth_storefile, 82761770877)

Harvard_HCDonly = RawHCDonly.loc[RawHCDonly.site == '1'].copy()
Harvard_HCDonly_storefile = os.path.join(
    store_space,
    'Harvard_HCDonly_Toolbox_Raw_Combined_' +
    snapshotdate +
    '.csv')
Harvard_HCDonly.to_csv(Harvard_HCDonly_storefile, index=False)
box.upload_file(Harvard_HCDonly_storefile, 82803734267)

#['WashU_HCAorBoth', 'WashU_HCD', 'UCLA_HCAorBoth', 'UCLA_HCD', 'UMN_HCAorBoth', 'UMN_HCD',   'MGH_HCAorBoth', 'Harvard_HCD']
#[82804729845,        82804015457, 82807223120,       82805124019,82803665867,   82805151056, 82761770877,      82803734267]


def catfromlocal(endpoint_temp, scores2cat):  # dataframe that has filenames
    scoresfiles = scores2cat.copy()
    scoresinit = pd.DataFrame()
    for i in scoresfiles.fname:
        filepath = os.path.join(endpoint_temp, i)
        try:
            temp = pd.read_csv(filepath, header=0, low_memory=False)
            temp['filename'] = "endpointmachine/" + i
            temp['raw_cat_date'] = snapshotdate
            scoresinit = pd.concat([scoresinit, temp], axis=0, sort=False)
        except BaseException:
            print(filepath + ' wouldnt import')
            temp = pd.DataFrame()
            temp['filename'] = pd.Series("endpointmachine/" + i, index=[0])
            temp['raw_cat_date'] = snapshotdate
            scoresinit = pd.concat([scoresinit, temp], axis=0, sort=False)
    return scoresinit


def extend_date(df, datevar):  # dataframe to be extended, and name of datavar on which to extend
    try:
        dates = df.loc[df[datevar].isnull() == False]
        dates = dates.drop_duplicates(subset='PIN', keep='first')[
            ['PIN', datevar]]
        dates = dates.rename(columns={datevar: 'FirstDate4PIN'})
        dates['FirstDate4PIN'] = pd.to_datetime(dates['FirstDate4PIN'])
        dfnew = pd.merge(df, dates, how='left', on='PIN')
        return dfnew
    except BaseException:
        print('No missing dates')
        return df
