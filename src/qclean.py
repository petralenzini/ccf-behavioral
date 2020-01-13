#!/usr/bin/env python3

import datetime
import os
import shutil
import numpy as np
import pandas as pd
from download.box import LifespanBox
from download.redcap import Redcap
from config import config

redcap = Redcap()

verbose = True
snapshotdate = datetime.datetime.today().strftime('%m_%d_%Y')

columnnames = config['QIntColumns']
cache_space = config['dirs']['cache']['qint']
store_space = config['dirs']['store']['qint']

# connect to Box
box = LifespanBox(cache=cache_space)

# snapshot folder (used to be the combined folder)
q_snapshotfolderid = 48203213208
snapshotQCfolder = 76434619813
cleanestdata = 465568117756

# %%

baseclean = pd.read_excel(box.readFile(cleanestdata))
basecleanexcluded = baseclean.loc[baseclean.source == 'perm-missing']
baseclean = baseclean.loc[baseclean.select_4clean == 1]
baseclean.row = baseclean.row.str.replace('-', '')

asslist = baseclean.groupby('assessment').count()
asslist.reset_index(inplace=True)

# %%

db = {}
db2 = {}
snap = {}

# %%

# read row as dataframe and assign varnames from stored var list file
for item in asslist.assessment:
    db[item] = baseclean.loc[baseclean.assessment == item]
    db[item].reset_index(inplace=True, drop=True)

    db2[item] = pd.DataFrame(
        db[item].row.str.split(',').tolist(),
        columns=columnnames[item]
    )

    # fix matrix completion and delay completion times
    fieldname = 'delay_completion' if item in ['RAVLT', 'RAVLT2'] else 'matrix_completion'
    e = db2[item][fieldname]
    # if "null", make NaN
    e.replace('null', np.nan, inplace=True)
    # turn durations greater than 1800 to NaN
    e.mask(e.astype(float) > 1800, inplace=True)

# %%

db2['RAVLT'] = pd.concat([db2['RAVLT'], db2['RAVLT2']], axis=0)
db['RAVLT'] = pd.concat([db['RAVLT'], db['RAVLT2']], axis=0)

# %%

studyids = redcap.getredcapids()
studydata = redcap.getredcapdata()

# %%

for item in asslist.assessment:
    snap[item] = pd.concat([db[item], db2[item]], axis=1)

    # create snapshot of combined file store snapshot in 'store' and in
    # 'snapshots' under all sites directory in box.
    snapfilename = 'Q_' + item + '_Snapshot_' + snapshotdate + '.csv'
    snapshotfile = os.path.join(store_space, snapfilename)
    QCfile = os.path.join(cache_space, 'QC_' + snapfilename)

    snap[item].to_csv(snapshotfile, index=False)
    box.upload_file(snapshotfile, q_snapshotfolderid)

# %%

allrowsofinterest = pd.concat(snap.values(), sort=False)
allrowsofinterest = allrowsofinterest[['subject', 'source', 'assessment']]

combined = allrowsofinterest.merge(studyids, 'left', 'subject')
notinredcap = combined.loc[combined.Subject_ID.isnull()].copy()
notinredcap['reason'] = 'PatientID not in Redcap'

# %%

combined = allrowsofinterest.merge(studydata, 'right', 'subject')
notinboxunique = combined.loc[combined.source.isnull() & combined.flagged.isnull()].drop_duplicates('subject')

# %% md

# Make sure records are complete

# %%

status1 = redcap.getredcapfields(['data_status', 'misscat'], study='hcpdchild')
status1 = status1[['data_status', 'subject_id', 'misscat___9']]
status1.columns = ['data_status', 'subject_id', 'misscat']

status2 = redcap.getredcapfields(['data_status', 'misscat'], study='hcpa')
status2 = status2[['data_status', 'subject_id', 'misscat___7']]
status2.columns = ['data_status', 'subject_id', 'misscat']

status3 = redcap.getredcapfields(['data_status', 'misscat'], study='hcpd18')
status3 = status3[['data_status', 'subject_id', 'misscat___9']]
status3.columns = ['data_status', 'subject_id', 'misscat']

t = notinboxunique \
    .merge(status1, 'left', 'subject_id', suffixes=('', '_x')) \
    .merge(status2, 'left', 'subject_id', suffixes=('', '_y')) \
    .merge(status3, 'left', 'subject_id', suffixes=('', '_z'))


t.data_status.mask(t.data_status.isnull(), t.data_status_y, inplace=True)
t.data_status.mask(t.data_status.isnull(), t.data_status_z, inplace=True)
t.misscat.mask(t.misscat.isnull(), t.misscat_y, inplace=True)
t.misscat.mask(t.misscat.isnull(), t.misscat_z, inplace=True)

t.drop(columns={'data_status_y', 'data_status_z', 'misscat_y', 'misscat_z'}, inplace=True)

# %%

t.loc[t.data_status.isna(), 'reason'] = 'Missing in Box - visit summary incomplete'
t.loc[t.data_status == 1, 'reason'] = 'Missing in Box - visit summary says complete '
t.loc[(t.data_status == 2) & \
      (t.misscat != 1), 'reason'] = 'Missing in Box - visit summary says incomplete but cog testing not specified '
notinboxunique = t[~((t.data_status == 2) & (t.misscat == 1))]

# %%

# get list of ids that need visit numbers associated with files
needsvisit = baseclean.loc[baseclean.visit.isnull()]
needsvisit = needsvisit \
    .merge(studydata, 'left', 'subject') \
    .drop(columns={'dob', 'select_4clean', 'flagged', 'gender', 'subject_id'})
needsvisit['reason'] = 'please specify visit number'

# %%

## Dead Code?
check = baseclean.groupby(['subject', 'visit']).count()
check.loc[check.select_4clean == 2]

# %%

catQC = pd.concat([notinredcap, notinboxunique, needsvisit], axis=0, sort=True)
catQC = catQC[['subject', 'interview_date', 'study', 'site', 'source', 'filename', 'row', 'reason', 'visit']]
catQC = catQC.sort_values(['site', 'study'])

# %%

# if known perminently deleted, then remove from list
catQC = catQC[~catQC.subject.isin(basecleanexcluded.subject)]

QCfile = os.path.join(
    store_space,
    'QC_Qinteractive_assessments_snapshot_' +
    snapshotdate +
    '.csv')

catQC.to_csv(QCfile, index=False)
box.upload_file(QCfile, snapshotQCfolder)
# 1, MGH (Massachusetts General Hospital) or Harvard | 2, UCLA | 3,
# University of Minnesota | 4, Washington University in St. Louis

# makedatadict(slimf,assessments[item]['dict_id'],cachekeyfile,assessments[item]['key_sheet'])

shutil.rmtree(box.cache)

