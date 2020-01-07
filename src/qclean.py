#!/usr/bin/env python3

import datetime
import os
import shutil

import pandas as pd

from download.box import LifespanBox
# from download.box import getredcapids
from download.redcap import Redcap

redcap = Redcap('../tmp/.boxApp/redcapconfig.csv')

verbose = True
#verbose = False
snapshotdate = datetime.datetime.today().strftime('%m_%d_%Y')

root_cache = '/data/intradb/tmp/box2nda_cache/'
cache_space = os.path.join(root_cache, 'q')
try:
    os.mkdir(cache_space)
except BaseException:
    print("cache already exists")

root_store = '/home/shared/HCP/hcpinternal/ccf-nda-behavioral/store/'
# this will be the place to save any snapshots on the nrg servers
store_space = os.path.join(root_store, 'qinteractive')
try:
    os.mkdir(store_space)  # look for store space before creating it here
except BaseException:
    print("store already exists")

# connect to Box
box = LifespanBox(cache=cache_space)

# snapshot folder (used to be the combined folder)
q_snapshotfolderid = 48203213208
snapshotQCfolder = 76434619813
slimfolder = 77947037982  # (for data dictionaries)
cleanestdata = 465568117756

# def main():  not defining a main because wnat to run lines one by one
# Coordinator monthly update process is to run qintparts_getraw.py to 'download' all of the individual records from box
# (sites will have to download,subject by subject, from Qinteractive website to box), and check for new rows.
# This qintparts_getraw program appends new data to the ProcessedBoxFiles_AllRawData_Qinteractive.csv file under snapshots/Q/raw_allfiles_in_box
# Note, this box file is also synced with /home/shared/HCP/hcpinternal/ccf-nda-behavioral/store/qinteractive/ProcessedBoxFiles_AllRawData_Qinteractive.csv
# File ids in the store are getting rounded and converted when saved to box...so if you need fileids, grab from store.
# after running qintparts_getraw.py, open the the current (and cumulatively cleaned) 'database' under BDAS/2019..._HCA-HCD_Allsites_QandRAVLT.csv
# along with this ProcessedBoxFiles_AllRawData file andn append by hand any new rows to cleaned database by hand,
# at this time, incorporate and errors from wiki...and/or hca data checklist
# then save updated file with new date, navigate to file in box and select 'upload new version.'
# This will allow BOX to track versioning until better system is in place
# Once you've updated the Allsites database, you're ready to begin QC.
# remember to check visit summary for information pertaining to missing rows.
# Get all rows from all site output files for cleanest files as a pandas
# dataframe with column labels BY ASSESSMENT and tease out vars
basecachefile = box.downloadFile(cleanestdata)
#excelfile='/home/petra/UbWinSharedSpace1/redcap2nda_Lifespan2019/LifeSpan_RedCap_Data_Dictionaries/HCP-A&D Family Relationships  (4).xlsx'
#dfdupids=pd.read_excel(excelfile,sheet_name='HCP Participants - Multiple IDs',header=0)
baseclean = pd.read_excel(
    basecachefile,
    sheet_name='HCA-HCD_Allsites_QandRAVLT_2019',
    header=0)
basecleanexcluded = baseclean.loc[baseclean.source == 'perm-missing']
# basecleanexcluded[['subject']]

# baseclean=pd.read_csv(basecachefile,header=0,low_memory=False)
baseclean = baseclean.loc[baseclean.select_4clean == 1]
baseclean.row = baseclean.row.str.replace('-', '')

asslist = baseclean.groupby('assessment').count()

asslist.reset_index(inplace=True)
# asslist.assessment
#     RAVLT
#    RAVLT2
#      WAIS
#      WISC
#     WPPSI

# read row as dataframe and assign varnames from stored var list file
for item in asslist.assessment:
    vars()[item + 'db'] = baseclean.loc[baseclean.assessment == item]
    vars()[item + 'db'].reset_index(inplace=True)
    vars()[item + 'db'] = vars()[item + 'db'].drop(columns=['index'])
    vars()[
        item +
        'vars'] = pd.read_csv(
        store_space +
        '/' +
        item +
        '_vars.csv',
        header=None,
        names=['Varnames'])
    vars()[item + 'expanded'] = vars()[item + 'db'].row.str.split(pat=',')
    vars()[item + 'db2'] = pd.DataFrame(vars()
                                        [item + 'expanded'].values.tolist())
    vars()[item + 'db2'].columns = vars()[item + 'vars'].Varnames
    vars()[item + 'db2'].reset_index(inplace=True)
    vars()[item + 'db2'] = vars()[item + 'db2'].drop(columns=['index'])

# fix matrix completion and delay completion times
RAVLTdb2.loc[RAVLTdb2.delay_completion == 'null', 'delay_completion'] = None
RAVLTdb2.loc[RAVLTdb2.delay_completion.astype(
    float) > 1800, 'delay_completion'] = None

RAVLT2db2.loc[RAVLT2db2.delay_completion == 'null', 'delay_completion'] = None
RAVLT2db2.loc[RAVLT2db2.delay_completion.astype(
    float) > 1800, 'delay_completion'] = None

WAISdb2.loc[WAISdb2.matrix_completion == 'null', 'matrix_completion'] = None
WAISdb2.loc[WAISdb2.matrix_completion.astype(
    float) > 1800, 'matrix_completion'] = None

WISCdb2.loc[WISCdb2.matrix_completion == 'null', 'matrix_completion'] = None
WISCdb2.loc[WISCdb2.matrix_completion.astype(
    float) > 1800, 'matrix_completion'] = None

WPPSIdb2.loc[WPPSIdb2.matrix_completion == 'null', 'matrix_completion'] = None
WPPSIdb2.loc[WPPSIdb2.matrix_completion.astype(
    float) > 1800, 'matrix_completion'] = None


RAVLTdb2 = pd.concat([RAVLTdb2, RAVLT2db2], axis=0)
RAVLTdb = pd.concat([RAVLTdb, RAVLT2db], axis=0)
# ass2=asslist.loc[~(asslist.assessment=='RAVLT2'),'assessment']
ass2 = asslist.assessment

studyids = redcap.getredcapids()
studydata = redcap.getredcapdata()


for item in ass2:
    vars()[item + 'snap'] = pd.concat([vars()
                                       [item + 'db'], vars()[item + 'db2']], axis=1)
    # create snapshot of combined file store snapshot in 'store' and in
    # 'snapshots' under all sites directory in box.
    snap = 'Q_' + item + '_Snapshot_' + snapshotdate + '.csv'
    snapshotfile = os.path.join(store_space, snap)
    QCfile = os.path.join(cache_space, 'QC_' + snap)
    # write to csv in store
    vars()[item + 'snap'].to_csv(snapshotfile, index=False)
    # upload the snapshot into box
    box.upload_file(snapshotfile, q_snapshotfolderid)
    # makedatadict(snapshotfile,dict_id=None,cachekeyfile=None,sheet=None,folderout=slimfolder)

notinredcap = pd.DataFrame()
allrowsofinterest = pd.DataFrame()
for item in ass2:
    # compare ids from snapshot (currently loaded into 'rows' dataframe) with
    # those in Redcap.
    rowsofinterest = vars()[item +
                            'snap'][['subject', 'source', 'assessment']].copy()
    combowredcap = pd.merge(rowsofinterest, studyids, how='left', on='subject')
    # these are the ids that need to be checked
    combonotinredcap = combowredcap.loc[combowredcap.Subject_ID.isnull()].copy(
    )
    combonotinredcap['reason'] = 'PatientID not in Redcap'
    notinredcap = pd.concat([notinredcap, combonotinredcap], axis=0)
    allrowsofinterest = pd.concat(
        [allrowsofinterest, rowsofinterest], axis=0, sort=True)

combowredcap2 = pd.merge(
    allrowsofinterest,
    studydata,
    how='right',
    on='subject')
combonotinbox = combowredcap2.loc[combowredcap2.source.isnull()].copy()
notinboxunique = combonotinbox.drop_duplicates('subject')
#notinboxunique.loc[(notinboxunique.interview_date<'2019-05-01') & (notinboxunique.flagged.isnull()==True)]
notinboxunique = notinboxunique.loc[notinboxunique.flagged.isnull()]
# notinboxunique=notinboxunique.loc[notinboxunique.interview_date<'2019-05-01']
# makes sure records are complete
status1 = redcap.getredcapfields(['data_status', 'misscat'], study='hcpdchild')
status1 = status1[['data_status', 'misscat___9', 'subject_id']].copy()
tnotinboxunique = pd.merge(
    notinboxunique,
    status1,
    how='left',
    on='subject_id')
status2 = redcap.getredcapfields(['data_status', 'misscat'], study='hcpa')
status2 = status2[['data_status', 'misscat___7', 'subject_id']].copy()
tnotinboxunique = pd.merge(
    tnotinboxunique,
    status2,
    how='left',
    on='subject_id')
tnotinboxunique['misscat'] = tnotinboxunique['misscat___9']
tnotinboxunique.loc[tnotinboxunique.misscat.isnull(),
                    'misscat'] = tnotinboxunique['misscat___7']
tnotinboxunique.loc[tnotinboxunique.data_status_x.isnull(),
                    'data_status_x'] = tnotinboxunique.data_status_y
status3 = redcap.getredcapfields(['data_status', 'misscat'], study='hcpd18')
status3 = status3[['data_status', 'subject_id', 'misscat___9']].copy()
tnotinboxunique = pd.merge(
    tnotinboxunique.drop(
        columns={
            'data_status_y',
            'misscat___7',
            'misscat___9'}),
    status3,
    how='left',
    on='subject_id')
tnotinboxunique.loc[tnotinboxunique.data_status_x.isnull(),
                    'data_status_x'] = tnotinboxunique.data_status
tnotinboxunique.loc[tnotinboxunique.misscat.isnull(),
                    'misscat'] = tnotinboxunique['misscat___9']
notinboxunique = tnotinboxunique.drop(
    columns={
        'data_status',
        'misscat___9'}).rename(
            columns={
                'data_status_x': 'data_status'})

notinboxunique.loc[notinboxunique.data_status == '',
                   'reason'] = 'Missing in Box - visit summary incomplete'
notinboxunique.loc[notinboxunique.data_status == '1',
                   'reason'] = 'Missing in Box - visit summary says complete '
notinboxunique.loc[(notinboxunique.data_status == '2') & (notinboxunique.misscat != '1'),
                   'reason'] = 'Missing in Box - visit summary says incomplete but cog testing not specified '
notinboxunique = notinboxunique.loc[~(
    (notinboxunique.data_status == '2') & (notinboxunique.misscat == '1'))]

# get list of ids that need visit numbers associated with files
needsvisit = baseclean.loc[baseclean.visit.isnull()]
needsvisit = pd.merge(
    needsvisit,
    studydata,
    how='left',
    on='subject').drop(
        columns={
            'dob',
            'select_4clean',
            'flagged',
            'gender',
            'subject_id'})
needsvisit['reason'] = 'please specify visit number'

check = baseclean.groupby(['subject', 'visit']).count()
check.loc[check.select_4clean == 2]


catQC = pd.concat([notinredcap, notinboxunique, needsvisit], axis=0, sort=True)[
    ['subject', 'interview_date', 'study', 'site', 'source', 'filename', 'row', 'reason', 'visit']]
catQC = catQC.sort_values(['site', 'study'])
# remove flagged ids that are known to be permanently missing
catQC = pd.merge(catQC,
                 basecleanexcluded[['subject']],
                 on='subject',
                 how='left',
                 indicator=True).copy()
catQC = catQC.loc[catQC._merge == 'left_only'].drop('_merge', axis=1).copy()
catQC.loc[catQC.site == 4]


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


def makedatadict(
        slimf,
        dict_id=None,
        cachekeyfile=None,
        sheet=None,
        folderout=None):
    """
    create datadictionary from csvfile and upload dictionary to box
    """
    try:
        box.downloadFile(dict_id)
        dictf = box.getFileById(dict_id)
        dictf = box.download_file(dict_id)
    except BaseException:
        dictf = None
    try:
        cachefile = os.path.join(box.cache, slimf.get().name.split('.')[0])
    except BaseException:
        cachefile = os.path.join(box.cache, slimf.split('.')[0])
    ksadsraw = pd.read_csv(cachefile + '.csv', header=0, low_memory=False)
    varvalues = pd.DataFrame(
        columns=[
            'variable',
            'values_or_example',
            'numunique'])
    varvalues['variable'] = ksadsraw.columns
    varvalues['type'] = ksadsraw.dtypes.reset_index()[0]
    kcounts = ksadsraw.count().reset_index().rename(
        columns={'index': 'variable', 0: 'num_nonmissing'})
    varvalues = pd.merge(varvalues, kcounts, on='variable', how='inner')
    # create a data frame containing summary info of data in the ksadraw, e.g.
    # variablse, their formats, values, ect.
    for var in ksadsraw.columns:
        row = ksadsraw.groupby(var).count().reset_index()[var]
        varvalues.loc[varvalues.variable == var, 'numunique'] = len(
            row)  # number of unique vars in this column
        varvalues.loc[(varvalues.variable == var) & (varvalues.numunique <= 10) & (
            varvalues.num_nonmissing >= 10), 'values_or_example'] = ''.join(str(ksadsraw[var].unique().tolist()))
        try:
            varvalues.loc[(varvalues.variable == var) & (varvalues.numunique <= 10) & (
                varvalues.num_nonmissing < 10), 'values_or_example'] = ksadsraw[var].unique().tolist()[1]
        except BaseException:
            pass
        try:
            varvalues.loc[(varvalues.variable == var) & (
                varvalues.numunique > 10), 'values_or_example'] = ksadsraw[var].unique().tolist()[1]
        except BaseException:
            pass
    # capture labels for the vars in this assessment from the key
    if cachekeyfile:
        keyasrow = pd.read_excel(cachekeyfile, sheet_name=sheet, header=0)
        varlabels = keyasrow.transpose().reset_index().rename(
            columns={'index': 'variable', 0: 'question_label'})
        varlabels['variable'] = varlabels['variable'].apply(str)
        # now merge labels for the informative variables from cache
        varvalues2 = pd.merge(varvalues, varlabels, on='variable', how='left')
        varvalues2 = varvalues2[['variable',
                                 'question_label',
                                 'values_or_example',
                                 'numunique',
                                 'num_nonmissing']].copy()
    else:
        varvalues2 = varvalues.copy()
    # push this back to box
    fileoutdict = os.path.join(cache_space, cachefile + "_DataDictionary.csv")
    varvalues2.to_csv(fileoutdict, index=False)
    if dictf is None:
        box.upload_file(fileoutdict, str(folderout))
    else:
        box.update_file(dict_id, fileoutdict)
