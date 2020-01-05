import datetime
import os
import shutil

import pandas as pd

from download.box import LifespanBox
# from download.box import getredcapids
from download.redcap import Redcap

verbose = True
#verbose = False
snapshotdate = datetime.datetime.today().strftime('%m_%d_%Y')


root_cache = '/data/intradb/tmp/box2nda_cache/'
cache_space = os.path.join(root_cache, 'eprime')
try:
    os.mkdir(cache_space)
except BaseException:
    print("cache already exists")

root_store = '/home/shared/HCP/hcpinternal/ccf-nda-behavioral/store/'
# this will be the place to save any snapshots on the nrg servers
store_space = os.path.join(root_store, 'eprime')
try:
    os.mkdir(store_space)  # look for store space before creating it here
except BaseException:
    print("store already exists")

# connect to Box
box = LifespanBox(cache=cache_space)
redcap = Redcap('../tmp/.boxApp/redcapconfig.csv')

# snapshot folder (used to be the combined folder)
e_snapshotfolderid = 82670538107
snapshotQCfolder = 76434619813
slimfolder = 82670800769  # (for data dictionaries)
cleanestdata = 495490047901


# Coordinator monthly update process is to run eprime_getraw.py to 'download' all of the individual records from box
# UCLA and WU upload folders for individual subjects...the python program converts the text files in these folders into rows
# of data for a given subject.  Coordinator role to check for new rows.  The eprime getraw program appends new data
# to the ProcessedBoxFiles_AllRawData_Eprime.csv file under snapshots/ePrimeDD/raw_allfiles_in_box
# Note, this box file is also synced with /home/shared/HCP/hcpinternal/ccf-nda-behavioral/store/eprime/ProcessedBoxFiles_AllRawData_Eprime.csv
# File ids in the store are getting rounded and converted when saved to box...so if you need fileids, grab from store.
# after running eprime_getraw.py, open the the current (and cumulatively cleaned) 'database' under BDAS/
# along with this ProcessedBoxFiles_AllRawData file andn append by hand any new rows to cleaned database by hand,
# at this time, incorporate and errors from wiki...and/or hca data checklist
# then save updated file with new date, navigate to file in box and select 'upload new version.'
# This will allow BOX to track versioning until better system is in place
# Once you've updated the Allsites database, you're ready to begin QC.
# remember to check visit summary for information pertaining to missing rows.
basecachefile = box.downloadFile(cleanestdata)
baseclean = pd.read_csv(basecachefile, header=0, low_memory=False)

studyids = redcap.getredcapids()
studydata = redcap.getredcapdata()
# create snapshot of combined file store snapshot in 'store' and in
# 'snapshots' under all sites directory in box.
snap = 'Eprime_Snapshot_' + snapshotdate + '.csv'
snapshotfile = os.path.join(store_space, snap)
QCfile = os.path.join(cache_space, 'QC_' + snap)
# write to csv in store
baseclean.to_csv(snapshotfile, index=False)
# upload the snapshot into box
box.upload_file(snapshotfile, e_snapshotfolderid)
# makedatadict(snapshotfile,dict_id=None,cachekeyfile=None,sheet=None,folderout=slimfolder)
baseclean = baseclean.loc[baseclean.exclude == 0]

rowsofinterest = baseclean[['subject', 'source', 'visit']].copy()
combowredcap = pd.merge(rowsofinterest, studydata, how='left', on='subject')
# these are the ids that need to be checked
combonotinredcap = combowredcap.loc[combowredcap.subject_id.isnull()].copy()
combonotinredcap['reason'] = 'PatientID not in Redcap'

combowredcap2 = pd.merge(rowsofinterest, studydata, how='right', on='subject')
combonotinbox = combowredcap2.loc[combowredcap2.source.isnull()].copy()
notinboxunique = combonotinbox.drop_duplicates('subject')
# this code needs to be moved to box.py next iteration - exclude subjects
# who are too young to have had the test
datevar = 'interview_date'
notinboxunique['nb_months'] = (12 *
                               (pd.to_datetime(notinboxunique[datevar]).dt.year -
                                pd.to_datetime(notinboxunique.dob).dt.year) +
                               (pd.to_datetime(notinboxunique[datevar]).dt.month -
                                   pd.to_datetime(notinboxunique.dob).dt.month) +
                               (pd.to_datetime(notinboxunique[datevar]).dt.day -
                                   pd.to_datetime(notinboxunique.dob).dt.day) /
                               31)
#notinboxunique['nb_months'] = notinboxunique['nb_months'].apply(np.floor).astype(int)
notinboxunique = notinboxunique.loc[notinboxunique.nb_months < 96].copy()
notinboxunique = notinboxunique.loc[notinboxunique.flagged.isnull()].copy()
notinboxunique['reason'] = 'Missing in Box'
# remove the permanent missings
status1 = redcap.getredcapfields(['data_status', 'misscat'], study='hcpdchild')
status1 = status1[['data_status', 'misscat___9', 'subject']].copy()
tnotinboxunique = pd.merge(notinboxunique, status1, how='left', on='subject')
notinboxunique = tnotinboxunique.loc[~(tnotinboxunique.misscat___9 == '1')]
notinboxunique = notinboxunique.loc[notinboxunique.data_status == '2']
# find missing visits and dup ids;
missvis = combowredcap.loc[combowredcap.visit.isnull()]
missvis['reason'] = 'Missing Visit Number'
# finddups
try:
    dups = pd.concat(g for _, g in baseclean.groupby("subject")
                     if len(g) > 1)  # with duplicate filenames
    dups['reason'] = 'duplicate id'
except BaseException:
    dups = pd.DataFrame()


catQC = pd.concat([combonotinredcap, notinboxunique, missvis, dups], axis=0, sort=True)[
    ['subject', 'study', 'site', 'source', 'gender', 'interview_date', 'reason']]

catQC = catQC.sort_values(['site', 'study'])
# remove flagged ids that are known to be permanently missing
# catQC=pd.merge(catQC,basecleanexcluded[['subject']],on='subject',how='left',indicator=True).copy()
# catQC=catQC.loc[catQC._merge=='left_only'].drop('_merge',axis=1).copy()


QCfile = os.path.join(
    store_space,
    'QC_Eprime_snapshot_' +
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
