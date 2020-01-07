#!/usr/bin/env python3

import os
import datetime
import pandas as pd
import numpy as np
from config import config
from download.redcap import Redcap
from download.box import LifespanBox

redcap = Redcap()
verbose = True
snapshotdate = datetime.datetime.today().strftime('%m_%d_%Y')

cache_space = config['dirs']['cache']['penncnp']
# this will be the place to save any snapshots on the nrg servers
store_space = config['dirs']['store']['penncnp']

# connect to Box
box = LifespanBox(cache=cache_space)

# snapshot folder (used to be the combined folder)
penn_snapshotfolderid = config['penn_snapshotfolderid']
snapshotQCfolder = config['snapshotQCfolder']
site_file = config['PennCNP']['snapshot']


def main():
    # KIDS UNDER 8 dont do PENNCNP
    path = box.downloadFile(site_file)
    rows = pd.read_excel(path)
    rows.columns = rows.columns.astype(str)
    # create snapshot of combined file store snapshot in 'store' and in
    # 'snapshots' under all sites directory in box.
    snap = 'PennCNP_Snapshot_' + snapshotdate + '.csv'
    snapshotfile = os.path.join(store_space, snap)
    QCfile = os.path.join(cache_space, 'QC_' + snap)
    # write rows to csv in store
    rows.to_csv(snapshotfile, index=False)
    # upload the snapshot into box
    box.upload_file(snapshotfile, penn_snapshotfolderid)
    # compare ids from snapshot (currently loaded into 'rows' dataframe)
    # with those in Redcap.
    studyids = redcap.getredcapids()
    studydata = redcap.getredcapdata()
    # rowsofinterest=rows[['ID','PatientID','PatientType','SiteName']].copy()
    rowsofinterest = rows[['datasetid',
                           'siteid', 'subid', 'assessment']].copy()
    rowsofinterest['subject'] = rowsofinterest.subid.str.strip()
    combowredcap = pd.merge(
        rowsofinterest,
        studyids,
        how='left',
        on='subject')
    # these are the ids that need to be checked by the sites
    combonotinredcap = combowredcap.loc[combowredcap.Subject_ID.isnull()].copy(
    )
    combonotinredcap['reason'] = 'PatientID not in Redcap'
    # combonotinredcap[['datasetid','subject','assessment','siteid','reason']].to_csv(QCfile,index=False)
    combowredcap2 = pd.merge(
        rowsofinterest,
        studydata,
        how='right',
        on='subject')
    combonotinbox = combowredcap2.loc[combowredcap2.subid.isnull()]
    notinboxunique = combonotinbox.drop_duplicates('subject_id')
    #notinboxunique.loc[(notinboxunique.interview_date<'2019-05-01') & (notinboxunique.flagged.isnull()==True)]
    notinboxunique = notinboxunique.loc[notinboxunique.flagged.isnull()]
    # notinboxunique=notinboxunique.loc[notinboxunique.interview_date<'2019-05-01']
    status1 = redcap.getredcapfields(
        ['data_status', 'misscat'], study='hcpdchild')
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
    tnotinboxunique.loc[tnotinboxunique.data_status_x.isnull(
    ), 'data_status_x'] = tnotinboxunique.data_status_y
    status3 = redcap.getredcapfields(
        ['data_status', 'misscat'], study='hcpd18')
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
    tnotinboxunique.loc[tnotinboxunique.data_status_x.isnull(
    ), 'data_status_x'] = tnotinboxunique.data_status
    tnotinboxunique.loc[tnotinboxunique.misscat.isnull(),
                        'misscat'] = tnotinboxunique['misscat___9']
    notinboxunique = tnotinboxunique.drop(
        columns={
            'data_status',
            'misscat___9'}).rename(
        columns={
            'data_status_x': 'data_status'})
    # this code needs to be moved to box.py next iteration - exclude
    # subjects who are too young to have had the test
    datevar = 'interview_date'
    notinboxunique['nb_months'] = (12 *
                                   (pd.to_datetime(notinboxunique[datevar]).dt.year -
                                    pd.to_datetime(notinboxunique.dob).dt.year) +
                                   (pd.to_datetime(notinboxunique[datevar]).dt.month -
                                       pd.to_datetime(notinboxunique.dob).dt.month) +
                                   (pd.to_datetime(notinboxunique[datevar]).dt.day -
                                       pd.to_datetime(notinboxunique.dob).dt.day) /
                                   31)
    notinboxunique['nb_months'] = notinboxunique['nb_months'].apply(
        np.floor).astype(int)
    notinboxunique = notinboxunique.loc[notinboxunique.nb_months >= 96]
    notinboxunique.loc[notinboxunique.subject_id != 'HCDJanePractice']

    # drop ids known to be permanently missing
    perm_misslist = [
        'HCA6012744',
        'HCA6605569',
        'HCA8689410',
        'HCA8735289',
        'HCA8917700',
        'HCA9695713',
        'HCD1539759',
        'HCD1714852',
        'HCD1769978',
        'HCD2642858',
        'HCD2913459',
        'HCD2978386']
    notinboxunique = notinboxunique.loc[~(
        notinboxunique.subject.isin(perm_misslist))]
    # subset to ids listed as data_status=2, misscat=0 - these are the
    # folks that should be
    notinboxunique = notinboxunique.loc[notinboxunique.interview_date < '2019-08-13']
    notinboxunique = notinboxunique.loc[(
        notinboxunique.data_status == 2) & (notinboxunique.misscat == 0)]

    notinboxunique['reason'] = 'Missing in Box'
    #####################
    # now find duplicates and missing visits
    dupls = rows.loc[rows.duplicated(subset=['subid', 'assessment'], keep=False)][[
        'datasetid', 'siteid', 'subid', 'assessment', 'sex', 'age', 'handedness']]
    dupls['reason'] = 'duplicated subid/assessment combination'
    checkvisit = rows.loc[rows.assessment.isin(['V1', 'V2', 'V3']) == False][[
        'datasetid', 'siteid', 'subid', 'assessment', 'sex', 'age', 'handedness']]
    checkvisit['reason'] = 'visit number not in V1, V2, or V3'

    catQC = pd.concat([combonotinredcap,
                       notinboxunique,
                       dupls,
                       checkvisit],
                      axis=0,
                      sort=True)[['datasetid',
                                  'siteid',
                                  'subid',
                                  'assessment',
                                  'subject',
                                  'study',
                                  'site',
                                  'gender',
                                  'interview_date',
                                  'reason']]
    catQC = catQC.sort_values(['site', 'study'])
    catQC.to_csv(QCfile, index=False)

    # upload QC file to box
    box.upload_file(QCfile, snapshotQCfolder)
    # just keep these in mind for time being
    # these are the ids that need to be excluded before any data is
    # shared...make note.
    comboflaggedinredcap = combowredcap.loc[combowredcap.flagged.isnull(
    ) == False]
    # NDA release will be based on an explicitly named snapshot...not just whatever is in the rows right now - but the following files will
    # ease the process of harmonization.
    # this is a temporary file in the Behavioral Data - Snapshots / PENN /
    # temp folder for use viewing data
    slimhandle = config['PennCNP']['slim']
    # make slim file in cache from snapshot in store (which is identical
    # copy with same named file on box).
    slimf = makeslim(snapshotfile, slimhandle)
    # Note this will overwrite whatever is in the slim file with current data - write slim file to cache and to Scratch Export to NDA
    # make a draft datadictionary from slim file and start comparing columns - write dictionary to cache and to Scratch Export to NDA

    # Clean up cache space
    # shutil.rmtree(box.cache)





def makeslim(storefilename, slim_id):
    """
    remove columns from cachecopy that have no data and upload slim file to box
    """
    box.downloadFile(slim_id)
    slimf = box.getFileById(slim_id)
    ksadsraw = pd.read_csv(storefilename, header=0, low_memory=False)
    ksadsraw = ksadsraw.dropna(axis=1, how='all')
    snapname = os.path.basename(storefilename)
    combined_fileout = os.path.join(
        box.cache, snapname.split('.')[0] + 'Slim.csv')
    ksadsraw.to_csv(combined_fileout, index=False)
    # box.client.folder(str(ksadscombinedfolderid)).upload(fileout)
    slimf.update_contents(combined_fileout)
    return slimf




if __name__ == '__main__':
    main()
