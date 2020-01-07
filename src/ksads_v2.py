#!/usr/bin/env python3


import os
import datetime
import pandas as pd
import numpy as np
from config import config
from download import redcap

from download.box import LifespanBox
from download.redcap import Redcap

config['root'] = {'cache': '/home/osboxes/PycharmProjects/ccf/tmp/cache/',
                  'store': '/home/osboxes/PycharmProjects/ccf/tmp/store/'}
config['config_files']['box'] = '/home/osboxes/PycharmProjects/ccf/tmp/.boxApp/config.json'

# verbose = False
verbose = True
snapshotdate = datetime.datetime.today().strftime('%m_%d_%Y')
ksads_cache_path = config['dirs']['cache']['ksads']
# this will be the place to save any snapshots on the nrg servers
store_space = config['dirs']['store']['ksads']

# connect to Box
box = LifespanBox(cache=ksads_cache_path, config_file=config['box'])
redcap = Redcap(config['redcap']['config'])
assessments = config['Assessments']
sites = config['Sites']
# verbose = False

# snapshot folder (used to be the combined folder)
ksads_snapshotfolderid = config['ksads_snapshotfolderid']
snapshotQCfolder = config['snapshotQCfolder']

# download one of the identical key files which contain the labels for
# all the numbered questions in KSADS
cachekeyfile = box.downloadFile(config['cachekeyfile'])


def main():
    for item in assessments:
        # Download latest clean (box) files for each site - make sure they exist.  send up warning if they dont -
        # Before running this program, Montly update process is to download data (3 files - intro,screener,supplement,
        # for each site) from KSADS.net, append rows to site/assessment file (by hand until this program written to automate),
        # and incorporate and errors from wiki...look for any missing rows, but check redcap visit summary to make sure not already known to be missing
        # then save updated xls files with new date, navigate to file in box and select 'upload new version.'
        # This will allow BOX to track versioning until better system is in place
        site_files = assessments[item]['cleanest_start']
        rows = get_all_rows(site_files)

        # create snapshot of combined file (all four sites together) store
        # snapshot in 'store' and in 'snapshots' under all sites directory in
        # box.
        snap = 'KSADS_%s_Snapshot_%s.csv' % (item, snapshotdate)
        snapshot_filepath = os.path.join(store_space, snap)
        QC_filepath = os.path.join(ksads_cache_path, 'QC_' + snap)
        dictcsv_filepath = os.path.join(ksads_cache_path, 'Dict_' + snap)

        # write rows to csv in store, then upload to box
        rows.to_csv(snapshot_filepath, index=False)
        box.upload_file(snapshot_filepath, ksads_snapshotfolderid)

        # compare ids from snapshot (currently loaded into 'rows' dataframe)
        # with those in Redcap.
        studyids = redcap.getredcapids()
        studydata = redcap.getredcapdata()
        rowsofinterest = rows[['ID', 'PatientID',
                               'PatientType', 'SiteName']].copy()
        new = rowsofinterest['PatientID'].str.split("_", 1, expand=True)
        rowsofinterest['subject'] = new[0].str.strip()
        combowredcap = pd.merge(
            rowsofinterest,
            studyids,
            how='left',
            on='subject')
        # these are the ids that need to be checked by the sites
        combonotinredcap = combowredcap.loc[combowredcap.Subject_ID.isnull()].copy()
        combonotinredcap['reason'] = 'PatientID not in Redcap'
        combonotinredcap['site'] = combonotinredcap.SiteName
        # combonotinredcap[['ID','PatientID','PatientType','SiteName','reason']].to_csv(QC_filepath,index=False)

        combowredcap2 = pd.merge(
            rowsofinterest,
            studydata,
            how='right',
            on='subject')
        combonotinbox = combowredcap2.loc[combowredcap2.ID.isnull()]
        notinboxunique = combonotinbox.drop_duplicates('subject_id')
        # notinboxunique.loc[(notinboxunique.interview_date<'2019-05-01') & (notinboxunique.flagged.isnull()==True)]
        notinboxunique = notinboxunique.loc[notinboxunique.flagged.isnull()]
        notinboxunique = notinboxunique.loc[notinboxunique.interview_date < '2019-05-01']
        notinboxunique = notinboxunique.loc[~(
                notinboxunique.study == 'hcpa')].copy()
        notinboxunique['reason'] = 'Missing in Box'

        dups = combowredcap.loc[combowredcap.duplicated(
            subset=['PatientID', 'PatientType'], keep=False)]
        dups['reason'] = 'Duplicated ID'

        catQC = pd.concat([combonotinredcap, notinboxunique, dups], axis=0, sort=True)[
            ['ID', 'PatientID', 'subject', 'study', 'site', 'gender', 'interview_date', 'reason']]
        catQC = catQC.sort_values(['site', 'study'])
        catQC.to_csv(QC_filepath, index=False)

        # upload QC file to box
        box.upload_file(QC_filepath, snapshotQCfolder)

        # just keep these in mind for time being
        # these are the ids that need to be
        comboflaggedinredcap = combowredcap.loc[combowredcap.flagged.notnull()]
        # excluded before any data is shared...make note.

        # NDA release will be based on an explicitly named snapshot...not just whatever is in the rows right now - but the following files will
        # ease the process of harmonization.
        # slimhandle = assessments[item]['slim_id'] #this is a temporary file in the Behavioral Data - Snapshots / KSADS / temp folder for use viewing data
        # slimf = makeslim(snapshot_filepath,slimhandle)  #make slim file in cache from snapshot in store (which is identical copy with same named file on box).
        # Note this will overwrite whatever is in the slim file with current data - write slim file to cache and to Scratch Export to NDA
        # make a draft datadictionary from slim file and start comparing columns - write dictionary to cache and to Scratch Export to NDA
        # makedatadictv2(slimf,assessments[item]['dict_id'],cachekeyfile,assessments[item]['key_sheet'])
        inst = item
        makedatadictv2(snapshot_filepath, dictcsv_filepath, inst)
        box.upload_file(dictcsv_filepath, ksads_snapshotfolderid)
    # Clean up cache space
    # shutil.rmtree(box.cache)


def get_all_rows(sites):
    """
    reads all rows from site xls files as data frame
    """
    dataframes = []
    for site_file in sites:
        # Download file contents to cache
        path = box.downloadFile(site_file)
        df = pd.read_excel(path)
        df.columns = df.columns.astype(str)
        dataframes.append(df)

    return pd.concat(dataframes)


def makedatadictv2(filecsv, dictcsv, inst):
    """
    create datadictionary from csvfile
    """
    ksadsraw = pd.read_csv(filecsv, header=0, low_memory=False)
    varvalues = pd.DataFrame(
        columns=[
            'variable',
            'values_or_example',
            'numunique'])
    varvalues['variable'] = ksadsraw.columns
    kcounts = ksadsraw.count().reset_index().rename(
        columns={'index': 'variable', 0: 'num_nonmissing'})
    varvalues = pd.merge(varvalues, kcounts, on='variable', how='inner')
    summarystats = ksadsraw.describe().transpose(
    ).reset_index().rename(columns={'index': 'variable'})
    varvalues = pd.merge(varvalues, summarystats, on='variable', how='left')
    varvalues['min'] = varvalues['min'].fillna(-99)
    varvalues['max'] = varvalues['max'].fillna(-99)
    varvalues['ValueRange'] = varvalues['min'].astype(int).astype(
        str) + ' :: ' + varvalues['max'].astype(int).astype(str)
    varvalues['min'] = varvalues['min'].replace(-99.0, np.nan)
    varvalues['max'] = varvalues['max'].replace(-99.0, np.nan)
    varvalues.loc[varvalues.ValueRange.str.contains('-99'), 'ValueRange'] = ' '
    # create a data frame containing summary info of data in the ksadraw, e.g.
    # variablse, their formats, values, ect.
    for var in ksadsraw.columns:
        row = ksadsraw.groupby(var).count().reset_index()[var]
        varvalues.loc[varvalues.variable == var, 'numunique'] = len(
            row)  # number of unique vars in this column
        varvalues.loc[(varvalues.variable == var) & (varvalues.numunique <= 10) &
                      (varvalues.num_nonmissing >= 10), 'values_or_example'] = ''.join(
            str(ksadsraw[var].unique().tolist()))
        varvalues.loc[(varvalues.variable == var) & (varvalues.numunique <= 10) & (
                varvalues.num_nonmissing < 10), 'values_or_example'] = ksadsraw[var].unique().tolist()[0]
        varvalues.loc[(varvalues.variable == var) & (varvalues.numunique > 10),
                      'values_or_example'] = ksadsraw[var].unique().tolist()[0]
    varvalues['Instrument Title'] = inst
    varvalues.to_csv(dictcsv, index=False)


if __name__ == '__main__':
    main()
