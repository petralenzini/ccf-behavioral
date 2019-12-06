import os
import datetime
import csv
import pycurl
import sys
import shutil
from openpyxl import load_workbook
import pandas as pd
import numpy as np

from download.box import LifespanBox
#from download.box import getredcapids


verbose = True
#verbose = False
snapshotdate = datetime.datetime.today().strftime('%m_%d_%Y')

root_cache = '/nrgpackages/tools.release/intradb/ccf-nda-behavioral/cache/'
cache_space = os.path.join(root_cache, 'penncnp')
try:
    os.mkdir(cache_space)
except BaseException:
    print("cache already exists")

root_store = '/home/shared/HCP/hcpinternal/ccf-nda-behavioral/store/'
# this will be the place to save any snapshots on the nrg servers
store_space = os.path.join(root_store, 'penncnp')
try:
    os.mkdir(store_space)  # look for store space before creating it here
except BaseException:
    print("store already exists")

# connect to Box
box = LifespanBox(cache=cache_space)

# snapshot folder (used to be the combined folder)
penn_snapshotfolderid = 48203214504
snapshotQCfolder = 76434619813
slimfolder = 77127935742


# hard coding to prevent read of files that shouldnt be in these folders in box.  cant do a search.
# WU,UMN,UCLA, and Harvard, HCA and HCD data all in one single file under
# Behavioral Data - allsites . This is the 'database' for PennCNP
assessments = {
    'PennCNP': {
        'slim_id': 460781218444,
        'dict_id': 460884641280,
        'cleanest_start': [452784840845]
    }
}


def main():
    for item in assessments:
        # Download latest clean (box) allsites file - make sure they exist.  send up warning if it dont -
        # Coordinator monthly update process is to download data (1 file contains all site data -convenient) from penncnp.med.upenn.edu/results.pl ,
        # append any new rows to site/assessment file (by downloading box file), incorporate and errors from wiki...and/or hca data checklist
        # then save updated xls files with new date, navigate to file in box and select 'upload new version.'
        # This will allow BOX to track versioning until better system is in place
        # KIDS UNDER 8 dont do PENNCNP
        site_files = assessments[item]['cleanest_start']
        # Get all rows from all site output files for cleanest files as a
        # pandas dataframe with column labels
        rows = get_all_rows(site_files)
        # create snapshot of combined file store snapshot in 'store' and in
        # 'snapshots' under all sites directory in box.
        snap = 'PennCNP_' + item + '_Snapshot_' + snapshotdate + '.csv'
        snapshotfile = os.path.join(store_space, snap)
        QCfile = os.path.join(cache_space, 'QC_' + snap)
        # write rows to csv in store
        rows.to_csv(snapshotfile, index=False)
        # upload the snapshot into box
        box.upload_file(snapshotfile, penn_snapshotfolderid)
        # compare ids from snapshot (currently loaded into 'rows' dataframe)
        # with those in Redcap.
        studyids = box.getredcapids()
        studydata = box.getredcapdata()
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
        status1 = box.getredcapfields(
            ['data_status', 'misscat'], study='hcpdchild')
        status1 = status1[['data_status', 'misscat___9', 'subject_id']].copy()
        tnotinboxunique = pd.merge(
            notinboxunique,
            status1,
            how='left',
            on='subject_id')
        status2 = box.getredcapfields(['data_status', 'misscat'], study='hcpa')
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
        status3 = box.getredcapfields(
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
        slimhandle = assessments[item]['slim_id']
        # make slim file in cache from snapshot in store (which is identical
        # copy with same named file on box).
        slimf = makeslim(snapshotfile, slimhandle)
        # Note this will overwrite whatever is in the slim file with current data - write slim file to cache and to Scratch Export to NDA
        # make a draft datadictionary from slim file and start comparing columns - write dictionary to cache and to Scratch Export to NDA
        # makedatadict(slimf,dict_id=assessments[item]['dict_id'],folderout=slimfolder)#,assessments[item]['dict_id'])#,cachekeyfile,assessments[item]['key_sheet'])
    # Clean up cache space
    shutil.rmtree(box.cache)


def findupdates(base_id=454918321952, compare_id=454298717674):
    """
    compare two files by dataset id for updates to other columns
    """
    fbase = box.getFileById(base_id)
    basecachefile = box.downloadFile(base_id)
    wb_base = load_workbook(filename=basecachefile)
    basequestionnaire = wb_base[wb_base.sheetnames[0]]
    fbaseraw = pd.DataFrame(basequestionnaire.values)
    header = fbaseraw.iloc[0]
    fbaseraw = fbaseraw[1:]
    fbaseraw.columns = header
    # now the file to compare
    fcompare = box.getFileById(compare_id)
    comparecachefile = box.downloadFile(compare_id)
    wb_compare = load_workbook(filename=comparecachefile)
    comparequestionnaire = wb_compare[wb_compare.sheetnames[0]]
    fcompareraw = pd.DataFrame(comparequestionnaire.values)
    header = fcompareraw.iloc[0]
    fcompareraw = fcompareraw[1:]
    fcompareraw.columns = header
    fjoined = pd.merge(fbaseraw, fcompareraw, on='ID', how='inner')
    # for all columns except the ID, compare...
    updates = pd.DataFrame()
    for col in fbaseraw.columns:
        if col == 'ID':
            pass
        else:
            fjoined.loc[fjoined[str(col) + '_x'] is None] = ""
            fjoined.loc[fjoined[str(col) + '_y'] is None] = ""
            update = fjoined.loc[~(
                fjoined[str(col) + '_x'] == fjoined[str(col) + '_y'])].copy()
            if update.empty:
                pass
            else:
                update['column_affected'] = str(col)
                update['base_value'] = fjoined[str(col) + '_x']
                update['compare_value'] = fjoined[str(col) + '_y']
                updates = updates.append(update)
    updates = updates[['ID', 'PatientID_x', 'SiteName_x',
                       'column_affected', 'base_value', 'compare_value']].copy()
    updates.rename(
        columns={
            'PatientID_x': 'PatientID_base',
            'SiteName_x': 'SiteName_base'})
    updates['basename'] = fbase.get().name
    updates['comparename'] = fcompare.get().name
    updates['base_id'] = base_id
    updates['compare_id'] = compare_id
    return updates


def get_all_rows(sites):
    """
    reads all rows from site xls files as data frame
    """
    rows = []
    for site_file in sites:
        # Download file contents to cache
        path = box.downloadFile(site_file)
        wb = load_workbook(filename=path)
        # print(wb.sheetnames)
        if len(wb.sheetnames) > 1:
            print('More than one worksheet.')
            print(wb.sheetnames)
            print('Using the first one --> ' + wb.sheetnames[0])
            continue
        questionnaire = wb[wb.sheetnames[0]]
        print(questionnaire)
        # Skip the header for all but the first file
        current_row = 0
        for row in questionnaire.values:
            if current_row == 0:
                header = row
            if current_row != 0:
                rows.append(row)
            current_row += 1
        rowsasdf = pd.DataFrame(rows, columns=header)
    return rowsasdf


def append_new_data(rows, combined_file):
    """
    Add rows for ids that don't exist and upload to Box
    """
    # print(rows)
    print(str(len(rows)) +
          ' rows found in old or new box files (may contain duplicates)')
    # combined_file_name = combined_file.get().name
    combined_file_path = os.path.join(box.cache, combined_file.get().name)
    # Get existing ids
    existing_ids = []
    with open(combined_file_path) as f:
        for combinedrow in f.readlines():
            # print(row.split(',')[0])
            existing_ids.append(str(combinedrow.split(',')[0]))
    # Get new rows based on id
    new_rows = []
    for row in rows:
        # print('record id: ' + str(row[0]))
        if str(row[0]) not in existing_ids:
            new_rows.append(row)
            # print(combined_file_name)
            # print('record id: ' + str(row[0]))
    print(str(len(new_rows)) + ' new rows')
    if not new_rows:
        print('Nothing new to add. Exiting Append Method...')
        return
    # Write new rows to combined file
    with open(combined_file_path, 'a') as csvfile:
        writer = csv.writer(
            csvfile,
            delimiter=',',
            quotechar='"',
            quoting=csv.QUOTE_MINIMAL
        )
        for row in new_rows:
            writer.writerow(row)
    # Put the file back in Box as a new version
    # box.update_file(combined_file_id, combined_file_name)
    combined_file.update_contents(combined_file_path)


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
    cachefile = os.path.join(box.cache, slimf.get().name.split('.')[0])
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


if __name__ == '__main__':
    main()
