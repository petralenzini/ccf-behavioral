#!/usr/bin/env python3

# this program just gathers all of the raw Q-interactive data from Box and generates a copy of this data (as a single data frame of filenames, box location, file_ids, the row of data, assessment type...etc)
# think of its output like a 'download' of everything newly saved to BOX.
# this 'download' is then saved to store space and to box raw folder under
# snapshots

import datetime
import os
import shutil

import pandas as pd

from config import config
from download.box import LifespanBox

# %%

verbose = True
snapshotdate = datetime.datetime.today().strftime('%m_%d_%Y')
cache_space = config['dirs']['cache']['qint']
store_space = config['dirs']['store']['qint']

processed_filename = os.path.join(store_space, 'ProcessedBoxFiles_AllRawData_Qinteractive.csv')
available_box_files = os.path.join(cache_space, 'AllBoxFiles_Qinteractive.csv')

box = LifespanBox(cache=cache_space)

sites = {
    18446355408: 'WUHCD',
    18446433567: 'WUHCA',
    18446318727: 'UMNHCD',
    18446298983: 'UMNHCA',
    18446352116: 'UCLAHCD',
    18446404271: 'UCLAHCA',
    18446321439: 'HARVHCD',
    18446404071: 'MGHHCA',
    #    47239506949: 'UMNHCASUB'
}


# %%

def findasstype(subset, search_string='RAVLT'):
    found = []
    notfound = []
    for f in subset.filename:
        absfile = os.path.join(cache_space, f)
        try:
            for line in open(absfile, encoding='utf-16'):
                if search_string in line:
                    found.append(f)
        except BaseException:
            notfound.append(f)
    nf = pd.DataFrame(notfound, columns=['filename'])
    nf = pd.DataFrame(nf.filename.unique(), columns=['filename'])
    fnd = pd.DataFrame(found, columns=['filename'])
    fnd = pd.DataFrame(fnd.filename.unique(), columns=['filename'])
    fnd['assessment'] = search_string
    return nf, fnd


# %%

def create_row(cachepath, filename, assessment):
    """
    Generates a single row per subject for a given output.
    Look for unique rows in the file that indicate data points,
    then capture subsequent rows from file until a newline.
    """
    section_headers = [
        'Subtest,,Raw score\n',
        'Subtest,,Scaled score\n',
        'Subtest,Type,Total\n',  # this not in aging or RAVLT
        'Subtest,,Completion Time (seconds)\n',
        'Subtest,Type,Yes/No\n',
        'Item,,Raw score\n',
        # 'Scoring Type,,Scores\n'
    ]
    # Last section header is repeat data except for RAVLT
    if ('RAVLT' in assessment):
        section_headers.append('Scoring Type,,Scores\n')
    subject_id = filename[:10]
    # this will have to be furthur cleaned for exceptions that dont have underscore in file name
    # subject_id = subject_id.split('.csv')[0]
    new_row = subject_id
    new_label = 'src_subject_id'
    path = os.path.join(cachepath, filename)
    capture_flag = False
    with open(path, encoding='utf-16') as f:
        for row in f.readlines():
            #  We know we want the data in the next rows
            if row in section_headers:
                capture_flag = True
                continue
            # We know a single newline char is the end of a section
            if row == '\n':
                capture_flag = False
                continue
            if not capture_flag:
                continue
            # print(row)
            value = row.split(',')[-1]
            label = row.split(',')[-3]
            # if value == '-':
            #     value = ''
            new_row += ',' + value.strip()
            new_label += ',' + label.strip()
    # print(new_row)
    print('Finished processing {}.'.format(filename))
    # sys.exit()
    # Save this file to already processed store
    # with open(processed_file, 'a') as store:
    #    store.write(filename + '\n')
    return new_row, new_label


# %%

def getfiles(folders, recursively=True):
    result = {}

    for folder_id, label in folders.items():

        print('getting filenames of box folder ' + label)
        items = list(box.client.folder(folder_id).get_items())
        files = {i.id: i.name for i in items if i.type == 'file'}
        folders = {i.id: i.name for i in items if i.type == 'folder'}

        result.update(files)
        if recursively:
            result.update(getfiles(folders, True))

    return result


def main():
    # %%

    files = getfiles(sites)
    superfilelist = pd.DataFrame(files.items(), columns=['file_id', 'filename'])
    # post processing of dataframe of box-site files...
    superfilelist['source'] = 'box-site'

    # filenames that dont begin with H wont produce subject ids that begin
    # with H...i.e. they dont follow naming convention.
    superfilelist.loc[~(superfilelist.filename.str[:1] == 'H')]
    # one was in the UCLA aging folder (9584097_5_). the other was in box-site
    # folder but checklist said to find data in BDAS --no idea who this is

    # %%

    bdas_folders = {75755393630: 'BDAS_HCD', 75755777913: 'BDAS_HCA'}
    bdasfiles = getfiles(bdas_folders)

    bdasfilelist = pd.DataFrame(bdasfiles.items(), columns=['file_id', 'filename'])
    bdasfilelist['source'] = 'BDAS'

    # %%

    # put them together...note that as of 5/22/19, there are 559 HCA, 162 HCD, and 2 others who are only represetned in the folders 1.
    # all other individuals in HCA and HCD have data in multiple places.
    # remove the info files from the list,
    # assign visit, and file type where it can be gleaned from filename

    # %%

    allboxfiles = pd.concat([superfilelist, bdasfilelist], axis=0)
    allboxfiles = allboxfiles[allboxfiles.filename.str.endswith('.csv')]
    allboxfiles['subject'] = allboxfiles.filename.str[:10]
    allboxfiles['visit'] = ''
    allboxfiles.loc[allboxfiles.filename.str.contains('V1'), 'visit'] = 'V1'
    allboxfiles.loc[allboxfiles.filename.str.contains('V2'), 'visit'] = 'V2'
    allboxfiles['assessment'] = ''
    allboxfiles.head()

    is_IQ = allboxfiles.filename.str.contains('Matrix Reasoning')
    allboxfiles.loc[(is_IQ & allboxfiles.filename.str.contains(' 17', regex=False)), 'assessment'] = 'WAIS'
    allboxfiles.loc[(is_IQ & allboxfiles.filename.str.contains('6-16', regex=False)), 'assessment'] = 'WISC'
    allboxfiles.loc[(is_IQ & allboxfiles.filename.str.contains('5_scores', regex=False)), 'assessment'] = 'WPPSI'
    allboxfiles.loc[allboxfiles.filename.str.contains('Aging_scores'), 'assessment'] = 'RAVLT'
    allboxfiles.loc[allboxfiles.filename.str.contains('RAVLT V2'), 'assessment'] = 'RAVLT2'

    allboxfiles.file_id = allboxfiles.file_id.astype(int)
    dupfilenames = pd.concat(g for _, g in allboxfiles.groupby(
        "filename") if len(g) > 1)  # with duplicate filenames

    # compare allboxfiles to list in store of already processed files -- not done yet...
    ######################################
    processed = pd.read_csv(processed_filename)
    processed = processed[['file_id', 'raw_processed_date']].copy()

    files4process = pd.merge(allboxfiles, processed, on='file_id', how='left')
    files4process = files4process.loc[files4process.raw_processed_date.isnull(
    )].copy()
    files4process.drop(columns=['raw_processed_date'], inplace=True)

    # download everything not already processed to cache - originally there
    # should be len(allboxfiles.file_id)=3506 as of 5/22/19 minus 2 with
    # duplicate filenames.  Updates will have far fewer
    # should end up with 3504 files plus whatever meta files I've made.
    # --note if not exact, its okay.  will grab in next run or in next steps
    box.download_files(files4process.file_id)

    # for filenames with pattern in their title, file type is easy.
    # determine filetype for others by looking for pattern in actual
    # downloaded file
    subset1 = files4process.loc[files4process.assessment == '']
    nfravlt, fndravlt = findasstype(subset1, search_string='RAVLT')

    # go back and find the nfravlt files that werent downloaded properly.
    # getlist=pd.merge(subset1,nfravlt,on='filename',how='inner')
    # box.download_files(getlist.file_id)
    # now repeat the findasstype call
    # nfravlt,fndravlt=findasstype(subset1,search_string='RAVLT')

    nfwais, fndwais = findasstype(subset1, search_string='WAIS')
    nfwisc, fndwisc = findasstype(subset1, search_string='WISC')
    nfwppsi, fndwppsi = findasstype(subset1, search_string='WPPSI')

    foundlist = pd.concat([fndravlt, fndwais, fndwisc, fndwppsi], axis=0)
    if not foundlist.empty:
        files4process = pd.merge(
            foundlist,
            files4process,
            on='filename',
            how='right')
        files4process.loc[files4process.assessment_x.isnull(
        ) == False, 'assessment'] = files4process.assessment_x
        files4process.loc[files4process.assessment_x.isnull(),
                          'assessment'] = files4process.assessment_y
        files4process = files4process.drop(
            columns=['assessment_x', 'assessment_y']).copy()
    else:
        pass  # file is probably corrupted, or already has assessment type -
        # files4process.loc[files4process.filename=='HCD0040113V1.csv','raw_processed_date']=snapshotdate
        # #this is corrupted file

    # check
    singles = pd.concat(g for _, g in files4process.groupby("subject") if len(
        g) == 1)  # some subjects have data in only one location--yay!
    duplicates = pd.concat(g for _, g in files4process.groupby("subject") if len(
        g) > 1)  # many subjects have data in more than one location
    dupfilenames = pd.concat(g for _, g in files4process.groupby("filename") if len(
        g) > 1)  # some even have exactly the same filenames in two locations

    # make rows of all these files, where possible because assessment is specified
    # then turn the entire thing into a csv file and save in store and in box
    # as a 'raw data' snapshot

    ###################################################################
    # if files4process isnt empty then proceed with an update otherwise stop here:

    files4process = files4process.reset_index()
    files4process.drop(columns=['index'], inplace=True)
    # files4process.drop(columns=['level_0'],inplace=True)

    new_rows = []
    for i in range(len(files4process.file_id)):
        print('processing ' + files4process.filename[i])
        try:
            # returns row=string of comma sep values in the file, and labels=list
            # of the labels associated with each value)
            rows, labels = create_row(
                cache_space, files4process.filename[i], files4process.assessment[i])
        except BaseException:
            rows = ' '
        new_rows.append(rows)

    # merge these with dataframe
    files4process['row'] = new_rows
    files4process['raw_processed_date'] = snapshotdate

    # sub4.to_csv(processed_file,index=False) - original initialization of processed file required 'to_csv'
    # cat these to the processed file

    processed = pd.read_csv(processed_filename)
    newprocessed = pd.concat([processed, files4process], axis=0, sort=True)
    newprocessed.to_csv(processed_filename, index=False)

    # this is in the behavioral data/snapshots/Q/raw_allfiles_in_box/ folder...is not the curated BDAS file - just keeping a record of the raw unprocessed download
    # box.upload_file(processed_file,76432368853) first run had to upload file
    # - subsequent runs just update
    box.update_file(462800613671, processed_filename)

    shutil.rmtree(box.cache)


if __name__ == '__main__':
    main()
