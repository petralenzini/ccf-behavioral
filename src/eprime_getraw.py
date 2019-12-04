# this program just gathers all of the raw E-prime data from Box and generates a copy of this data (as a single data frame of filenames, box location, file_ids, the row of data, assessment type...etc)
# think of its output like a 'download' of everything newly saved to BOX.
# this 'download' is then saved to store space and to box raw folder under
# snapshots

import os
import datetime
import sys
import shutil

import pandas as pd

from download.box import LifespanBox
verbose = True
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

processed_file = os.path.join(store_space,
                              'ProcessedBoxFiles_AllRawData_Eprime.csv')
available_box_files = os.path.join(cache_space, 'AllBoxFiles_Eprime.csv')

# generate the box object which contains the necessary client config to
# talks to box, and sets up the cache space
box = LifespanBox(cache=cache_space)

# this section will
# necessary to search folders with
# generate list of all files in Q directories and identify those that dont follow pattern
# each of the site folders contains individual folders
sitefolderslabels = ["WUHCD", "UCLAHCD"]  # ,"UMNHCASUB"]
sitefolderlist = [41361544018, 61956482658]


# get folder contents for all the sites including the known subfolder of individuals folders
# folderlistcontents generates two dfs: a df with names and ids of files
# and a df with names and ids of folders
superfilelist, superfolderlist = folderlistcontents(
    sitefolderslabels, sitefolderlist)  # 2378 files and 1 folders as of 5/22/2019
if (superfilelist.shape[0] == 0):
    print('found expected folderstructure ')
else:
    print('well, hello, new files whose contents may or may not be captured')
    superfilelist


# iterate through the folders in superfolderlist and grab the text files
# therein
subjectfiles, subjectfolders = folderlistcontents(
    list(superfolderlist.foldername), list(map(int, superfolderlist.folder_id)))

# just want the text files for processing
textfiles = subjectfiles.loc[subjectfiles.filename.str.contains('.txt')]


# post processing of dataframe of box-site files...
# rename so you can use old code
superfilelist = textfiles.copy()
superfilelist['source'] = 'box-site'
superfilelist['visit'] = ''
superfilelist['subject'] = superfilelist.filename.str[:10]
# filenames that dont begin with H wont produce subject ids that begin
# with H...i.e. they dont follow naming convention.
superfilelist.loc[~(superfilelist.subject.str[:1] == 'H')]


# allboxfiles=pd.concat([superfilelist,bdasfilelist],axis=0)
allboxfiles = superfilelist
allboxfiles.loc[allboxfiles.filename.str.contains('V1'), 'visit'] = 'V1'
allboxfiles.loc[allboxfiles.filename.str.contains('V2'), 'visit'] = 'V2'

allboxfiles.file_id = allboxfiles.file_id.astype(int)
dupfilenames = pd.concat(g for _, g in allboxfiles.groupby(
    "filename") if len(g) > 1)  # with duplicate filenames

# compare allboxfiles to list in store of already processed files -- not done yet...
######################################
processed = pd.read_csv(processed_file)
processed = processed[['file_id', 'raw_processed_date']].copy()

files4process = pd.merge(allboxfiles, processed, on='file_id', how='left')
files4process = files4process.loc[files4process.raw_processed_date.isnull(
)].copy()
files4process.drop(columns=['raw_processed_date'], inplace=True)
# files4process=allboxfiles #first time around grab everything
# download everything not already processed to cache - originally there
# should be len(allboxfiles.file_id)=3506 as of 5/22/19 minus 2 with
# duplicate filenames.  Updates will have far fewer
# only a handful of really young kids ran this battery
box.download_files(files4process.file_id)
# one file was thwarting naming convention and goofing up read row
# functions...figure out how to be more flexible....

# need to convert the windows text file to unix (extra iconv step because
# no BOM)
for file in files4process.filename:
    myCmd = 'iconv -f UTF-16 -t UTF-8 ' + cache_space + '/' + \
        file + ' | dos2unix > ' + cache_space + '/Utf8_unix_' + file
    print('Running system command: ' + myCmd)
    os.system(myCmd)

# now read each file into a row (create row from txt file)
rows = pd.DataFrame()
for file in files4process.filename:
    test = os.popen('tail -20 ' + cache_space + '/Utf8_unix_' + file).read()
    test2 = pd.DataFrame(test.split('\n'))
    test3 = test2[0].str.split(':', expand=True)
    try:
        test3.columns = ['varname', 'values']
        test4 = test3.loc[test3.varname.str.contains('SV')]
        test4.index = test4.varname
        test5 = test4.drop(columns='varname').transpose()
        test5['filename'] = file
        rows = pd.concat([rows, test5], axis=0)
    except BaseException:
        print(file + ' doesnt conform to pattern...please examine')

files4process = pd.merge(files4process, rows, how='left', on='filename')
files4process['raw_processed_date'] = snapshotdate

# files4process.to_csv(processed_file,index=False) # original initialization of processed file required 'to_csv'
# cat these to the processed file


processed = pd.read_csv(processed_file)
newprocessed = pd.concat([processed, files4process], axis=0, sort=True)
# reorder columns so they can be pasted into curated more easily
newprocessed_reorder = newprocessed[['file_id',
                                     'filename',
                                     'source',
                                     'visit',
                                     'subject',
                                     'SV1wk20',
                                     'SV2wk20',
                                     'SV1mo20',
                                     'SV6mo20',
                                     'SV1yr20',
                                     'SV3yr20',
                                     'SV1wk100',
                                     'SV2wk100',
                                     'SV1mo100',
                                     'SV6mo100',
                                     'SV1yr100',
                                     'SV3yr100',
                                     'raw_processed_date']]


# check for dups
dups = newprocessed.loc[newprocessed_reorder.duplicated(
    subset=['subject', 'visit'], keep=False)]

newprocessed_reorder.to_csv(processed_file, index=False)

# this is in the behavioral data/snapshots/ePrimeDD/raw_allfiles_in_box/ folder...is not the curated BDAS file nor is it officially a snapshot - just keeping a record of the raw unprocessed download
# box.upload_file(processed_file,82670454492)# first run had to upload
# file - subsequent runs just update
box.update_file(495494179106, processed_file)

shutil.rmtree(box.cache)


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


def folderlistcontents(folderslabels, folderslist):
    bdasfilelist = pd.DataFrame()
    bdasfolderlist = pd.DataFrame()
    for i in range(len(folderslist)):
        print(
            'getting file and folder contents of box folder ' +
            folderslabels[i])
        # foldercontents generates two dfs: a df with names and ids of files
        # and a df with names and ids of folders
        subfiles, subfolders = foldercontents(folderslist[i])
        bdasfilelist = bdasfilelist.append(subfiles)
        bdasfolderlist = bdasfolderlist.append(subfolders)
    return bdasfilelist, bdasfolderlist


def foldercontents(folder_id):
    filelist = []
    fileidlist = []
    folderlist = []
    folderidlist = []
    WUlist = box.client.folder(
        folder_id=folder_id).get_items(
        limit=None,
        offset=0,
        marker=None,
        use_marker=False,
        sort=None,
        direction=None,
        fields=None)
    for item in WUlist:
        if item.type == 'file':
            filelist.append(item.name)
            fileidlist.append(item.id)
        if item.type == 'folder':
            folderlist.append(item.name)
            folderidlist.append(item.id)
    files = pd.DataFrame({'filename': filelist, 'file_id': fileidlist})
    folders = pd.DataFrame(
        {'foldername': folderlist, 'folder_id': folderidlist})
    return files, folders


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
    #subject_id = subject_id.split('.csv')[0]
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
