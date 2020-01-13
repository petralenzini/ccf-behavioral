#!/usr/bin/env python
# coding: utf-8

# ### Purpose
# This program gathers all of the new raw Q-interactive data from Box and appends it to current snapshot

# In[2]:


import datetime
import os
import shutil

import pandas as pd
from config import config
from download.box import LifespanBox

# In[3]:


verbose = True
snapshotdate = datetime.datetime.today().strftime('%Y-%m-%d')
cache_space = config['dirs']['cache']['qint']
store_space = config['dirs']['store']['qint']

processed_filename = os.path.join(store_space,
                                  'ProcessedBoxFiles_AllRawData_Qinteractive.csv')
combined_filename = os.path.join(store_space, 'HCA-HCD_Allsites_QandRAVLT_%s.xlsx' % snapshotdate)
available_box_files = os.path.join(cache_space, 'AllBoxFiles_Qinteractive.csv')

# In[4]:


box = LifespanBox(cache=cache_space)

# In[5]:


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
specialfolders = {
    '47239506949',  # this is a subfolder within UMN HCA that has more Q data in individual
    '83367512058',  # this is subfolder ('UMNHCASUB') with rescored data from late July, 2019
}
bdas_folders = {75755393630: 'BDAS_HCD', 75755777913: 'BDAS_HCA'}
cleanestdata = '465568117756'
processfile_id = '462800613671'


# In[6]:


def getfiles(folders, recursively=True):
    result = {}

    for folder_id, label in folders.items():

        #         print('getting filenames of box folder ' + label)
        items = list(box.client.folder(folder_id).get_items())
        files = {i.id: i.name for i in items if i.type == 'file'}
        folders = {i.id: i.name for i in items if i.type == 'folder'}

        result.update(files)
        if recursively:
            result.update(getfiles(folders, True))

    return result


# ### Recursively scan box folders for list of all files

# In[7]:


files = getfiles(sites)
boxfiles_df = pd.DataFrame(files.items(), columns=['file_id', 'filename'])
boxfiles_df['source'] = 'box-site'

# In[8]:


bdasfiles = getfiles(bdas_folders)

bdasfiles_df = pd.DataFrame(bdasfiles.items(), columns=['file_id', 'filename'])
bdasfiles_df['source'] = 'BDAS'

# In[9]:


# put them together...note that as of 5/22/19, there are 559 HCA, 162 HCD, and 2 others who are only represetned in the folders 1.
# all other individuals in HCA and HCD have data in multiple places.
# remove the info files from the list,
# assign visit, and file type where it can be gleaned from filename


# In[10]:


files_df = pd.concat([boxfiles_df, bdasfiles_df], axis=0)
files_df = files_df[files_df.filename.str.endswith('.csv')]
files_df['subject'] = files_df.filename.str[:10]
files_df['visit'] = ''
files_df.loc[files_df.filename.str.contains('V1'), 'visit'] = 'V1'
files_df.loc[files_df.filename.str.contains('V2'), 'visit'] = 'V2'
files_df['assessment'] = ''
files_df.head()

is_IQ = files_df.filename.str.contains('Matrix Reasoning')
files_df.loc[(is_IQ & files_df.filename.str.contains(' 17', regex=False)), 'assessment'] = 'WAIS'
files_df.loc[(is_IQ & files_df.filename.str.contains('6-16', regex=False)), 'assessment'] = 'WISC'
files_df.loc[(is_IQ & files_df.filename.str.contains('5_scores', regex=False)), 'assessment'] = 'WPPSI'
files_df.loc[files_df.filename.str.contains('Aging_scores'), 'assessment'] = 'RAVLT'
files_df.loc[files_df.filename.str.contains('RAVLT V2'), 'assessment'] = 'RAVLT2'

files_df.file_id = files_df.file_id.astype(int)
dupfilenames = pd.concat(g for _, g in files_df.groupby(
    "filename") if len(g) > 1)  # with duplicate filenames

# In[11]:


fileio = box.readFile(processfile_id)
processed = pd.read_csv(fileio, encoding='utf8')
processed_df_subset = processed[['file_id', 'raw_processed_date']].copy()

# In[12]:


files4process = files_df.merge(processed_df_subset, how='left', on='file_id')
files4process = files4process.loc[files4process.raw_processed_date.isnull()]

# In[13]:


# download everything not already processed to cache - originally there
box.download_files(files4process.file_id)


# In[14]:


def find_assessment_type(subset, search_string):
    found = set()

    for filename in set(subset.filename):
        fullpath = os.path.join(cache_space, filename)
        with open(fullpath, 'r', encoding='utf-16') as fd:
            if search_string in fd.read():
                found.add(filename)

    found = pd.DataFrame(found, columns=['filename'])
    found['assessment'] = search_string

    return found


# In[15]:


# for filenames with pattern in their title, file type is easy.
# determine filetype for others by looking for pattern in actual
# downloaded file
subset1 = files4process[files4process.assessment == '']

# In[16]:


foundlist = pd.concat([
    find_assessment_type(subset1, 'RAVLT'),
    find_assessment_type(subset1, 'WAIS'),
    find_assessment_type(subset1, 'WISC'),
    find_assessment_type(subset1, 'WPPSI'),
])

# In[17]:


if not foundlist.empty:
    files4process = files4process.merge(foundlist, 'left', on='filename', suffixes=('', '_y'))
    files4process.assessment = files4process.assessment.mask(files4process.assessment.isna(),
                                                             files4process.assessment_y)
    files4process.drop(columns=['assessment_y'], inplace=True)

# In[18]:


# unused checks for duplicates base on columns such as subject and filename
df = files4process.copy()
singles = df[df.duplicated('subject', keep=False) == False]  # good
duplicates = df[df.duplicated('subject', keep=False) == True]  # bad
dupfilenames = df[df.duplicated('filename', keep=False) == True]

# In[19]:


# make rows of all these files, where possible because assessment is specified
# then turn the entire thing into a csv file and save in store and in box
# as a 'raw data' snapshot

###################################################################
# if files4process isnt empty then proceed with an update otherwise stop here:


# In[20]:


files4process.reset_index(drop=True, inplace=True)


# In[21]:


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


# In[22]:


new_rows = []

for filename, assessment in files4process.loc[:, ['filename', 'assessment']].itertuples(False):
    try:
        rows, labels = create_row(cache_space, filename, assessment)
    except Exception:
        rows = ''
    new_rows.append(rows)

# In[23]:


# merge these with dataframe
files4process['row'] = new_rows
files4process['raw_processed_date'] = snapshotdate

# In[87]:


# sub4.to_csv(processed_file,index=False) - original initialization of processed file required 'to_csv'
# cat these to the processed file

newprocessed = pd.concat([processed, files4process], axis=0, sort=True, )

newprocessed.to_csv(processed_filename, index=False)

# this is in the behavioral data/snapshots/Q/raw_allfiles_in_box/ folder...is not the curated BDAS file - just keeping a record of the raw unprocessed download
# box.upload_file(processed_file,76432368853) first run had to upload file
# - subsequent runs just update
box.update_file(processfile_id, processed_filename)

# In[78]:


shutil.rmtree(box.cache)

# In[ ]:


# In[99]:


cleaned = pd.read_excel(box.readFile(cleanestdata))

# In[212]:


combined = pd.concat([cleaned, files4process], sort=False, ignore_index=True)

# In[180]:


e = []
for nid, x in combined.groupby('subject'):
    if x.select_4clean.isna().any():
        x = x.sort_values('visit', ascending=False)
        keep = (~x.duplicated('row', keep='first')).astype(int)
        x.loc[x.select_4clean.isna(), 'select_4clean'] = keep
        e.append(x)
e = pd.concat(e)

# In[183]:


combined.update(e)

# In[267]:


combined.to_excel(combined_filename, index=False)

# In[268]:


box.update_file(cleanestdata, combined_filename)

# In[ ]:




