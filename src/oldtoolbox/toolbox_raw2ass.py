import os
import datetime
import csv
import pycurl
import sys
import shutil
from openpyxl import load_workbook
import pandas as pd
import download.box

from download.box import LifespanBox

verbose = True
#verbose = False
snapshotdate = datetime.datetime.today().strftime('%m_%d_%Y')

root_cache = '/data/intradb/tmp/box2nda_cache/'
cache_space = os.path.join(root_cache, 'endpointbak2109_0620')
try:
    os.mkdir(cache_space)
except BaseException:
    print("cache already exists")

# rsync
# petra@nrg-toolbox.nrg.mir:/var/www_nih_app_endpoint/html/projects/lifespan/2018*Scores*
# /data/intradb/tmp/box2nda_cache/endpointbak2109_0620/.

# connect to Box
box = LifespanBox(cache=cache_space)

# endpointfolder=42957934974
assessmentsfolder = 42902161768
files, folders = foldercontents(assessmentsfolder)
# Rawname
# 2019-04-28 11.15.14 Assessment Scores.csv_149.142.103.176_2019-04-28T13:15:22-05:00
# assname
# 2019-04-28_11_15_14_Assessment_Scores_149_142_103_176_2019-04-28T13_15_22-05_00.csv

files['filenamealphanum'] = files.filename.str.replace('.csv', '')
files['filenamealphanum'] = files.filenamealphanum.str.replace('_', '')
files['filenamealphanum'] = files.filenamealphanum.str.replace('.', '')
files['filenamealphanum'] = files.filenamealphanum.str.replace(':', '')
files['filenamealphanum'] = files.filenamealphanum.str.replace('-', '')
files['filenamealphanum'] = files.filenamealphanum.str.replace(' ', '')

baknames = pd.DataFrame(
    os.listdir(cache_space),
    columns={'RawEndpointFilename'})
baknames['filenamealphanum'] = baknames.RawEndpointFilename.str.replace(
    '.csv', '')
baknames['filenamealphanum'] = baknames.filenamealphanum.str.replace('_', '')
baknames['filenamealphanum'] = baknames.filenamealphanum.str.replace('.', '')
baknames['filenamealphanum'] = baknames.filenamealphanum.str.replace(':', '')
baknames['filenamealphanum'] = baknames.filenamealphanum.str.replace('-', '')
baknames['filenamealphanum'] = baknames.filenamealphanum.str.replace(' ', '')

overlap = pd.merge(files, baknames, on='filenamealphanum', how='right')
# copyfile(src,dst)


shutil.rmtree(box.cache)


def catcontents(files):  # dataframe that has filename and file_id as columns
    scoresfiles = files.copy()
    scoresinit = pd.DataFrame()
    for i in scoresfiles.filename:
        filepath = os.path.join(cache_space, i)
        filenum = scoresfiles.loc[scoresfiles.filename == i, 'file_id']
        try:
            temp = pd.read_csv(filepath, header=0, low_memory=False)
            temp['filename'] = i
            temp['file_id'] = pd.Series(
                int(filenum.values[0]), index=temp.index)
            temp['raw_cat_date'] = snapshotdate
            scoresinit = pd.concat([scoresinit, temp], axis=0, sort=False)
        except BaseException:
            print(filepath + ' wouldnt import')
            temp = pd.DataFrame()
            temp['filename'] = pd.Series(i, index=[0])
            temp['file_id'] = pd.Series(int(filenum.values[0]), index=[0])
            temp['raw_cat_date'] = snapshotdate
            scoresinit = pd.concat([scoresinit, temp], axis=0, sort=False)
    return scoresinit


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
