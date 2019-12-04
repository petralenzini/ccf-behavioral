import os
import csv
import pandas as pd
from download.box import LifespanBox

root_cache = '/data/intradb/tmp/box2nda_cache/'
cache_space = os.path.join(root_cache, 'ksads')
box = LifespanBox(cache=cache_space)
ksadsboxfolderid = 48203202724

# get one of the identical key files which contain the labels for the
# numbered questions for all KSADS assessments
keyfileid = box.search(pattern='*Key')
box.download_file(keyfileid[0].id)
qkey = keyfileid[0].name
fileq = os.path.join(cache_space, qkey)

# specify particular KSADS assessment to be tweaked (e.g. intro, screening, suppliement)
#sheetname in key
# sheet='intro'
# combined file
# comborawfile='KSADS_Intro_Combined'
sheet = 'Screener'
# combined file
comborawfile = 'KSADS_Screener_Combined'
sheet = 'Supplement'
comborawfile = 'KSADS_Supplement_Combined'

# caputure labels for the vars in this assessment from the key
keyasrow = pd.read_excel(fileq, sheet_name=sheet, header=0)
varlabels = keyasrow.transpose().reset_index().rename(
    columns={'index': 'variable', 0: 'question_label'})
varlabels['variable'] = varlabels['variable'].apply(str)

# simplify the combined file by removing empty columns
# def make_slim(comborawfile,sheet,outslim,outdatadict)
filename = os.path.join(cache_space, comborawfile + '.csv')
ksadsraw = pd.read_csv(filename, header=0, low_memory=False)
# remove columns that are completely empty
ksadsraw = ksadsraw.dropna(axis=1, how='all')
# ksadsraw.describe(include='all')
# push this back to box
fileout = os.path.join(cache_space, comborawfile + "_Slim.csv")
ksadsraw.to_csv(fileout, index=False)
box.client.folder(str(ksadsboxfolderid)).upload(fileout)


# make humanreadable data dictionary that has all the columns necessary to
# map (by hand) to NDA;
varvalues = pd.DataFrame(
    columns=[
        'variable',
        'values_or_example',
        'numunique'])
varvalues['variable'] = ksadsraw.columns
kcounts = ksadsraw.count().reset_index().rename(
    columns={'index': 'variable', 0: 'num_nonmissing'})
varvalues = pd.merge(varvalues, kcounts, on='variable', how='inner')

# if number of unique values in a given column is less than 10 (and thats
# not just because the question was only answered 10 times, print out the
# list ov values...otherwise, print out the first value as an example of
# the column contents;
for item in ksadsraw.columns:
    row = ksadsraw.groupby(item).count().reset_index()[item]
    varvalues.loc[varvalues.variable == item, 'numunique'] = len(
        row)  # number of unique items in this column
    varvalues.loc[(varvalues.variable == item) & (varvalues.numunique <= 10) & (
        varvalues.num_nonmissing >= 10), 'values_or_example'] = ''.join(str(ksadsraw[item].unique().tolist()))
    varvalues.loc[(varvalues.variable == item) & (varvalues.numunique <= 10) & (
        varvalues.num_nonmissing < 10), 'values_or_example'] = ksadsraw[item].unique().tolist()[1]
    varvalues.loc[(varvalues.variable == item) & (varvalues.numunique > 10),
                  'values_or_example'] = ksadsraw[item].unique().tolist()[1]

# now merge labels for the informative variables from cache
varvalues2 = pd.merge(varvalues, varlabels, on='variable', how='left')
varvalues2 = varvalues2[['variable',
                         'question_label',
                         'values_or_example',
                         'numunique',
                         'num_nonmissing']].copy()
# push this back to box
fileoutdict = os.path.join(cache_space, comborawfile + "_DataDictionary.csv")
varvalues2.to_csv(fileoutdict, index=False)
box.client.folder(str(ksadsboxfolderid)).upload(fileoutdict)
