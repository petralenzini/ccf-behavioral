import os
import sys
import shutil

import pandas as pd

from download.box import LifespanBox
# from nda.validationtool import ClientConfiguration, Validation
#from nda import validation

"""
"""

verbose = True

root_cache = '/data/intradb/tmp/box2nda_cache/'
cache_space = os.path.join(root_cache, 'qinteractive')
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


processed_file = os.path.join(cache_space, 'Processed_Qinteractive.csv')
available_box_files = os.path.join(cache_space, 'AllBoxFiles_Qinteractive.csv')
pattern_exceptions = os.path.join(cache_space, 'Pattern Exceptions')
behavioral_folder_id = '18445162758'
assessments = {
    'RAVLT': {
        'pattern': '-Aging_scores',
        'combined_file_id': 287508176642
    },
    'RAVLT2': {
        'pattern': 'RAVLT V2_scores',
        'combined_file_id': 28750817662
    },
    'WAIS': {
        'pattern': 'Matrix Reasoning*17+_scores',
        'combined_file_id': 287501812244
    },
    'WISC': {
        'pattern': 'Matrix Reasoning*6-16_scores',
        'combined_file_id': 287314850009
    },
    'WPPSI': {
        'pattern': 'Matrix Reasoning*5_scores',
        'combined_file_id': 287503839413
    }
}

# generate the box object which contains the necessary client config to
# talks to box, and sets up the cache space
box = LifespanBox(cache=cache_space)

# validation = Validation(None, config=config)

#########################################################this section will
##########################################necessary to search folders with
# generate list of all files in Q directories and identify those that dont
# follow pattern
sitefolderslabels = [
    "WUHCD",
    "WUHCA",
    "UMNHCD",
    "UMNHCA",
    "UCLAHCD",
    "UCLAHCA",
    "HARVHCD",
    "MGHHCA"]  # ,"UMNHCASUB"]
sitefolderlist = [
    18446355408,
    18446433567,
    18446318727,
    18446298983,
    18446352116,
    18446404271,
    18446321439,
    18446404071]  # ,47239506949]
# this is a subfolder within UMN HCA that has more Q data in individual
# subject folders
specialfolder = 47239506949
speciallabel = "UMNHCASUB"

# get folder contents for all the sites including the known subfolder of individuals folders
# folderlistcontents generates two dfs: a df with names and ids of files
# and a df with names and ids of folders
superfilelist, superfolderlist = folderlistcontents(
    sitefolderslabels, sitefolderlist)  # 2378 files and 1 folders as of 5/22/2019
if (superfolderlist.shape[0] == 1) & (
        superfolderlist.folder_id[0] == str(specialfolder)):
    print('found expected subfolder ', superfolderlist.folder_id[0])
else:
    print('well, hello, new folders whose contents may or may not be captured')
    superfolderlist

# known subfolder contents -reps Cindy's initial download to UMN site
# folder - contents of this special folder is a list of other  individual
# folder_ids, and 2 files...go in them and grab files
# grabbing df of names and id of subfolders in specialfolder
specialsubfilelist, specialsubfolderlist = foldercontents(specialfolder)
superfilelist = superfilelist.append(
    specialsubfilelist)  # there were two extra files here
# now get foldercontents
specialsuperfilelist, specialsuperfolderlist = folderlistcontents(
    specialsubfolderlist.foldername, specialsubfolderlist.folder_id)
# add the subfolder files to list of files that should be processed
superfilelist = superfilelist.append(
    specialsuperfilelist)  # now have 2678 (may 22)

# specialsuperfolderlist has two more folders that we didnt know about
# before running this excercise - if future runs of this code identify any
# more, we will be informed here
if (
    specialsuperfolderlist.shape[0] == 2) & (
        specialsuperfolderlist.reset_index().folder_id[0] == '43861663941') & (
            specialsuperfolderlist.reset_index().folder_id[1] == '43241473238'):
    print('found expected sub-subfolders ', specialsuperfolderlist)
else:
    print(
        'hello, new folders whose contents may or may not be captured',
        specialsuperfolderlist)

# known sub-sub folder contents
specialsuperfolderlist.reset_index(inplace=True)
subspecialsuperfilelist, subspecialsuperfolderlist = folderlistcontents(
    specialsuperfolderlist.foldername, specialsuperfolderlist.folder_id)  # 4 more files - no more folders - as of 5/22/2019

# add the specialfolder-s subfolder-s two subfolders to the final list of
# files that should be processed
superfilelist = superfilelist.append(
    subspecialsuperfilelist)  # 2682 files as of 5/22/19
# post processing of dataframe of box-site files...
superfilelist['source'] = 'box-site'
superfilelist['visit'] = ''
superfilelist['subject'] = superfilelist.filename.str[:10]
# filenames that dont begin with H wont produce subject ids that begin
# with H...i.e. they dont follow naming convention norm...
superfilelist.loc[~(superfilelist.subject.str[:1] == 'H')]
# one was in the UCLA aging folder (9584097_5_). the other was in box-site
# folder but checklist said to find data in BDAS.


###############now find all the files in the BDAS folders#################
flabels = ["BDAS_HCD", "BDAS_HCA"]
flist = [75755393630, 75755777913]
bdasfilelist, bdasfolderlist = folderlistcontents(
    flabels, flist)  # 1455 files and 0 folders as of 5/22/2019

# post processing of dataframe of BDAS files...
bdasfilelist['source'] = 'BDAS'
bdasfilelist['visit'] = ''
bdasfilelist['subject'] = bdasfilelist.filename.str[:10]
# filenames that dont follow norm...just drop .DS_store
bdasfilelist.loc[~(bdasfilelist.subject.str[:1] == 'H')]


# put them together...note that as of 5/22/19, there are 559 HCA, 162 HCD, and 2 others who are only represetned in the folders 1.
# all other individuals in HCA and HCD have data all over the place.
allboxfiles = pd.concat([superfilelist, bdasfilelist], axis=0)
allboxfiles = allboxfiles.loc[~allboxfiles.filename.str.contains(
    'information.txt')].copy()
allboxfiles = allboxfiles.loc[~allboxfiles.filename.str.contains(
    '.DS_Store')].copy()
allboxfiles.loc[allboxfiles.filename.str.contains('V1'), 'visit'] = 'V1'
allboxfiles.loc[allboxfiles.filename.str.contains('V2'), 'visit'] = 'V2'
allboxfiles['assessment'] = ''
allboxfiles.loc[allboxfiles.filename.str.contains(
    'Aging_scores'), 'assessment'] = 'RAVLT'
allboxfiles.loc[allboxfiles.filename.str.contains(
    'RAVLT V2'), 'assessment'] = 'RAVLT2'
allboxfiles.loc[(allboxfiles.filename.str.contains('Matrix Reasoning 17') &
                 allboxfiles.filename.str.contains(' 17', regex=False)), 'assessment'] = 'WAIS'
allboxfiles.loc[(allboxfiles.filename.str.contains('Matrix Reasoning') &
                 allboxfiles.filename.str.contains('6-16', regex=False)), 'assessment'] = 'WISC'
allboxfiles.loc[(allboxfiles.filename.str.contains('Matrix Reasoning') &
                 allboxfiles.filename.str.contains('5_scores', regex=False)), 'assessment'] = 'WPPSI'

# compare allboxfiles to list in store of already processed files -- not done yet...
# can get date on these and download only new ones - not done yet..;

# download everything to cache - there should be
# len(allboxfiles.file_id)=3506 as of 5/22/19 minus 2 with duplicate
# filenames.
dupfilenames = pd.concat(g for _, g in allboxfiles.groupby(
    "filename") if len(g) > 1)  # 2 with duplicate filenames
# should end up with 3504 files plus whatever meta files I've made.
# --note if not exat, its okay.  processed file list will contain log of
# anything missed.
box.download_files(allboxfiles.file_id)


# for filenames with pattern in their title, file type is easy.
# determine filetype for others by looking for pattern?
# 1370 do not have assessment in title and need to be opened to check
subset1 = allboxfiles.loc[allboxfiles.assessment == '']

nfravlt, fndravlt = findasstype(subset1, search_string='RAVLT')
# go back and find the nfravlt files that werent downloaded properly.
getlist = pd.merge(subset1, nfravlt, on='filename', how='inner')
box.download_files(getlist.file_id)
# now repeat the findasstype call
nfravlt, fndravlt = findasstype(subset1, search_string='RAVLT')
# only one left - reps a file that needs to be re-saved HCD0040113V1.csv
# cant be read...file looks like it was saved in different format then
# converted
nfwais, fndwais = findasstype(subset1, search_string='WAIS')
nfwisc, fndwisc = findasstype(subset1, search_string='WISC')
nfwppsi, fndwppsi = findasstype(subset1, search_string='WPPSI')

foundlist = pd.concat([fndravlt, fndwais, fndwisc, fndwppsi], axis=0)
allboxfiles = pd.merge(foundlist, allboxfiles, on='filename', how='right')
allboxfiles.loc[allboxfiles.assessment_x.isnull() == False,
                'assessment'] = allboxfiles.assessment_x
allboxfiles.loc[allboxfiles.assessment_x.isnull(),
                'assessment'] = allboxfiles.assessment_y
allboxfiles = allboxfiles.drop(columns=['assessment_x', 'assessment_y']).copy()

###########################STOPPED HERE###################################
##########################################################################

allboxfiles.loc[~(allboxfiles.files.str.contains('txt'))].sort_values(
    'subject')  # find the people who have data in both locations
allboxnotxt = allboxfiles.loc[~(allboxfiles.files.str.contains('txt'))]
singles = pd.concat(
    g for _,
    g in allboxnotxt.groupby("subject") if len(g) == 1)
duplicates = pd.concat(
    g for _,
    g in allboxnotxt.groupby("subject") if len(g) > 1)
dupfilenames = pd.concat(g for _, g in allboxfiles.groupby(
    "files") if len(g) > 1)  # some even have exactly the same filenames


# this is just a list of files...not file_ids or folder_ids
allboxfiles.to_csv(available_box_files, index=False)

# determind file type and make rows.


sub1 = allboxfiles.loc[~(allboxfiles.files.str.contains('Aging'))]
sub2 = sub1.loc[~(sub1.files.str.contains('RAVLT'))]
sub3 = sub2.loc[~(sub2.files.str.contains('Matrix'))]
sub4 = sub3.loc[~(sub3.source.str.contains('BDAS'))]
# at time of this check, there were 12 exceptions to pattern search in the site folders (see assessments definition above).
# define function to identify their type

intialize the processed file list by processing these exceptions.
# check
if len(sub3.files) > 12:
    print('Found new search pattern exceptions')

# add relevant info to the exceptions
sub4 = sub3.files.reset_index().copy()
sub4 = sub4.drop(columns=['index'])
nopatternfile_ids = [
    416895376935,
    432845377622,
    416889427467,
    432825497629,
    432848331339,
    432818808650,
    432822051979,
    416853666946,
    416918214720,
    432853459166,
    432820134098,
    432805484048]
assess = [
    'WISC',
    'WISC',
    'WAIS',
    'WISC',
    'WISC',
    'WISC',
    'WISC',
    'WAIS',
    'WISC',
    'WISC',
    'WISC',
    'WISC']
sub4['nopatternfile_ids'] = nopatternfile_ids
sub4['assessment'] = assess
sub4.to_csv(pattern_exceptions, index=False)

# downlaod these exceptions and initialize processed file list and data
# for combined file
nopattern_rows = []
for i in range(len(sub4.nopatternfile_ids)):
    box.download_file(str(sub4.nopatternfile_ids[i]))
    # returns row=string of comma sep values in the file, and labels=list of
    # the labels associated with each value (to double check that no WISCS got
    # renamed as WAIS, etc)
    rows, labels = create_row(sub4.files[i])
    nopattern_rows.append(rows)

sub4['data'] = nopattern_rows

# now look for csv vs xls exceptions
subx1 = df.loc[~(df.files.str.contains('csv'))]
subx2 = subx1.loc[~(subx1.files.str.contains('txt'))].copy()
# one csv saved as xls had issues beyond format opening - this was WAIS data- fixed by Tymber 5/3/2019 who re-upoaded as csv
# the following code is no longer needed
# box.download_file(str(444746828166)) #file handle for xls - will download but not read...
# subx2['nopatternfile_ids']=444746828166
# subx2['assessment']="WAIS"
# until resolved, append row by hand
# subx2['data']='HCD1988687_V1,17,9,0,0,0,347.68,Yes,No,C,C,1,1,1,1,1,1,1,1,1,1,0,1,1,1,1,1,1,1,0,0,0,,,,,'
# subx2=subx2.reset_index().drop(columns=['index']).copy()
# sub5=pd.concat([sub4,subx2],axis=0,sort=False).reset_index().drop(columns=['index'])

sub5 = sub4

# append to file that contains list of processed files
with open(processed_file, 'a') as f:
    for row in sub5.files:
        f.write(row + '\n')

# HCD0009422V1.csv,416895376935,WISC
# HCD0148537V2.csv,432845377622,WISC
# HCD0584860V1.csv,416889427467,WAIS
# HCD1852056V2.csv,432825497629,WISC
# HCD1856468V2.csv,432848331339,WISC
# HCD1880162V2.csv,432818808650,WISC
# HCD1943867V2.csv,432822051979,WISC
# HCD2059245V1.csv,416853666946,WAIS
# HCD2082341V1.csv,416918214720,WISC
# HCD2240030V2.csv,432853459166,WISC
# HCD2711649V2.csv,432820134098,WISC
# HCD2757370V2.csv,432805484048,WISC

# append to combined file which has a header
# first WAIS
boxcombined = box.client.file(file_id=assessments['WAIS']['combined_file_id'])
box.download_file(assessments['WAIS']['combined_file_id'])
# now the current combined file is in the cache with the name cachecombined
cachecombined = os.path.join(box.cache, boxcombined.get()['name'])
# append these specialcase rows to the combined files
with open(cachecombined, 'a') as f:
    for row in sub5.loc[sub5.assessment == 'WAIS'].data:
        f.write(row + '\n')

boxcombined.update_contents(cachecombined)

# now WISC
boxcombined = box.client.file(file_id=assessments['WISC']['combined_file_id'])
box.download_file(assessments['WISC']['combined_file_id'])
# now the current combined file is in the cache with the name cachecombined
cachecombined = os.path.join(box.cache, boxcombined.get()['name'])
# append these specialcase rows to the combined files
with open(cachecombined, 'a') as f:
    for row in sub5.loc[sub5.assessment == 'WISC'].data:
        f.write(row + '\n')

boxcombined.update_contents(cachecombined)

# now that all the special cases are done...we can move onto automated part


def main():
    # Get filenames that have already been combined
    existing_files = already_processed(processed_file)
    print('existing_files:\n' + existing_files)

    for i in assessments:
        # Download output for each site
        pattern = assessments[i]['pattern']
        # limit and max results did nothing to limit the 205 results ...205
        # included the trashed xls file
        results = box.search(pattern=pattern, limit=100, maxresults=2000)
        #results2 = box.search(pattern=pattern, limit=1, maxresults=1)
        #results3 = box.search(pattern=pattern, exclude='information',ancestor_folders=[box.client.folder(behavioral_folder_id)],file_extensions=['csv'])

        for r in results:
            print(r)
        print('^ {} results for {}\n'.format(len(results), pattern))

        # Download all new files from Box that need combined
        # new_file_names doesnt account for issue of box not properly
        # downloading...what to do?

        new_files = download_new_outputs(results, existing_files)
        # Don't do anything if no new data
        if not new_files:
            print('Nothing new to add for {}. Continuing...'.format(i))
            continue

        # print(new_files)
        # sys.exit()
        # remove duplicates
        new_files = list(set(new_files))
        # Write new output to file and upload to Box
        append_new_data(new_files, assessments[i])


# processed file updated -- check
existing_files2 = already_processed(processed_file)
print('existing_files2:\n' + existing_files2)

# update list of files processed
# check that all of the files in new files are in the new combined file...
# how is it possible that there are 207 rows of data (as expected if you
# find 205 and had started with 2) but only 204 matching the 'matrix'
# pattern in the cache, on one of them is the xls from trash?
# DUPLICATES...REMOVE 2 DUPLICATES - should only be 203 +2 previous = 205
# rows of data.

# Clean up cache space
# shutil.rmtree(box.cache)


def findasstype(subset, search_string='RAVLT'):
    found = []
    notfound = []
    for f in subset.filename:
        absfile = os.path.join(cache_space, f)
        try:
            for line in open(absfile, encoding='utf-16'):
                if search_string in line:
                    found.append(f)
                    # pass
                    #print(search_string+' in '+f)
                    # subset.loc[subset.filename==f,'assessment']=search_string
        except BaseException:
            # pass
            #print('file corrupt or doesnt exist-try download again...')
            notfound.append(f)
            # print(f) #box.download_file(str(allboxfiles.file_id[i])
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

# files,folders=foldercontents(75755393630)


def already_processed(processed_file):
    """
    Get a list of existing combined files so we can skip them
    """
    if not os.path.isfile(processed_file):
        # Create the file in the store if it doesn't exist
        with open(processed_file, 'w'):
            pass
    with open(processed_file) as f:
        return f.read()


def download_new_outputs(box_files, existing_files):
    """
    Builds a list of Box files not already processed from the store,
    downloads them to cache space, and returns a list of filenames
    :box_files - List of Box file objects
    :existing_files - String containing already processed files
    """
    new_file_ids = []
    new_file_names = []
    for box_file in box_files:
        if str(box_file.name) not in existing_files:
            print('Adding ' + box_file.name)
            new_file_ids.append(box_file.id)
            new_file_names.append(box_file.name)
    box.download_files(new_file_ids)
    # new_file_names doesnt account for issue of box not properly
    # downloading...what to do?
    return new_file_names


def append_new_data(new_files, assessment):
    """
    Get rows for all files not already processed in the cache
    Download the combined file from Box, write new rows, and upload to Box
    :new_files - List of filenames to add to combined file
    :assessment - Object containing the combined file id on Box
    """
    print('Adding ' + str(len(new_files)) + ' files')
    new_rows = []
    for filename in new_files:
        # processed file is updated when create new row is excecuted
        row, label = create_row(filename)
        new_rows.append(row)
    print('{} new rows'.format(len(new_rows)))
    # Download the current combined file
    # NOTE, assuming that the file exists, and for the first run, this would
    # be the csv header with no associated data
    combined_file_id = assessment['combined_file_id']  # boxcombined
    boxcombined = box.client.file(file_id=combined_file_id)
    box.download_file(combined_file_id)
    # now the current combined file is in the cache with the name
    # cachecombined as follows
    cachecombined = os.path.join(box.cache, boxcombined.get()['name'])
    with open(cachecombined, 'a') as f:
        for row in new_rows:
            f.write(row + '\n')
    boxcombined.update_contents(cachecombined)
    return new_rows


def create_row(filename):
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
    if ('Aging' in filename) or ('RAVLT V2') in filename:
        section_headers.append('Scoring Type,,Scores\n')
    subject_id = filename.split('_')[0]
    # this will have to be furthur cleaned for exceptions that dont have
    # underscore in file name
    subject_id = subject_id.split('.csv')[0]
    new_row = subject_id
    new_label = 'src_subject_id'
    path = os.path.join(cache_space, filename)
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
    with open(processed_file, 'a') as store:
        store.write(filename + '\n')
    return new_row, new_label


def validate():
    # Get combined file from Box
    box_file = box.download_file(287314850009)
    combined_path = os.path.join(box.cache, box_file.get().name)

    # Remove columns not in NDA from combined file
    df = pandas.read_csv(combined_path)
    drop_columns = ['matrix_completion', 'discontinue', 'reverse']

    for col in drop_columns:
        del df[col]

    df.to_csv(combined_path, index=False, header=True)

    # Prepend all required subject level columns
    new_header = ''
    new_rows = []

    with open(combined_path) as f:
        header = f.readline()
        new_header = \
            'subjectkey,interview_age,interview_date,gender,' + header

        for row in f.readlines():
            subject = row.split(',')[0]
            guid = get_guid(subject)
            demo = get_demographics(subject)

            new_row = '{},{},"{}",{},{}'.format(
                guid['subjectkey'],
                demo['interview_age'],
                demo['interview_date'],
                demo['gender'],
                row
            )
            new_rows.append(new_row)

    # Write new data back to a csv
    out_file = box_file.get().name.split('.')[0] + '_Output.csv'
    out_path = os.path.join(box.cache, out_file)

    with open(out_path, 'w') as f:
        # Add data type and version as header
        f.write('"wisc_v","01"\n')
        f.write(new_header)

        for row in new_rows:
            f.write(row)

    # Call validation service
    # validation.file_list = [out_path]
    # validation.validate()
    # nda.output()
    validation.submit_csv(out_path)


def get_guid(subject):
    print('looking up GUID for {}'.format(subject))
    guid = {
        'subjectkey': 'NDAR_INVGT203YG5'
    }
    return guid


def get_demographics(subject):
    demo = {
        'interview_age': 0,
        'interview_date': '01/01/2018',
        'gender': 'F'
    }
    return demo


if __name__ == '__main__':
    main()
