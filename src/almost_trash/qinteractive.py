import os
import sys
import shutil

import pandas

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


assessments = {
    'RAVLT': {
        'pattern': '-Aging_scores.csv',
        'combined_file_id': 287508176642
    },
    'WAIS': {
        'pattern': 'Matrix Reasoning*17+_scores.csv',
        'combined_file_id': 287501812244
    },
    'WISC': {
        'pattern': 'Matrix Reasoning*6-16_scores.csv',
        'combined_file_id': 287314850009
    },
    'WPPSI': {
        'pattern': 'Matrix Reasoning*5_scores.csv',
        'combined_file_id': 287503839413
    }
}

box = LifespanBox(cache=cache_space)

# config = ClientConfiguration()
# validation = Validation(None, config=config)


def main():
    # Get filenames that have already been combined
    existing_files = already_processed()
    print('existing_files:\n' + existing_files)

    for i in assessments:
        # Download output for each site
        pattern = assessments[i]['pattern']
        # results = box.search(pattern=pattern, limit=100, maxresults=2000)
        results = box.search(pattern=pattern, limit=1, maxresults=1)
        # for r in results:
        #     print(r)
        print('^ {} results for {}\n'.format(len(results), pattern))

        # Download all new files from Box that need combined
        new_files = download_new_outputs(results, existing_files)
        # Don't do anything if no new data
        if not new_files:
            print('Nothing new to add for {}. Continuing...'.format(i))
            continue

        print(new_files)
        # sys.exit()

        # Write new output to file and upload to Box
        append_new_data(new_files, assessments[i])

    # Clean up cache space
    # shutil.rmtree(box.cache)


def already_processed():
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
        new_rows.append(create_row(filename))

    print('{} new rows'.format(len(new_rows)))

    # Download the combined file
    # NOTE, assuming that the file exists, and for the first run, this would
    # be the csv header with no associated data
    combined_file_id = assessment['combined_file_id']
    box.download_file(combined_file_id)

    # Write all the new rows to combined file
    f = box.client.file(file_id=combined_file_id)
    combined_file_path = os.path.join(box.cache, f.get()['name'])

    with open(combined_file_path, 'a') as f:
        for row in new_rows:
            f.write(row + '\n')

    # sys.exit()

    # Put the file back in Box as a new version
    # f = box.client.file(str(combined_file_id))
    # f.update_contents(combined_file_path)
    # box.update_file(combined_file_id, combined_filename)


def create_row(filename):
    """
    Generates a single row per subject for a given output.
    Look for unique rows in the file that indicate data points,
    then capture subsequent rows from file until a newline.
    """
    section_headers = [
        'Subtest,,Raw score\n',
        'Subtest,,Scaled score\n',
        'Subtest,,Completion Time (seconds)\n',
        'Subtest,Type,Yes/No\n',
        'Item,,Raw score\n',
        # 'Scoring Type,,Scores\n'
    ]
    # Last section header is repeat data except for RAVLT
    if 'Aging' in filename:
        section_headers.append('Scoring Type,,Scores\n')

    subject_id = filename.split('_')[0]
    new_row = subject_id

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
            # if value == '-':
            #     value = ''
            new_row += ',' + value.strip()

    # print(new_row)
    print('Finished processing {}.'.format(filename))
    # sys.exit()

    # Save this file to already processed store
    with open(processed_file, 'a') as store:
        store.write(filename + '\n')

    return new_row


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
