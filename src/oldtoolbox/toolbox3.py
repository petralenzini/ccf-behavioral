import os
import sys
import shutil
import pandas
from download.box import LifespanBox

"""
"""

verbose = True

root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
cache_space = os.path.join(root_dir, 'cache', 'toolbox')
combined_path = os.path.join(cache_space, 'Toolbox_Combined.csv')
toolbox_folder_id = 42902161768
box = LifespanBox(cache=cache_space)

# label_errors = []
# instrument_errors = []

# hca_path = os.path.join(root_dir, 'store', 'toolbox-hca-instruments.txt')
# with open(hca_path) as f:
#     hca_instruments = f.read().splitlines()

# hcd_path = os.path.join(root_dir, 'store', 'toolbox-hcd-instruments.txt')
# with open(hcd_path) as f:
#     hcd_instruments = f.read().splitlines()

# par_path = os.path.join(root_dir, 'store', 'toolbox-parent-instruments.txt')
# with open(par_path) as f:
#     par_instruments = f.read().splitlines()

# print(hcd_instruments)
# sys.exit()

instrument_map = {
    # Used in ABCD
    "NIH Toolbox Picture Vocabulary Test Age 3+ v2.0": "nihtbx_picvocab_",
    "NIH Toolbox Flanker Inhibitory Control and Attention Test Ages 8-11 v2.0": "nihtbx_flanker_",
    "NIH Toolbox Flanker Inhibitory Control and Attention Test Ages 8-11 v2.1": "nihtbx_flanker_",
    "NIH Toolbox List Sorting Working Memory Test Age 7+ v2.0": "nihtbx_list_",
    "NIH Toolbox List Sorting Working Memory Test Age 7+ v2.1": "nihtbx_list_",
    "NIH Toolbox Dimensional Change Card Sort Test Ages 8-11 v2.0": "nihtbx_cardsort_",
    "NIH Toolbox Dimensional Change Card Sort Test Ages 8-11 v2.1": "nihtbx_cardsort_",
    "NIH Toolbox Pattern Comparison Processing Speed Test Age 7+ v2.0": "nihtbx_pattern_",
    "NIH Toolbox Pattern Comparison Processing Speed Test Age 7+ v2.1": "nihtbx_pattern_",
    "NIH Toolbox Picture Sequence Memory Test Age 8+ Form A v2.0": "nihtbx_picture_",
    "NIH Toolbox Picture Sequence Memory Test Age 8+ Form A v2.1": "nihtbx_picture_",
    "NIH Toolbox Oral Reading Recognition Test Age 3+ v2.0": "nihtbx_reading_",
    "Cognition Fluid Composite": "nihtbx_fluidcomp_",
    "Cognition Fluid Composite v1.1": "nihtbx_fluidcomp_",
    "Cognition Crystallized Composite": "nihtbx_cryst_",
    "Cognition Crystallized Composite v1.1": "nihtbx_cryst_",
    "Cognition Total Composite Score": "nihtbx_totalcomp_",
    "Cognition Total Composite Score v1.1": "nihtbx_totalcomp_",
    "Cognition Early Childhood Composite": "nihtbx_earlycomp_",
    "Cognition Early Childhood Composite v1.1": "nihtbx_earlycomp_",
    # New for Lifespan
    "NIH Toolbox Grip Strength Test Age 3+ v2.0": "nihtbx_grip_",
    "Social Satisfaction Summary (18+)": "nihtbx_socialsat_",
    "Social Satisfaction Summary (13-17)": "nihtbx_socialsat_",
    "Social Satisfaction Summary (8-12)": "nihtbx_socialsat_",
    "NIH Toolbox Self-Efficacy CAT Ages 13-17 v2.0": "nihtbx_selfeff_",
    "NIH Toolbox Self-Efficacy CAT Age 18+ v2.0": "nihtbx_selfeff_",
    "NIH Toolbox Self-Efficacy CAT Ages 8-12 v2.0": "nihtbx_selfeff_",
    "NIH Toolbox Self-Efficacy Parent Report CAT Ages 8-12 v2.0": "nihtbx_selfeff_par_",
    "Negative Affect Summary (8-12)": "nihtbx_negaffect_",
    "Negative Affect Summary (18+)": "nihtbx_negaffect_",
    "Negative Affect Summary (13-17)": "nihtbx_negaffect_",
    "NIH Toolbox Dimensional Change Card Sort Test Age 12+ v2.1": "nihtbx_cardsort_",
    "NIH Toolbox Visual Acuity Test Age 8+ v2.0": "nihtbx_visualacuity_",
    "NIH Toolbox Visual Acuity Test Ages 3-7 v2.0": "nihtbx_visualacuity_",
    "NIH Toolbox Visual Acuity Practice Ages 3-7 v2.0": "nihtbx_visualacuity_",
    "NIH Toolbox Visual Acuity Practice Age 8+ v2.0": "nihtbx_visualacuity_",
    "NIH Toolbox Words-In-Noise Test Age 6+ v2.1": "nihtbx_wordsnoise_",
    "NIH Toolbox Picture Sequence Memory Test Ages 3-4 Form A v2.1": "nihtbx_picture_",
    "NIH Toolbox Picture Sequence Memory Test Age 7 Form A v2.1": "nihtbx_picture_",
    "NIH Toolbox Picture Sequence Memory Test Ages 5-6 Form A v2.1": "nihtbx_picture_",
    "NIH Toolbox Picture Sequence Memory Test Age 8+ Form B v2.1": "nihtbx_picture_",
    "NIH Toolbox Picture Sequence Memory Test Age 8+ Form B v2.0": "nihtbx_picture_",
    "NIH Toolbox Picture Sequence Memory Test Age 8+ Form C v2.1": "nihtbx_picture_",
    "NIH Toolbox Loneliness FF Ages 8-17 v2.0": "nihtbx_lonely_",
    "NIH Toolbox Loneliness FF Age 18+ v2.0": "nihtbx_lonely_",
    "NIH Toolbox Positive Affect FF Ages 8-12 v2.0": "nihtbx_posaffect_",
    "NIH Toolbox Positive Affect CAT Age 18+ v2.0": "nihtbx_posaffect_",
    "NIH Toolbox Positive Affect CAT Ages 13-17 v2.0": "nihtbx_posaffect_",
    "NIH Toolbox Positive Affect FF Ages 13-17 v2.0": "nihtbx_posaffect_",
    "NIH Toolbox Positive Affect Parent Report CAT Ages 8-12 v2.0": "nihtbx_posaffect_par_",
    "NIH Toolbox Positive Affect Parent Report CAT Ages 3-7 v2.0": "nihtbx_posaffect_par_",
    "NIH Toolbox Perceived Stress FF Age 18+ v2.0": "nihtbx_stress_",
    "NIH Toolbox Perceived Stress FF Ages 13-17 v2.0": "nihtbx_stress_",
    "NIH Toolbox Perceived Stress Parent Report CAT Ages 8-12 v2.0": "nihtbx_stress_par_",
    # "NIH Toolbox Regional Taste Test 12+ v2.0": "nihtbx_taste_",
    "NIH Toolbox Sadness CAT Age 18+ v2.0": "nihtbx_sadness_",
    "NIH Toolbox Sadness FF Ages 8-17 v2.0": "nihtbx_sadness_",
    "NIH Toolbox Sadness Parent Report FF Ages 3-7 v2.0": "nihtbx_sadness_par_",
    "NIH Toolbox Sadness Parent Report CAT Ages 8-12 v2.0": "nihtbx_sadness_par_",
    "NIH Toolbox 4-Meter Walk Gait Speed Test Age 7+ v2.0": "nihtbx_gaitspeed_",
    "NIH Toolbox General Life Satisfaction CAT Ages 13-17 v2.0": "nihtbx_lifesat_",
    "NIH Toolbox General Life Satisfaction CAT Age 18+ v2.0": "nihtbx_lifesat_",
    "NIH Toolbox General Life Satisfaction FF Ages 8-12 v2.0": "nihtbx_lifesat_",
    "NIH Toolbox General Life Satisfaction FF Ages 13-17 v2.0": "nihtbx_lifesat_",
    "NIH Toolbox General Life Satisfaction Parent Report FF Ages 3-12 v2.0": "nihtbx_lifesat_par_",
    "NIH Toolbox Dom-Spec Life Satis Parent Rpt FF Ages 3-12 v2.0": "nihtbx_lifesat_par_",
    "NIH Toolbox Flanker Inhibitory Control and Attention Test Ages 3-7 v2.1": "nihtbx_flanker_",
    "NIH Toolbox Flanker Inhibitory Control and Attention Test Age 12+ v2.0": "nihtbx_flanker_",
    "NIH Toolbox Flanker Inhibitory Control and Attention Test Age 12+ v2.1": "nihtbx_flanker_",
    "Anxiety Summary Parent Report (3-7)": "nihtbx_anxiety_par_",
    "NIH Toolbox 2-Minute Walk Endurance Test Age 3+ v2.0": "nihtbx_endurance_",
    "NIH Toolbox 9-Hole Pegboard Dexterity Test Age 3+ v2.0": "nihtbx_pegboard_",
    "NIH Toolbox Anger FF Ages 8-17 v2.0": "nihtbx_anger_",
    "NIH Toolbox Anger Parent Report CAT Ages 8-12 v2.0": "nihtbx_anger_par_",
    "NIH Toolbox Anger Parent Report FF Ages 3-7 v2.0": "nihtbx_anger_par_",
    "NIH Toolbox Anger-Affect CAT Age 18+ v2.0": "nihtbx_angeraff_",
    "NIH Toolbox Anger-Hostility FF Age 18+ v2.0": "nihtbx_angerhos_",
    "NIH Toolbox Anger-Physical Aggression FF Age 18+ v2.0": "nihtbx_angerphys_",
    "NIH Toolbox Dimensional Change Card Sort Test Age 12+ v2.0": "nihtbx_cardsort_",
    "NIH Toolbox Dimensional Change Card Sort Test Ages 3-7 v2.1": "nihtbx_cardsort_",
    "NIH Toolbox Emotional Support FF Age 18+ v2.0": "nihtbx_emosupport_",
    "NIH Toolbox Emotional Support FF Ages 8-17 v2.0": "nihtbx_emosupport_",
    "NIH Toolbox Empathic Behaviors Parent Report CAT Ages 3-12 v2.0": "nihtbx_empathy_par_",
    "NIH Toolbox Fear FF Ages 8-17 v2.0": "nihtbx_fear_",
    "NIH Toolbox Fear Parent Report CAT Ages 8-12 v2.0": "nihtbx_fear_par_",
    "NIH Toolbox Fear-Affect CAT Age 18+ v2.0": "nihtbx_fearaffect_",
    "NIH Toolbox Fear-Over Anxious Parent Report FF Ages 3-7 v2.0": "nihtbx_fearoveranx_par_",
    "NIH Toolbox Fear-Separation Anxiety Parent Report FF Ages 3-7 v2.0": "nihtbx_fearsepanx_par_",
    "NIH Toolbox Fear-Somatic Arousal FF Age 18+ v2.0": "nihtbx_fearsomarousal_",
    "NIH Toolbox Friendship FF Age 18+ v2.0": "nihtbx_friend_",
    "NIH Toolbox Friendship FF Ages 8-17 v2.0": "nihtbx_friend_",
    "NIH Toolbox Instrumental Support FF Age 18+ v2.0": "nihtbx_instsupport_",
    "NIH Toolbox List Sorting Working Memory Test Ages 3-6 v2.1": "nihtbx_list_",
    "NIH Toolbox Meaning and Purpose CAT Age 18+ v2.0": "nihtbx_meaning_",
    "NIH Toolbox Odor Identification Test Age 10+ v2.0": "nihtbx_odor_",
    "NIH Toolbox Odor Identification Test Ages 3-9 v2.0": "nihtbx_odor_",
    "NIH Toolbox Pain Interference CAT Age 18+ v2.0": "nihtbx_pain_",
    "NIH Toolbox Pattern Comparison Processing Speed Test Ages 3 - 6 v2.1": "nihtbx_pattern_",
    "NIH Toolbox Peer Rejection Parent Report FF Ages 3-12 v2.0": "nihtbx_rejection_par_",
    "NIH Toolbox Perceived Hostility FF Age 18+ v2.0": "nihtbx_perchostil_",
    "NIH Toolbox Perceived Hostility FF Ages 8-17 v2.0": "nihtbx_perchostil_",
    "NIH Toolbox Perceived Rejection FF Age 18+ v2.0": "nihtbx_rejection_",
    "NIH Toolbox Perceived Rejection FF Ages 8-17 v2.0": "nihtbx_rejection_",
    "NIH Toolbox Positive Peer Interaction Parent Report FF Ages 3-12 v2.0": "nihtbx_peerinter_par_",
    "NIH Toolbox Social Withdrawal Parent Report FF Ages 3-12 v2.0": "nihtbx_withdrawal_par_",
    "Negative Social Perception Summary (13-17)": "nihtbx_negsocial_",
    "Negative Social Perception Summary (8-12)": "nihtbx_negsocial_",
    "Psychological Well Being Summary (18+)": "nihtbx_wellbeing_",
}

scores_map = {
    "RawScore": "rawscore",
    "Theta": "theta",
    "ItmCnt": "itmcnt",
    "Computed Score": "computedscore",
    "Uncorrected Standard Score": "uncorrected",
    "Age-Corrected Standard Score": "agecorrected",
    # "Fully-Corrected T-score": "fullycorrected",
    # "DateFinished": "date",
    # "Language": "language"
    "Age-Corrected Standard Scores Dominant": "agecorrected_dominant",
    "Age-Corrected Standard Scores Non-Dominant": "agecorrected_nondominant",
    "Age-Corrected Standard Scores Quinine Whole": "agecorrected_quinine",
    "Age-Corrected Standard Scores Salt Whole": "agecorrected_salt",
    "Dominant Score": "dominant",
    # "Fully-Corrected T-scores Dominant": "fullycorrected_dominant",
    # "Fully-Corrected T-scores Non-Dominant": "fullycorrected_nondominant",
    # "Fully-Corrected T-scores Quinine Whole": "fullycorrected_quinine",
    # "Fully-Corrected T-scores Salt Whole": "fullycorrected_salt",
    "National Percentile (age adjusted)": "percentile",
    "National Percentile (age adjusted) Dominant": "percentile_dominant",
    "National Percentile (age adjusted) Non-Dominant": "percentile_nondominant",
    "Non-Dominant Score": "nondominant",
    "Raw Score Left Ear": "raw_left",
    "Raw Score Right Ear": "raw_right",
    "SE": "se",
    "Static Visual Acuity Snellen": "snellen",
    "Static Visual Acuity logMAR": "logmar",
    "TScore": "tscore",
    "Threshold Left Ear": "thresh_left",
    "Threshold Right Ear": "thresh_right",
    "Uncorrected Standard Scores Dominant": "uncorrected_dominant",
    "Uncorrected Standard Scores Non-Dominant": "uncorrected_nondominant",
    "Uncorrected Standard Scores Quinine Whole": "uncorrected_quinine",
    "Uncorrected Standard Scores Salt Whole": "uncorrected_salt",
    "Whole Mouth Quinine": "whole_quinine",
    "Whole Mouth Salt": "whole_salt",
}


def main():
    # Download scores
    raw_dir = os.path.join(cache_space, 'raw')
    download_files(pattern='_Assessment_Scores_', directory=raw_dir)

    # Combine scores files
    files = os.listdir(raw_dir)
    combine_new_data(new_files)

    # Transform into regular CSV
    nda_transform('PROJ')

    # Download raw

    # Add to combined CSV

    ###

    # Get list of all Toolbox outputs on Box
    box_files = box.get_files(
        folder_id=toolbox_folder_id,
        pattern='_Assessment_Scores_',
        # maxfiles=3
    )
    # log(box_files)
    log("Found {} files in Box folder id {}".format(
        len(box_files), toolbox_folder_id))

    # Get filenames that have already been combined
    existing_files = already_processed()
    # log('{} existing_files'.format(len(existing_files)))

    # Download all new files from Box that need combined
    new_files = download_new_outputs(box_files, existing_files)
    # Don't do anything if no new data
    if not new_files:
        log('Nothing new to add. Exiting...')
        shutil.rmtree(box.cache)
        return
    # new_files = os.listdir(cache_space)

    # Combine all new output to file and upload to Box
    df = combine_new_data(new_files)

    duplicate_instruments(df)

    # Transform combined file into one subject per row
    # nda_transform()

    # Clean up cache space
    # shutil.rmtree(box.cache)


def download_files(pattern, directory):
    # Get all Toolbox raw assessment data on Box and download
    toolbox_folder_id = 42902161768

    box_files = box.get_files(
        folder_id=toolbox_folder_id,
        pattern=pattern,
        maxfiles=100
    )
    file_ids = []

    for f in box_files:
        file_ids.append(f.id)

    box.download_files(file_ids, directory)


def combine_new_data(new_files):
    """
    Get rows for all files in the cache
    Download the combined file from Box, add new rows, and upload to Box
    :new_files - List of filenames to add to combined file
    returns combined csv as a pandas dataframe
    """
    # Download combined file and add it to list of dataframes
    box.download_file(combined_file_id)
    df = pandas.read_csv(combined_path)
    dataframes = [df]

    # Load up all files into pandas dataframes and combine
    for fname in new_files:
        path = os.path.join(cache_space, fname)
        df = pandas.read_csv(path)
        dataframes.append(df)

        with open(processed_file, 'a') as store:
            store.write(fname + '\n')

    combined_df = pandas.concat(dataframes, axis=0, ignore_index=True)

    # Get rid of any empty columns and DeviceID
    filtered_data = combined_df.dropna(axis='columns', how='all')
    del filtered_data['DeviceID']
    filtered_data = filtered_data.drop_duplicates()

    # Sort by subject ID and instrument
    filtered_data.sort_values(by=['PIN', 'Inst'], inplace=True)

    # Move PIN and Inst to the first two columns, rest alphabetical
    column_headers = list(filtered_data)
    column_headers.remove('PIN')
    column_headers.insert(0, 'PIN')
    column_headers.remove('Inst')
    column_headers.insert(1, 'Inst')

    # Write to file and upload to Box as new version
    filtered_data.to_csv(combined_path, index=False, columns=column_headers)
    combined_box = box.client.file(str(combined_file_id))
    combined_box.update_contents(combined_path)

    return filtered_data


def duplicate_instruments(df):
    """
    Return list of subjects that have duplicate instruments with different data
    :df - Combined output in pandas dataframe
    ??? Maybe pull down the combined file from Box
    """
    subjects = set()
    dups = df[df.duplicated(subset=['PIN', 'Inst'], keep=False)]

    for pin in dups['PIN']:
        subjects.add(pin)

    if subjects:
        print('WARNING: Duplicate instruments for {} subjects'.format(
            len(subjects)))
        for s in subjects:
            print(s)

    return subjects


def nda_transform(project, parent=False):
    # Project is one of HCA or HCD

    # f = box.download_file(combined_file_id)
    # print(f)

    # combined_path = os.path.join(
    #     cache_space, 'HCD_Toolbox_sample_correct_batteries.csv')
    combined_path = os.path.join(
        cache_space, 'Toolbox_Combined.csv')

    df = pandas.read_csv(combined_path)
    # del df['DateFinished']
    del df['Language']
    del df['Assessment Name']
    # column_headers = list(df)

    # Go through each row of the CSV and build a dict for each subject
    dicts = []
    subject_dict = {}
    # parent_report = False

    for idx, row in df.iterrows():
        inst = row['Inst']
        subject = row['PIN']

        # print('subject: {}, inst: {}'.format(subject, inst))

        if project not in subject:
            continue

        # if parent and 'Parent' not in inst:
        #     continue

        # print("Parent instrument found -- {}".format(inst))

        if not valid_instrument(subject, inst):
            print('Instrument error. Skipping {}'.format(subject))
            continue

        try:
            # See if we've reached the next subject
            next_row_subject = df['PIN'].iloc[idx + 1]
        except IndexError:
            # We've reached the end of the CSV
            print('end of {}'.format(combined_path))
            continue

        if subject != next_row_subject:

            print("{} != {}".format(subject, next_row_subject))

            if not valid_subject_label(subject):
                print('Label error. Skipping {}'.format(subject))
                subject_dict = {}
                continue

            # Add the subject ID
            subject_dict['PIN'] = subject

            dicts.append(subject_dict)

            subject_dict = {}

            continue

        # Construct a dict for each score type
        for score, suffix in scores_map.items():
            try:
                key = instrument_map[inst] + suffix
                subject_dict[key] = row[score]
            except KeyError:
                continue

    combined = pandas.DataFrame(dicts)
    filtered = combined.dropna(axis='columns', how='all')

    if parent:
        pins = par_cols = filtered
        pins = pins.filter(like="PIN")
        par_cols = par_cols.filter(like="_par_")
        filtered = par_cols

    print(filtered.info(verbose=False))

    out = os.path.join(
        cache_space, 'NDA_Toolbox_Scores_{}.csv'.format(project))
    filtered.to_csv(out, index=False)


def valid_subject_label(label):
    global label_errors
    subject_errors = []

    # Subjects with known anomalies
    # skip = ['HCA9268996_V1']
    skip = []

    if label in skip:
        subject_errors.append('{} is a known anomaly'.format(label))

    # Check for 13 characters total
    # if len(label) != 13:
    #     subject_errors.append(
    #         '{} is not 13 characters long'.format(label)
    #     )

    # Has a visit in the label, i.e., ends in _V#
    # if '_V' not in label:
    #     subject_errors.append(
    #         '{} does not have a visit in the label'.format(label)
    #     )

    # Should be 10 characters long not including the Visit
    label_wo_visit = label.split('_')[0]
    if len(label_wo_visit) != 10:
        subject_errors.append(
            '{} is not 10 characters long'.format(label_wo_visit)
        )

    # Check that numerical ranges are correct
    sub = label.split('_')[0]
    numbers = [int(d) for d in sub if d.isdigit()]
    subject_number = ''

    for num in numbers:
        subject_number += str(num)

    if not subject_number.isdigit():
        subject_errors.append(
            '{} does not contain expected number'.format(label)
        )
        label = 'skip'

    # HCD subjects are 0000000-5999999
    if 'HCD' in label and not \
       (int(subject_number) >= 0 and int(subject_number) <= 5999999):
        subject_errors.append(
            '{} is not in the range 0000000-5999999'.format(label)
        )
    # HCA subjects are 6000000-9999999
    if 'HCA' in label and not \
       (int(subject_number) >= 6000000 and int(subject_number) <= 9999999):
        subject_errors.append(
            '{} is not in the range 6000000-9999999'.format(label)
        )

    if subject_errors:
        # print(subject_errors)
        label_errors.extend(subject_errors)
        return False

    return True


def valid_instrument(subject, instrument):
    global instrument_errors

    # if subject == 'HCA9268996_V1':
    #     print('{} -- {}'.format(subject, instrument))

    sub = subject.split('_')[0]
    numbers = [int(d) for d in sub if d.isdigit()]
    subject_number = ''

    for num in numbers:
        subject_number += str(num)

    if not subject_number:
        return False

    if int(subject_number) >= 6000000 and int(subject_number) <= 9999999 \
       and instrument not in hca_instruments:
        e = '{} has incorrect instrument -- {}'.format(subject, instrument)
        instrument_errors.append(e)
        return False

    if int(subject_number) >= 0 and int(subject_number) <= 5999999 \
       and instrument not in hcd_instruments:
        e = '{} has incorrect instrument -- {}'.format(subject, instrument)
        instrument_errors.append(e)
        return False

    return True


def map_instrument_names():
    """ Figure out what is already included in ABCD data type and what still
        needs mapped
    """
    df = pandas.read_csv(combined_path)
    needs_mapped = set()
    already_mapped = set()
    unique_instruments = set()

    for idx, row in df.iterrows():
        # subject = row['PIN']
        inst = row['Inst']

        unique_instruments.add(inst)

        if inst in instrument_map.keys():
            already_mapped.add(inst)
            continue

        needs_mapped.add(inst)

    print('\n{} already_mapped:'.format(len(already_mapped)))
    print(sorted(already_mapped))

    print('\n{} needs_mapped:'.format(len(needs_mapped)))
    print(sorted(needs_mapped))

    for inst in sorted(needs_mapped):
        print(inst)


def get_hcd_instruments():
    path = '/Users/michael/Dropbox/Dev/nda/ccf-nda-behavioral/test/toolbox/'
    fname = 'toolbox-hcd-tests.csv'
    sample_df = pandas.read_csv(os.path.join(path, fname))
    combined_df = pandas.read_csv(combined_path)

    sample_unique_inst = set()
    combined_unique_inst = set()

    for idx, row in sample_df.iterrows():
        # subject = row['PIN']
        inst = row['Inst'].replace(' v1.1', '') \
                          .replace(' v2.0', '') \
                          .replace(' v2.1', '')
        sample_unique_inst.add(inst)

    for idx, row in combined_df.iterrows():
        subject = row['PIN']
        inst = row['Inst'].replace(' v1.1', '') \
                          .replace(' v2.0', '') \
                          .replace(' v2.1', '')

        if 'HCD' in subject and valid_subject_label(subject):
            combined_unique_inst.add(inst)

    # for inst in sorted(sample_unique_inst):
    #     print(inst)

    combined_only = combined_unique_inst - sample_unique_inst
    sample_only = sample_unique_inst - combined_unique_inst

    print('{} unique HCD instruments in SAMPLE output'.format(
        len(sample_unique_inst))
    )

    print('{} unique HCD instruments in COMBINED output'.format(
        len(combined_unique_inst))
    )
    # return sample_unique_inst

    # print('{} instruments differing between the two'.format(len(diff)))

    with open(os.path.join(cache_space, 'sample_unique_inst.txt'), 'w') as f:
        for inst in sample_unique_inst:
            f.write(inst + '\n')

    with open(os.path.join(cache_space, 'combined_unique_inst.txt'), 'w') as f:
        for inst in combined_unique_inst:
            f.write(inst + '\n')

    # with open(os.path.join(cache_space, 'diff_unique_inst.txt'), 'w') as f:
    #     for inst in diff:
    #         f.write(inst + '\n')

    with open(os.path.join(cache_space, 'combined_only_inst.txt'), 'w') as f:
        for inst in combined_only:
            f.write(inst + '\n')

    with open(os.path.join(cache_space, 'sample_only_inst.txt'), 'w') as f:
        for inst in sample_only:
            f.write(inst + '\n')


def get_hca_instruments():
    hca_instruments = set()

    for instrument, prefix in instrument_map.items():
        if instrument not in hcd_instruments:
            hca_instruments.add(instrument)

    for inst in hca_instruments:
        print(inst)

    print('{} unique HCA instruments'.format(len(hca_instruments)))


def instrument_summary(output):
    df = pandas.read_csv(output)

    # Unique test names
    unique_tests = set()

    for idx, row in df.iterrows():
        subject = row['PIN']
        inst = row['Inst']

        if not valid_subject_label(subject):
            print('{} not a valid label'.format(subject))
            continue

        unique_tests.add(inst)

    for test in unique_tests:
        print(test)

    print('{} unique tests'.format(len(unique_tests)))

    # Count for each test name
    tests_map = {}

    for idx, row in df.iterrows():
        subject = row['PIN']
        inst = row['Inst']

        try:
            tests_map[inst]['count'] += 1
        except Exception:
            tests_map[inst] = {}
            tests_map[inst]['count'] = 1

        try:
            tests_map[inst]['subjects'].append(subject)
        except Exception:
            tests_map[inst]['subjects'] = []
            tests_map[inst]['subjects'].append(subject)

    # for test, value in tests_map.items():
    #     print('{} - {}'.format(test, value))

    print('Low test occurrences in {}:\n'.format(output))

    for test, value in tests_map.items():
        if value['count'] < 1:
            print('({}) - {}'.format(value['count'], test))
            print('for participants {}\n'.format(', '.join(value['subjects'])))

    # Subjects for outlier counts


def generate_datadict(project):
    processed = []
    definitions = []

    fname = 'NDA_Toolbox_Scores_{}.csv'.format(project)
    with open(os.path.join(cache_space, fname)) as f:
        scores = f.readline()

    for instrument, prefix in instrument_map.items():
        # Only consider the first for each prefix
        if prefix in processed:
            continue

        processed.append(prefix)

        for score, suffix in scores_map.items():
            name = prefix + suffix
            desc = instrument + ' ' + score
            trim_start = desc.find('Age')
            trim_end = desc.find('v2.') + 5

            desc = desc.replace(' (18+)', '') \
                       .replace(' (13-17)', '') \
                       .replace(' (8-12)', '')

            if trim_start != -1 and trim_end != -1 and trim_end > trim_start:
                desc = desc[:trim_start] + desc[trim_end:]
                # print('{} -- {}'.format(trim_start, trim_end))

            line = '"{}","Float","30","Recommended","{}","","","",'.format(
                name, desc
            )

            # Check the headers of HCA/HCD outputs to get grouping
            if name in scores:
                definitions.append(line)

    fname = '{}_toolbox_definitions.csv'.format(project.lower())
    with open(os.path.join(cache_space, fname), 'a') as f:
        for line in sorted(definitions):
            # print(line)
            f.write(line + '\n')

    print(len(definitions))


def field_types(project):
    hca_scores = os.path.join(
        cache_space, 'NDA_Toolbox_Scores_{}.csv'.format(project))

    df = pandas.read_csv(hca_scores)

    for idx, row in df.iterrows():
        for col, value in row.items():

            if col == 'PIN':
                continue

            if isinstance(value, float):
                continue

            # if col == 'nihtbx_visualacuity_snellen':
            #     continue

            print('{} -- {}'.format(col, value))


def print_errors():
    global label_errors
    global instrument_errors

    with open(os.path.join(cache_space, 'label_errors.txt'), 'w') as f:
        for err in label_errors:
            f.write(err + '\n')

    with open(os.path.join(cache_space, 'instrument_errors.txt'), 'w') as f:
        for err in instrument_errors:
            f.write(err + '\n')

    print('{} subject label errors'.format(len(label_errors)))
    print('{} instrument errors'.format(len(instrument_errors)))


def log(message):
    # Write out to file, or possibly just pipe stdout to file on command line
    if verbose:
        print(message)


if __name__ == '__main__':
    # main()

    # map_instrument_names()

    nda_transform('HCA', parent=False)

    # generate_datadict('HCA')

    # print_errors()

    # get_hcd_instruments()
    # get_hca_instruments()

    # hca_combined = os.path.join(cache_space, 'Toolbox_Combined_HCA.csv')
    # instrument_summary(hca_combined)

    # hcd_combined = os.path.join(
    #     cache_space,
    #     'HCD_Toolbox_sample correct_batteries.csv'
    # )
    # instrument_summary(hcd_combined)

    # field_types('HCD_parent')
