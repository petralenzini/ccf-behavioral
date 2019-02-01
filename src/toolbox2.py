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
# combined_path = os.path.join(cache_space, 'Toolbox_Combined.csv')
# processed_file = os.path.join(root_dir, 'store/processed-toolbox.txt')
# combined_file_id = 288874095195
# toolbox_folder_id = 42902161768
box = LifespanBox(cache=cache_space)

# label_errors = []
# instrument_errors = []

# print(hcd_instruments)
# sys.exit()

# Map Toolbox Instrument names to NDA datatype
instrument_datatype_map = {
    "Cognition Fluid Composite": "cogcomp01",
    "Cognition Fluid Composite v1.1": "cogcomp01",
    "Cognition Crystallized Composite": "cogcomp01",
    "Cognition Crystallized Composite v1.1": "cogcomp01",
    "Cognition Total Composite Score": "cogcomp01",
    "Cognition Total Composite Score v1.1": "cogcomp01",
    "Cognition Early Childhood Composite": "cogcomp01",
    "Cognition Early Childhood Composite v1.1": "cogcomp01",

    "NIH Toolbox Flanker Inhibitory Control and Attention Test Age 12+ v2.1": "flanker01",

    "NIH Toolbox Emotional Support FF Age 18+ v2.0": "tlbx_emsup01",

    "NIH Toolbox Empathic Behaviors Parent Report CAT Ages 3-12 v2.0": "tlbx_empbeh01",

    "NIH Toolbox Fear-Somatic Arousal FF Age 18+ v2.0": "tlbx_fearanx01",

    "NIH Toolbox Friendship FF Age 18+ v2.0": "tlbx_friend01",

    "NIH Toolbox Perceived Rejection FF Age 18+ v2.0": "tlbx_rej01",

    "NIH Toolbox Perceived Hostility FF Age 18+ v2.0": "tlbx_perhost01",

    "Psychological Well Being Summary (18+)": "tlbx_wellbeing01",

    "NIH Toolbox Sadness CAT Age 18+ v2.0": "tlbx_sadness01",

    "NIH Toolbox Self-Efficacy CAT Age 18+ v2.0": "self_effic01",

    "NIH Toolbox Social Withdrawal Parent Report FF Ages 3-12 v2.0": "tlbx_socwit01",

    "NIH Toolbox List Sorting Working Memory Test Age 7+ v2.1": "lswmt01",

    "NIH Toolbox Loneliness FF Ages 8-17 v2.0": "nihtlbxlone01",
    "NIH Toolbox Loneliness FF Age 18+ v2.0": "nihtlbxlone01",

    "NIH Toolbox Oral Reading Recognition Test Age 3+ v2.0": "orrt01",

    "NIH Toolbox Picture Vocabulary Test Age 3+ v2.0": "tpvt01",

    "": "tlbx_sensation01",

    "": "osdt01",

    "NIH Toolbox Pattern Comparison Processing Speed Test Ages 3 - 6 v2.1": "pcps01",

    "NIH Toolbox Perceived Stress FF Age 18+ v2.0": "pss01",

    "NIH Toolbox Picture Sequence Memory Test Age 8+ Form A v2.1": "psm01"
}

instrument_prefix_map = {
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
    "Age-Corrected Standard Scores Dominant": "agecorrected_dominant",
    "Age-Corrected Standard Scores Non-Dominant": "agecorrected_nondominant",
    "Age-Corrected Standard Scores Quinine Whole": "agecorrected_quinine",
    "Age-Corrected Standard Scores Salt Whole": "agecorrected_salt",
    "Dominant Score": "dominant",
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
    # "Fully-Corrected T-score": "fullycorrected",
    # "DateFinished": "date",
    # "Language": "language"
}

alias_map = {
    "nihtbx_flanker_fullycorrected": "flkr_fass",
    "nihtbx_flanker_rawscore": "nih_flanker_raw",
    "nihtbx_flanker_computedscore": ""
}

# dataframes_example2 = {
#     'flanker01': pandas.DataFrame(),
#     'tlbx_emsup01': pandas.DataFrame()
# }


dataframes = {}


def main():
    # Collect all raw assessment data into pandas dataframes
    raw_dir = os.path.join(cache_space, 'raw')
    download_files(pattern='_Assessment_Data_', directory=raw_dir)

    for f in os.listdir(raw_dir):
        fpath = os.path.join(raw_dir, f)
        convert_raw_csv(fpath)

    # # Collect all scored data into pandas dataframes
    # scores_dir = os.path.join(cache_space, 'scores')
    # # download_files(pattern='_Assessment_Scores_', directory=scores_dir)

    # for f in os.listdir(scores_dir):
    #     fpath = os.path.join(scores_dir, f)
    #     convert_scores_csv(fpath)

    # Write each dataframe out to an NDA submission CSV
    for datatype, dataframe in dataframes.items():
        # print("NDA datatype: {}".format(datatype))
        # print(dataframe + '\n')
        create_submission_csv(datatype, dataframe)

    # TODO prevent int conversion to float in pandas


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


def convert_raw_csv(f):
    """
    Go through each Toolbox RAW data CSV and populate pandas dataframe.
    There will be a file for each NDA datatype.
    """
    raw_df = pandas.read_csv(f)
    records = {}

    for idx, row in raw_df.iterrows():
        inst = row['Inst']
        subject = row['PIN']
        score = row['Score']

        try:
            date = row['DateCreated'].split(' ')[0]
        except AttributeError as e:
            date = '01/01/1900'
            print(e)
            print(row['DateCreated'])
            print("File: {}".format(f))

        # TODO convert date to correct format

        try:
            item = row['ItemID'].lower()
        except AttributeError as e:
            item = row['ItemID']
            print(e)
            print(item)
            print("File: {}".format(f))

        try:
            datatype = instrument_datatype_map[inst]
        except KeyError:
            # print("{} not in instrument_datatype map".format(inst))
            continue

        # Collect all the data for instruments which are one row each

        if datatype not in dataframes:
            success = init_dataframe(datatype)

            if not success:
                continue

        if datatype not in records:
            records[datatype] = {}

        if subject not in records[datatype]:
            records[datatype][subject] = {
                'date': date,
                'items': {}
            }

        records[datatype][subject]['items'][item] = score

    add_submissions(records)


def convert_scores_csv(f):
    """
    Go through each Toolbox SCORED CSV and populate pandas dataframe.
    There will be a file for each NDA datatype.
    """
    scores_df = pandas.read_csv(f)
    records = {}

    for idx, row in scores_df.iterrows():
        inst = row['Inst']
        subject = row['PIN']

        try:
            date = row['DateCreated'].split(' ')[0]
            # TODO convert date to correct format
        except AttributeError as e:
            date = '01/01/1900'
            print(e)
            print(row['DateCreated'])
            print("File: {}".format(f))

        try:
            datatype = instrument_datatype_map[inst]
        except KeyError:
            print("{} not in instrument_datatype map".format(inst))
            continue

        # Collect all the data for instruments which are one row each

        if datatype not in dataframes:
            init_dataframe(datatype)

        if datatype not in records:
            records[datatype] = {}

        if subject not in records[datatype]:
            records[datatype][subject] = {
                'date': date,
                'items': {}
            }

        print(row)
        sys.exit()


def init_dataframe(datatype):
    """
    Create a dataframe for the datatype if it doesn't exit and
    Initialize it with the columns from the NDA template CSVs.
    """
    dataframes[datatype] = pandas.DataFrame(dtype=str)

    templates_dir = os.path.join(cache_space, 'templates')
    f = '{}_template.csv'.format(datatype)
    template_file = os.path.join(templates_dir, f)

    if not os.path.exists(template_file):
        print("Cannot find template file for {}".format(datatype))
        return False

    with open(template_file) as f:
        # First line of template file is datatype, the second are columns
        header = f.readlines()[1].split(',')
        columns = []

        for col in header:
            columns.append(col.replace('"', '').strip())

    dataframes[datatype] = pandas.DataFrame(columns=columns)

    return True


def add_submissions(records):
    """
    Go through each subject level record and add them to the dataframes
    """

    for datatype, record in records.items():
        for subject_id, data in record.items():
            subject_record = {
                'subjectkey': get_nda_guid(subject_id),
                'src_subject_id': subject_id,
                'interview_date': data['date'],
                'interview_age': get_age(subject_id),
                'gender': get_gender(subject_id)
            }

            for col, value in data['items'].items():
                # print(col, value)
                subject_record[col] = value

            dataframes[datatype] = dataframes[datatype].append(
                subject_record, ignore_index=True)

    # print(dataframes)


def get_nda_guid(subject_id):
    # return 'NDAR_INVLE039CUM'  # Production GUID
    return 'NDAR_GUID'


def get_age(subject_id):
    return 360


def get_gender(subject_id):
    return 'F'


def create_submission_csv(datatype, dataframe):
    """
    Create an NDA submission CSV for a datatype for a dataframe
    """
    submissions_dir = os.path.join(cache_space, 'submissions')
    f = '{}_submission.csv'.format(datatype)
    submission_file = os.path.join(submissions_dir, f)

    # Write the headers and data to the CSV
    dataframe.to_csv(submission_file, index=False)

    # Prepend the datatype and version to the file as required by NDA
    with open(submission_file) as f:
        contents = f.read()

    with open(submission_file, 'w') as f:
        name = datatype[:-2]
        version = datatype[-2:]
        header = '"{}","{}"\n'.format(name, version)
        f.write(header)
        f.write(contents)


##########################


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


def log(message):
    # Write out to file, or possibly just pipe stdout to file on command line
    if verbose:
        print(message)


if __name__ == '__main__':
    main()
