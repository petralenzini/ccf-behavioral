import io

import pandas as pd
import pycurl
import json
from io import BytesIO
import numpy as np
import requests

from config import config

redcapconfigfile = config['redcap']['config']
redcap_api_url = config['redcap']['api_url']


def getredcapdata(self, fulldata=False):
    """
    Downloads required fields for all nda structures from Redcap databases specified by details in redcapconfig file
    Returns panda dataframe with fields 'study', 'Subject_ID, 'subject', and 'flagged', where 'Subject_ID' is the
    patient id in the database of interest (sometimes called subject_id, parent_id).
    subject is this same id stripped of underscores or flags like 'excluded' to make it easier to merge
    flagged contains the extra characters other than the id so you can keep track of who should NOT be uploaded to NDA
     or elsewwhere shared

    Args:
        fulldata (bool): Should parents be included
    """
    auth = pd.read_csv(redcapconfigfile)
    studydata = pd.DataFrame()

    records = auth.to_dict(orient='records')
    if not fulldata:
        # skip 1st row of auth which holds parent info
        records = records[1:]
    # else don't skip

    for z in records:
        fields = [
            z['field'],
            z['interview_date'],
            z['sexatbirth'],
            z['sitenum'],
            z['dobvar'],
        ]
        r = redcapApi(z['token'], fields=fields, events=[z['event']])

        pexpanded = pd.read_csv(r, encoding='utf8')
        pexpanded.rename(columns={z['interview_date']: 'interview_date'}, inplace=True)

        pexpanded = pexpanded.loc[~(pexpanded[z['field']] == '')]
        new = pexpanded[z['field']].str.split("_", 1, expand=True)
        pexpanded['subject'] = new[0].str.strip()
        pexpanded['flagged'] = new[1].str.strip()
        pexpanded['study'] = z['study']

        studydata = pd.concat([studydata, pexpanded], axis=0, sort=True)

    return studydata


def getfullredcapdata(self):
    return getredcapdata(False)


def getredcapfields(self, fieldlist, study):
    """"
    Downloads requested fields from Redcap databases specified by details in redcapconfig file
    Returns panda dataframe with fields 'study', 'Subject_ID, 'subject', and 'flagged', where 'Subject_ID' is the
    patient id in the database of interest (sometimes called subject_id, parent_id) as well as requested fields.
    subject is this same id stripped of underscores or flags like 'excluded' to make it easier to merge
    flagged contains the extra characters other than the id so you can keep track of who should NOT be uploaded to NDA
    or elsewwhere shared
    """
    auth = pd.read_csv(redcapconfigfile)
    studydata = pd.DataFrame()
    z = auth.set_index('study', drop=False).to_dict('index')
    z = z[study]
    fields = [
        z['field'],
        z['interview_date'],
        z['sexatbirth'],
        z['sitenum'],
        z['dobvar'],
    ]
    fields.extend(fieldlist)
    r = redcapApi(z['token'], fields=fields, events=[z['event']])

    pexpanded = pd.read_csv(r, encoding='utf8')
    pexpanded.rename(columns={z['interview_date']: 'interview_date'}, inplace=True)

    pexpanded = pexpanded.loc[~(pexpanded[z['field']] == '')]
    new = pexpanded[z['field']].str.split("_", 1, expand=True)
    pexpanded['subject'] = new[0].str.strip()
    pexpanded['flagged'] = new[1].str.strip()
    pexpanded['study'] = z['study']
    studydata = pd.concat([studydata, pexpanded], axis=0, sort=True)
    # Convert age in years to age in months
    # note that dob is hardcoded var name here because all redcap databases use same variable name...sue me
    # interview date, which was originally v1_date for hcpa, has been
    # renamed in line above, headerv2
    studydata['nb_months'] = (12 *
                              (pd.to_datetime(studydata['interview_date']).dt.year -
                               pd.to_datetime(studydata.dob).dt.year) +
                              (pd.to_datetime(studydata['interview_date']).dt.month -
                               pd.to_datetime(studydata.dob).dt.month) +
                              (pd.to_datetime(studydata['interview_date']).dt.day -
                               pd.to_datetime(studydata.dob).dt.day) /
                              31)
    studydata['nb_months'] = studydata['nb_months'].apply(np.floor)
    studydata['nb_monthsPHI'] = studydata['nb_months']
    studydata.loc[studydata.nb_months > 1080, 'nb_monthsPHI'] = 1200
    studydata = studydata.drop(
        columns={'nb_months'}).rename(
        columns={
            'nb_monthsPHI': 'interview_age'})

    return studydata


def redcapApi(token, fields=[], events=[],
              format='csv',
              content='record',
              type='flat',
              returnFormat='json',
              rawOrLabel='raw',
              rawOrLabelHeaders='raw',
              exportCheckboxLabel='false',
              exportSurveyFields='false',
              exportDataAccessGroups='false'):
    data = {
        'token': token,
        'format': format,
        'content': content,
        'type': type,
        'returnFormat': returnFormat,
        'rawOrLabel': rawOrLabel,
        'rawOrLabelHeaders': rawOrLabelHeaders,
        'exportCheckboxLabel': exportCheckboxLabel,
        'exportSurveyFields': exportSurveyFields,
        'exportDataAccessGroups': exportDataAccessGroups,
    }
    if fields:
        data['fields'] = fields

    if events:
        data['events'] = events

    r = requests.post(redcap_api_url, data=data)
    return io.BytesIO(r.content)


def getredcapids(self):
    """
    Downloads field (IDS) in Redcap databases specified by details in redcapconfig file
    Returns panda dataframe with fields 'study', 'Subject_ID, 'subject', and 'flagged', where 'Subject_ID' is the
    patient id in the database of interest (sometimes called subject_id, parent_id).
    subject is this same id stripped of underscores or flags like 'excluded' to make it easier to merge
    flagged contains the extra characters other than the id so you can keep track of who should NOT be uploaded to NDA
     or elsewwhere shared
    """
    auth = pd.read_csv(redcapconfigfile)
    studyids = pd.DataFrame()

    for z in auth.to_dict(orient='records'):
        r = redcapApi(z['token'], fields=[z['field']], events=[z['event']])

        df = pd.read_csv(r, encoding='utf8', header=['Subject_ID'])
        df.columns = 'Subject_ID'
        parent_ids = parent_ids.iloc[1:, ]
        parent_ids = parent_ids.loc[~(parent_ids.Subject_ID == '')]
        uniqueids = pd.DataFrame(
            parent_ids.Subject_ID.unique(),
            columns=['Subject_ID'])
        uniqueids['Subject_ID'] = uniqueids.Subject_ID.str.strip('\'"')
        new = uniqueids['Subject_ID'].str.split("_", 1, expand=True)
        uniqueids['subject'] = new[0].str.strip()
        uniqueids['flagged'] = new[1].str.strip()
        uniqueids['study'] = z['study']
        studyids = studyids.append(uniqueids)
    return studyids

# use json format because otherwise commas in strings convert wrong in csv
# read
# , token=token[0],field=field[0],event=event[0]):
def getredcapfieldsjson(self, fieldlist, study='hcpdparent '):
    """
    Downloads requested fields from Redcap databases specified by details in redcapconfig file
    Returns panda dataframe with fields 'study', 'Subject_ID, 'subject', and 'flagged', where 'Subject_ID' is the
    patient id in the database of interest (sometimes called subject_id, parent_id) as well as requested fields.
    subject is this same id stripped of underscores or flags like 'excluded' to make it easier to merge
    flagged contains the extra characters other than the id so you can keep track of who should NOT be uploaded to NDA
    or elsewwhere shared
    """
    auth = pd.read_csv(redcapconfigfile)
    studydata = pd.DataFrame()
    fieldlistlabel = [
        'fields[' +
        str(i) +
        ']' for i in range(
            5,
            len(fieldlist) +
            5)]
    fieldrow = dict(zip(fieldlistlabel, fieldlist))
    d1 = {'token': auth.loc[auth.study == study,
                            'token'].values[0],
          'content': 'record',
          'format': 'json',
          'type': 'flat',
          'fields[0]': auth.loc[auth.study == study,
                                'field'].values[0],
          'fields[1]': auth.loc[auth.study == study,
                                'interview_date'].values[0],
          'fields[2]': auth.loc[auth.study == study,
                                'sexatbirth'].values[0],
          'fields[3]': auth.loc[auth.study == study,
                                'sitenum'].values[0],
          'fields[4]': auth.loc[auth.study == study,
                                'dobvar'].values[0]}
    d2 = fieldrow
    d3 = {'events[0]': auth.loc[auth.study == study,
                                'event'].values[0],
          'rawOrLabel': 'raw',
          'rawOrLabelHeaders': 'raw',
          'exportCheckboxLabel': 'false',
          'exportSurveyFields': 'false',
          'exportDataAccessGroups': 'false',
          'returnFormat': 'json'}
    data = {**d1, **d2, **d3}
    buf = BytesIO()
    ch = pycurl.Curl()
    ch.setopt(
        ch.URL,
        redcap_api_url)
    ch.setopt(ch.HTTPPOST, list(data.items()))
    ch.setopt(ch.WRITEDATA, buf)
    ch.perform()
    ch.close()
    htmlString = buf.getvalue().decode('UTF-8')
    buf.close()
    d = json.loads(htmlString)
    pexpanded = pd.DataFrame(d)
    pexpanded = pexpanded.loc[~(
            pexpanded[auth.loc[auth.study == study, 'field'].values[0]] == '')]
    new = pexpanded[auth.loc[auth.study == study,
                             'field'].values[0]].str.split("_", 1, expand=True)
    pexpanded['subject'] = new[0].str.strip()
    pexpanded['flagged'] = new[1].str.strip()
    pexpanded['study'] = study  # auth.study[i]
    studydata = pd.concat([studydata, pexpanded], axis=0, sort=True)
    studydata = studydata.rename(columns={
        auth.loc[auth.study == study, 'interview_date'].values[0]: 'interview_date'})
    # Convert age in years to age in months
    # note that dob is hardcoded var name here because all redcap databases use same variable name...sue me
    # interview date, which was originally v1_date for hcpd, has been
    # renamed in line above, headerv2
    try:
        studydata['nb_months'] = (
                12 * (pd.to_datetime(studydata['interview_date']).dt.year - pd.to_datetime(studydata.dob).dt.year) +
                (pd.to_datetime(studydata['interview_date']).dt.month - pd.to_datetime(studydata.dob).dt.month) +
                (pd.to_datetime(studydata['interview_date']).dt.day - pd.to_datetime(studydata.dob).dt.day) / 31)
        studydatasub = studydata.loc[studydata.nb_months.isnull()].copy()
        studydatasuper = studydata.loc[~(
            studydata.nb_months.isnull())].copy()
        studydatasuper['nb_months'] = studydatasuper['nb_months'].apply(
            np.floor).astype(int)
        studydatasuper['nb_monthsPHI'] = studydatasuper['nb_months']
        studydatasuper.loc[studydatasuper.nb_months >
                           1080, 'nb_monthsPHI'] = 1200
        studydata = pd.concat([studydatasub, studydatasuper], sort=True)
        studydata = studydata.drop(
            columns={'nb_months'}).rename(
            columns={
                'nb_monthsPHI': 'interview_age'})
    except BaseException:
        pass
    # convert gender to M/F string
    try:
        studydata.gender = studydata.gender.str.replace('1', 'M')
        studydata.gender = studydata.gender.str.replace('2', 'F')
    except BaseException:
        print(study + ' has no variable named gender')
    return studydata
