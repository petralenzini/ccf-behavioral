import io
import pandas as pd
import numpy as np
import requests

from config import config

default_config = config['redcap']['config']
default_url = config['redcap']['api_url']

class Redcap:
    def __init__(self, config_file=default_config, url=default_url):
        self.config = config_file
        self.url = default_url

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
        auth = pd.read_csv(self.config)
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
            pexpanded = redcapApi(z['token'], fields=fields, events=[z['event']])
            pexpanded.rename(columns={z['interview_date']: 'interview_date'}, inplace=True)

            pexpanded = pexpanded.loc[~(pexpanded[z['field']] == '')]
            new = pexpanded[z['field']].str.split("_", 1, expand=True)
            pexpanded['subject'] = new[0].str.strip()
            pexpanded['flagged'] = new[1].str.strip()
            pexpanded['study'] = z['study']

            studydata = pd.concat([studydata, pexpanded], axis=0, sort=True)

        return studydata


    def getfullredcapdata(self):
        return self.getredcapdata(False)


    def getredcapfields(self, fieldlist, study):
        """"
        Downloads requested fields from Redcap databases specified by details in redcapconfig file
        Returns panda dataframe with fields 'study', 'Subject_ID, 'subject', and 'flagged', where 'Subject_ID' is the
        patient id in the database of interest (sometimes called subject_id, parent_id) as well as requested fields.
        subject is this same id stripped of underscores or flags like 'excluded' to make it easier to merge
        flagged contains the extra characters other than the id so you can keep track of who should NOT be uploaded to NDA
        or elsewwhere shared
        """
        auth = pd.read_csv(self.config)
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
        pexpanded = redcapApi(z['token'], fields=fields, events=[z['event']])
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




    def getredcapids(self):
        """
        Downloads field (IDS) in Redcap databases specified by details in redcapconfig file
        Returns panda dataframe with fields 'study', 'Subject_ID, 'subject', and 'flagged', where 'Subject_ID' is the
        patient id in the database of interest (sometimes called subject_id, parent_id).
        subject is this same id stripped of underscores or flags like 'excluded' to make it easier to merge
        flagged contains the extra characters other than the id so you can keep track of who should NOT be uploaded to NDA
         or elsewwhere shared
        """
        auth = pd.read_csv(self.config)
        studyids = pd.DataFrame()

        for z in auth.to_dict(orient='records'):
            df = redcapApi(z['token'], fields=[z['field']], events=[z['event']])
            df.columns = ['Subject_ID']
            parent_ids = df
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
        data['fields[]'] = fields

    if events:
        data['events[]'] = events

    r = requests.post(default_url, data=data)
    r = io.BytesIO(r.content)
    return pd.read_csv(r, encoding='utf8')
