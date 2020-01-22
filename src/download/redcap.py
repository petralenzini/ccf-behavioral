import io
import pandas as pd
import numpy as np
import requests

from config import LoadSettings

config = LoadSettings()['Redcap']
red = config['tables']

default_url = config['api_url']




class Redcap:
    def __init__(self, url=default_url):
        self.url = url

    def redcapApi(self, token, fields=[], events=[], forms=[],
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

        if forms:
            data['forms[]'] = forms

        r = requests.post(self.url, data=data)
        r = io.BytesIO(r.content)
        return pd.read_csv(r, encoding='utf8', parse_dates=True)

    def get_all_rows(self, study, fieldlist=[], subjectOnly=False):
        s = red[study]
        fieldnames = s['fields']
        token = s['token']
        events = [s['events']]
        if subjectOnly:
            fields = [fieldnames['field']]
        else:
            fields = list(fieldnames.values())
        fields.extend(fieldlist)

        df = self.redcapApi(token, fields, events)
        df.rename(columns={fieldnames['interview_date']: 'interview_date'}, inplace=True)
        df = df[df[fieldnames['field']] != '']
        split_df = df[fieldnames['field']].str.split("_", 1, expand=True)
        df['subject'] = split_df[0].str.strip()
        df['flagged'] = split_df[1].str.strip()
        df['study'] = study

        return df

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
        fields_dict = set(red.keys())

        if not fulldata:
            # skip 1st row of auth which holds parent info
            fields_dict.remove('hcpdparent')

        return pd.concat([self.get_all_rows(name) for name in fields_dict], sort=True)

    def getfullredcapdata(self):
        return self.getredcapdata(True)

    def getredcapfields(self, fieldlist, study):
        """"
        Downloads requested fields from Redcap databases specified by details in redcapconfig file
        Returns panda dataframe with fields 'study', 'Subject_ID, 'subject', and 'flagged', where 'Subject_ID' is the
        patient id in the database of interest (sometimes called subject_id, parent_id) as well as requested fields.
        subject is this same id stripped of underscores or flags like 'excluded' to make it easier to merge
        flagged contains the extra characters other than the id so you can keep track of who should NOT be uploaded to NDA
        or elsewwhere shared
        """
        studydata = self.get_all_rows(study, fieldlist)
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
        studyids = pd.DataFrame()

        for name in red.keys():
            df = self.get_all_rows(name, subjectOnly=True)
            df.columns = ['subject_id', 'subject', 'flagged', 'study']
            studyids = studyids.append(df)

        return studyids
