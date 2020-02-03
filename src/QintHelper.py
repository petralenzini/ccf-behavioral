from download.box import LifespanBox
import pandas as pd
from numpy import nan
import re
from config import LoadSettings
config = LoadSettings()


columnnames = config['Qint']['Columns']
class Qint:
    def __init__(self, box=None):
        if box is None:
            box = LifespanBox()
        self.box = box

    @staticmethod
    def detect_assessment(content):
        if 'RAVLT-Alternate' in content:
            assessment = 'RAVLT2'
        elif 'RAVLT' in content:
            assessment = 'RAVLT'
        elif 'WISC-V' in content:
            assessment = 'WISC'
        elif 'WAIS-IV' in content:
            assessment = 'WAIS'
        elif 'WPPSI-IV' in content:
            assessment = 'WPPSI'
        else:
            assessment = 'Unknown'
        return assessment

    @staticmethod
    def is_csv(filename):
        return filename.lower().endswith('.csv')

    @staticmethod
    def detect_visit(filename):
        visits = sorted(list(map(int,re.findall('[vV](\d)', filename))))
        return visits[-1] if visits else None

    @staticmethod
    def extract_subjectid(filename):
        return filename[:10]

    @staticmethod
    def parse_content(content):
        section_headers = [
            'Subtest,,Raw score',
            'Subtest,,Scaled score',
            'Subtest,Type,Total',  # this not in aging or RAVLT
            'Subtest,,Completion Time (seconds)',
            'Subtest,Type,Yes/No',
            'Item,,Raw score'
        ]
        # Last section header is repeat data except for RAVLT
        if 'RAVLT' in content:
            section_headers.append('Scoring Type,,Scores')

        new_row = []
        capture_flag = False
        for row in content.splitlines():
            row = row.strip(' "')
            if row in section_headers:
                capture_flag = True

            elif row == '':
                capture_flag = False

            elif capture_flag:
                value = row.split(',')[-1].strip()

                if value == '-':
                    value = ''
                new_row.append(value)

        return new_row

    @staticmethod
    def csv(content):
        return ','.join(Qint.parse_content(content))

    def get_data(self, files):
        data = []
        for fileid in files:
            print('.', end='')
            file = self.box.getFileById(fileid).get()
            content = self.box.read_text(fileid)
            assessment = self.detect_assessment(content)
            if assessment == 'RAVLT2':
                assessment = 'RAVLT'
                ravlt2 = 1
            else:
                ravlt2 = 0

            data.append({
                'fileid': fileid,
                'assessment': assessment,
                'ravlt_two': ravlt2,
                'created': file.content_created_at,
                'subjectid': self.extract_subjectid(file.name),
                'visit': self.detect_visit(file.name),
                'data': self.csv(content),
                'sha1': file.sha1,
                'created': file.content_created_at,
		'filename': file.name
                })

        df = pd.DataFrame(data)
        df.ravlt_two = df.ravlt_two.astype('Int64')
        df.visit = df.visit.astype('Int64')
        return df.sort_values('created').reset_index(drop=True)

    def elongate(self, updates):
        db = {}
        for item, columns in columnnames.items():
            x = updates[updates.assessment == item.upper()].reset_index(drop=True)
            csv = pd.DataFrame(
                x.data.str.split(',').tolist(),
                columns=columns
            )

            x = x[['subjectid', 'fileid', 'filename', 'sha1', 'created','assessment', 'visit', 'ravlt_two']]
            x = x.merge(csv, 'left', left_index=True, right_index=True)
            x[item + '_complete'] = 2
            x[item + '_complete'] = x[item + '_complete'].astype('Int64')


            x.visit = x.visit.astype(float).astype('Int64')
            # fix matrix completion and delay completion times
            time_field = 'ravlt_delay_completion' if item == 'ravlt' else item.lower()+'_matrix_completion'
            time_field = x[time_field]
            # if "null", make NaN
            time_field.replace('null', nan, inplace=True)
            # turn durations greater than 1800 to NaN
            time_field.mask(time_field.astype(float) > 1800, inplace=True)
            db[item] = x
        return db

