from download.box import LifespanBox


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
    def parse_name(filename):
        if not filename.endswith('.csv'):
            return None, None

        subject = filename[:10]

        filename = filename.upper()
        if 'V4' in filename:
            visit = '4'
        elif 'V3' in filename:
            visit = '3'
        elif 'V2' in filename:
            visit = '2'
        elif 'V1' in filename:
            visit = '1'
        else:
            visit = None

        return subject, visit

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

    def get_updates(self, files, preexisting):
        updates = {}
        for fileid, v in files.items():
            fileid = int(fileid)
            sha1 = v['sha1']
            name = v['filename']
            subjectid, visit = self.parse_name(name)
            current = {
                'fileid': fileid,
                'sha1': sha1,
                'filename': name,
                'subjectid': subjectid,
                'visit': visit
            }

            if fileid in preexisting:
                pre = preexisting[fileid]

                if sha1 != pre['sha1']:
                    # update content
                    print('ReReading', fileid, subjectid, visit)
                    content = self.box.read_text(fileid)
                    current['data'] = self.csv(content)
                    current['assessment'] = self.detect_assessment(content)
                    current['created'] = self.box.getFileById(fileid).get().content_created_at

                subjectid_old, visit_old = self.parse_name(pre['filename'])

                if subjectid != subjectid_old:
                    # data now corresponds to different subject_id
                    # delete from old subject_id
                    # transfer to new subject_id
                    current['previous_subjectid'] = subjectid_old

                if visit != visit_old:
                    # data now corresponds to different subject_id
                    # delete from old subject_id
                    # transfer to new subject_id
                    current['previous_visit'] = visit_old


            else:
                # download fresh data
                content = self.box.read_text(fileid)
                current['data'] = self.csv(content)
                current['assessment'] = self.detect_assessment(content)
                current['created'] = self.box.getFileById(fileid).get().content_created_at

            if len(current) > 5:
                updates[fileid] = current

        return updates
