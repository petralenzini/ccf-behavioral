#!/usr/bin/env python3
import os
from datetime import datetime
from io import BytesIO

import pandas
from robobrowser import RoboBrowser
from config import LoadSettings

browser = RoboBrowser(history=True, timeout=6000, parser="lxml")

config = LoadSettings()['KSADS']
download_dir = config['download_dir']

def main():
    login()
    filelist = download_all()
    #print(filelist)

    return filelist


def login():
    browser.open('https://ksads.net/Login.aspx')
    form = browser.get_form('form1')
    form['txtUsername'].value = config['user']
    form['txtPassword'].value = config['password']
    browser.submit_form(form)

    if browser.response.url == 'https://ksads.net/Login.aspx':
        Exception('Incorrect credentials provided')
        return False
    else:
        print('Logged in.')
        return True



def download(siteid, studytype, name):
    # submit the report "type"
    print('Requesting "%s" from "%s"' % (studytype, name))
    browser.open('https://ksads.net/Report/OverallReport.aspx')
    form = browser.get_form('form1')
    form['ddlGroupName'].value = str(siteid)
    form['chkUserType'].value = studytype
    browser.submit_form(form, form['btnexecute'])

    # request the results
    form = browser.get_form('form1')
    form['ddlGroupName'].value = str(siteid)
    form['chkUserType'].value = studytype
    browser.submit_form(form, form['btnexportexcel'])

    # save results to file
    if browser.response.ok:
        content = browser.response.content
        return pandas.read_excel(BytesIO(content),  parse_dates=['DateofInterview'], infer_datetime_format=True)
    else:
        pandas.DataFrame()


def download_all():
    """
    Download the KSADS excel files. Returns the list of files downloaded.
    """

    studytypes = config['forms']

    # the list of files that were downloaded, used as return value
    files = []

    # go through ever iteration of study site/type
    for studytype in studytypes:
        dfs = []
        for name, siteid in config['siteids'].items():
            dfs.append(download(siteid, studytype, name))

        timestamp = datetime.today().strftime('%Y-%m-%d')

        filename = os.path.join(download_dir, '{date}/{type}.csv')
        filename = filename.format(date=timestamp, type=studytype)
        filename = os.path.abspath(filename)
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        files.append(filename)

        df = pandas.concat(dfs, sort=False).sort_values(['DateofInterview', 'ID'])

        df.columns = ['ksads' + label if isnumeric else label.lower() \
                      for label, isnumeric in zip(df.columns, df.columns.str.isnumeric())]

        df.to_csv(filename, index=False)
        print('Saving file %s' % filename)

    return files


if __name__ == '__main__':
    main()
