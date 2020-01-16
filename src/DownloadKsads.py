#!/usr/bin/env python3
import os
from datetime import datetime
from robobrowser import RoboBrowser
from config import LoadSettings

browser = RoboBrowser(history=True, timeout=6000, parser="lxml")

config = LoadSettings()
default_cache = './cache/'
templates = config['KSADS']['nomenclature']

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



def download(siteid, studytype, name, overwrite=False):
    # create filename
    timestamp = datetime.today().strftime(templates['date'])
    filename = os.path.join(templates['download_dir'], templates['download_file'])
    filename = filename.format(date=timestamp, site=name, form=studytype)
    filename = os.path.abspath(filename)
    os.makedirs(os.path.dirname(filename), exist_ok=True)


    if not overwrite and os.path.exists(filename):
        print('File already exists, skipping  %s'%(filename))
        return filename


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
        if content:
            print('Saving ' + filename)
            with open(filename, "wb+") as fd:
                fd.write(content)

            return filename


def download_all():
    """
    Download the KSADS excel files. Returns the list of files downloaded.
    """

    studytypes = config['forms']

    # the list of files that were downloaded, used as return value
    files = []

    # go through ever iteration of study site/type
    for form in studytypes:
        for name, siteid in config['siteids'].items():
            filename = download(siteid, form, name)
            files.append(filename)

    return files


if __name__ == '__main__':
    main()
