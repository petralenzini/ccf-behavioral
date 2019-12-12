#!/usr/bin/python3
import argparse
import os
import subprocess
from datetime import datetime

from robobrowser import RoboBrowser


def main():
    parser = argparse.ArgumentParser(description="Downloads the data from KSADS.net")
    user_group = parser.add_mutually_exclusive_group(required=True)
    user_group.add_argument("-u", "--user", type=str, help="username")
    user_group.add_argument("-U", "--userexec", metavar="EXEC", type=str, help="run command to get username")
    password_group = parser.add_mutually_exclusive_group(required=True)
    password_group.add_argument("-p", "--password", type=str, help="password")
    password_group.add_argument("-P", "--passwordexec", metavar="EXEC", type=str, help="run command to get password")

    args = parser.parse_args()
    user = subprocess.check_output(args.userexec, shell=True).decode() \
        if args.userexec else \
        args.user

    password = subprocess.check_output(args.passwordexec, shell=True).decode() \
        if args.passwordexec else \
        args.password

    user = user.strip()
    password = password.strip()
    ksads = KSADS(user, password)
    ksads.downloadAllFiles()


default_cache = './cache/'
class KSADS:
    def __init__(self, user, password, cache=default_cache):
        self.user = user
        self.password = password
        self.cache = cache
        if not os.path.exists(cache):
            os.mkdir(cache)
        self.browser = RoboBrowser(history=True, timeout=6000, parser="lxml")
        self.loggedin = False

    def login(self):
        browser = self.browser
        browser.open('https://ksads.net/Login.aspx')
        form = browser.get_form('form1')
        form['txtUsername'].value = self.user
        form['txtPassword'].value = self.password
        browser.submit_form(form)

        if browser.response.url == 'https://ksads.net/Login.aspx':
            print('Incorrect credentials provided')
            self.loggedin = False
            return False
        else:
            print('Logged in.')
            self.loggedin = True
            return True

    def download(self, site, studytype):
        if not self.loggedin:
            if not self.login():
                return
        possible_sites = [34,36,37,38]
        possible_types = ['intro','screener','supplement']
        if site not in possible_sites:
            Exception('Site number not valid')

        if site not in possible_types:
            Exception('Type not valid')

        browser = self.browser

        # submit the report "type"
        browser.open('https://ksads.net/Report/OverallReport.aspx')
        form = browser.get_form('form1')
        form['ddlGroupName'].value = site
        form['chkUserType'].value = studytype
        browser.submit_form(form, form['btnexecute'])

        # request the results
        form = browser.get_form('form1')
        form['ddlGroupName'].value = site
        form['chkUserType'].value = studytype
        browser.submit_form(form, form['btnexportexcel'])

        # save results to file
        if browser.response.ok:
            return browser.response.content

    def download_file(self, site, studytype, overwrite=False, template="{site}_{type}_{date}.xlsx"):
        timestamp = datetime.today().strftime('_%m_%d_%Y')
        file_name = template.format(site=site, type=studytype, date=timestamp)
        file_name = os.path.join(self.cache, file_name)

        if not overwrite and os.path.exists(file_name):
            print('File already exists, skipping  %s'%(file_name))
            return file_name

        # else continue
        content = self.download(site, studytype)

        # save results to file
        if content:
            print('Saving ' + file_name)
            with open(file_name, "wb+") as fd:
                fd.write(content)

            return file_name


    def downloadAllFiles(self):
        """
        Download the KSADS excel files. Returns the list of files downloaded.
        """

        browser = self.browser

        # go through ever iteration of study site/type
        browser.open('https://ksads.net/Report/OverallReport.aspx')
        form = browser.get_form('form1')

        # skip the first studysite option which is just "Select an option"
        sites = form['ddlGroupName'].options[1:]
        studytypes = form['chkUserType'].options

        # the list of files that were downloaded, used as return value
        files = []

        for studysite in sites:
            for studytype in studytypes:
                file_name = self.download_file(studysite, studytype)
                files.append(file_name)

        return files


if __name__ == '__main__':
    main()
