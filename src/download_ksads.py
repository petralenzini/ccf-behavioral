#!/usr/bin/python3
import argparse
import subprocess
from robobrowser import RoboBrowser


def main():
    parser = argparse.ArgumentParser(description="Downloads the data from KSADS.net")
    user_group = parser.add_mutually_exclusive_group()
    user_group.add_argument("-u", "--user", type=str, help="username")
    user_group.add_argument("-U", "--userexec", metavar="EXEC", type=str, help="run command to get username")
    password_group = parser.add_mutually_exclusive_group()
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
    download_ksads(user, password)


def download_ksads(user, password):
    """
    Download the KSADS excel files. Returns the list of files downloaded.
    """
    browser = RoboBrowser(history=True, timeout=6000, parser="lxml")

    # login
    browser.open('https://ksads.net/Login.aspx')
    form = browser.get_form('form1')
    form['txtUsername'].value = user
    form['txtPassword'].value = password
    browser.submit_form(form)

    if browser.response.url == 'https://ksads.net/Login.aspx':
        print('Incorrect credentials provided')
        return []
    # else:
    print('Logged in.')

    # go through ever iteration of study site/type
    browser.open('https://ksads.net/Report/OverallReport.aspx')
    form = browser.get_form('form1')

    # the list of files that were downloaded, used as return value
    files = []

    # skip the first studysite option which is just "Select an option"
    for studysite in form['ddlGroupName'].options[1:]:

        for studytype in form['chkUserType'].options:

            # submit the report "type"
            browser.open('https://ksads.net/Report/OverallReport.aspx')
            form = browser.get_form('form1')
            form['ddlGroupName'].value = studysite
            form['chkUserType'].value = studytype
            browser.submit_form(form, form['btnexecute'])

            # request the results
            form = browser.get_form('form1')
            form['ddlGroupName'].value = studysite
            form['chkUserType'].value = studytype
            browser.submit_form(form, form['btnexportexcel'])

            # save results to file
            if browser.response.ok:
                file_name = '%s_%s.xlsx' % (studysite, studytype)
                print('Saving ' + file_name)
                with open(file_name, "wb+") as fd:
                    fd.write(browser.response.content)

                files.append(file_name)

    return files


if __name__ == '__main__':
    main()
