import argparse
import subprocess
import pandas as pd
import io
import os
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
    penncnp = PennCNP(user, password)
    penncnp.downloadAllFiles()


url = 'https://penncnp.med.upenn.edu/results.pl'
default_cache = './cache/'
class PennCNP:
    def __init__(self, user, password, cache=default_cache):
        self.user = user
        self.password = password
        self.cache = cache
        if not os.path.exists(cache):
            os.mkdir(cache)
        self.browser = RoboBrowser(history=True, timeout=6000, parser="lxml")
        self.login()

    def login(self):
        browser = self.browser
        browser.open(url)
        form = browser.get_form()
        form['adminid'].value = self.user
        form['pwd'].value = self.password
        browser.submit_form(form)


    def download(self):
        """
        Download results to file
        Returns:
            File path
        """
        # save results to file
        df = self.get()
        filename = os.path.join(self.cache, 'pennCNP.csv')
        df.to_csv(filename)
        return filename

    def get(self):
        """
        Download the database from PennCNP
        Returns:
            Pandas.DataFrame
        """
        browser = self.browser
        # request csv report
        data = {"op": "export_report", "search_new": "0", "field_set": "CNP", "advanced": "1", "sort1": "datasetid",
                "sort2": "siteid", "sort3": "famid", "sort4": "subid", "saved_search": "-Select-",
                "exclude_method_cell": "on", "exclude_method_cell_cb": "on", "valid_box_V1": "on", "valid_box_V2": "on",
                "valid_box_V3": "on", "valid_box_VC": "on", "valid_box_N": "on", "valid_box_S": "on",
                "valid_box_V": "on", "valid_box_0": "on", "valid_box_1": "on", "valid_box_2": "on", "valid_box_3": "on",
                "valid_box_4": "on", "valid_box_5": "on", "valid_box_6": "on", "valid_box_7": "on", "valid_box_8": "on",
                "valid_box_9": "on", "valid_box_10": "on", "valid_box_11": "on", "valid_box_12": "on",
                "valid_box_99": "on", "preset": "include_all", "genus": "All",
                "report_name": "117:  HCPLifespan-allimott", "separator": "period", }
        browser.open(url, method='post', data=data)

        # download
        form = browser.get_form()
        browser.submit_form(form)
        csv = browser.response.content

        df = pd.read_csv(io.BytesIO(csv), encoding='utf8')
        # add "HC" prefix
        df.subid = df.subid.where(df.subid.str.startswith('HC'), 'HC' + df.subid)

        return df


if __name__ == '__main__':
    main()
