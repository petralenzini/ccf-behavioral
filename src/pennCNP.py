#!/usr/bin/python3
import argparse
import os
import datetime
import subprocess

import pandas as pd
from config import config

from download.box import LifespanBox
from download.redcap import Redcap
from download.ksads import KSADS

# verbose = False
verbose = True
snapshotdate = datetime.datetime.today().strftime('%m_%d_%Y')
ksads_cache_path = config['dirs']['cache']['ksads']
# this will be the place to save any snapshots on the nrg servers
store_space = config['dirs']['store']['ksads']

# connect to Box
box = LifespanBox(cache=ksads_cache_path, config_file=config['box'])
redcap = Redcap(config['redcap']['config'])
assessments = config['Assessments']
sites = config['Sites']



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
    ksads = KSADS(user, password, cache='./cache/')


    for site, values in sites.items():
        for assessment_type in ['Intro', 'Screener', 'Supplement']:
            # read the files into b, k
            ksads_path = ksads.download_file(values['ksads.net'], assessment_type.lower())
            site_file = values[assessment_type]
            box_path = box.downloadFile(site_file)

            b = pd.read_excel(box_path)
            k = pd.read_excel(ksads_path)

            b.columns = b.columns.astype(str)
            k.columns = k.columns.astype(str)
            b.ID = b.ID.astype(int)
            k.ID = k.ID.astype(int)

            b = b.sort_values('ID').reset_index(drop=True)
            k = k.sort_values('ID').reset_index(drop=True)

            # find last row in b[ox]
            # find matching index location in k[sads] version
            last_row_id = b.iloc[-1].ID
            match_idx = k[k.ID == last_row_id].index[0]

            # append missing rows from k onto b
            # but since using an additional -5 as buffer,
            # need to remove the duplicates
            additional_rows = k.iloc[match_idx - 5:]
            combined = pd.concat([b, additional_rows], sort=False)
            combined.drop_duplicates('ID', inplace=True)

            # save to file, upload
            date = datetime.datetime.today().strftime('%Y%b%d')
            combined_path = os.path.join(
                './merged/',
                '%s %s %s.xlsx' % (site, assessment_type, date)
            )
            combined.to_excel(combined_path, index=False)
            box.update_file(site_file, combined_path)


if __name__ == '__main__':
    main()
