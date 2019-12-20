#!/usr/bin/python3

import argparse
import os
import datetime
import subprocess

import pandas as pd
from config import config

from download.box import LifespanBox
from download.pennCNP import PennCNP
from download.redcap import Redcap

# verbose = False
verbose = True
snapshotdate = datetime.datetime.today().strftime('%m_%d_%Y')
ksads_cache_path = config['dirs']['cache']['ksads']

# connect to Box
box = LifespanBox(cache=ksads_cache_path, config_file=config['box'])
site_file = config['PennCNP']



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
    cnp = PennCNP(user, password, cache='./cache/').get()
    box_path = box.downloadFile(site_file)
    boxdf = pd.read_excel(box_path)
    combined = append(boxdf, cnp)


    date = datetime.datetime.today().strftime('%Y%b%d')
    combined_path = os.path.join(
        './merged/',
        'HCA-HCD_AllSites_CNP_%s.xlsx' % (date)
    )
    combined.to_excel(combined_path, index=False)
    box.update_file(site_file, combined_path)

def append(old_df, new_df, colname=None):
    # if not specified, use the first column name
    if not colname:
        colname = old_df.columns[0]

    # make sure any int columns are converted to str
    old_df.columns = old_df.columns.astype(str)
    new_df.columns = new_df.columns.astype(str)

    sort_col = colname + '_sorted'
    # if multiple IDs are specified extract only the first one, and convert to int
    old_df[sort_col] = old_df[colname].astype(str).str.extract('^(\d+)').astype(int)
    new_df[sort_col] = new_df[colname].astype(str).str.extract('^(\d+)').astype(int)

    old_df = old_df.sort_values(sort_col).reset_index(drop=True)
    new_df = new_df.sort_values(sort_col).reset_index(drop=True)

    # find last row in b[ox]
    # find matching index location in k[sads] version
    last_row_id = old_df.iloc[-1][sort_col]
    match_idx = new_df[new_df[sort_col] == last_row_id].index[0]

    # append missing rows from k onto b
    # but since using an additional -5 as buffer,
    # need to remove the duplicates
    additional_rows = new_df.iloc[match_idx - 5:]
    combined = pd.concat([old_df, additional_rows], sort=False)
    combined.drop_duplicates(sort_col, inplace=True)
    combined.drop(columns=sort_col, inplace=True)

    return combined


if __name__ == '__main__':
    main()
