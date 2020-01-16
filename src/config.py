import argparse
import os
import subprocess

import yaml

def load_yaml(filename):
    if not os.path.exists(filename):
        return {}

    with open(filename, 'r') as fd:
        return yaml.load(fd, Loader=yaml.SafeLoader)

def LoadSettings(useArgs=False):
    config = load_yaml('./config.yml')

    if 'credentials' in config:
        config.update(load_yaml(config['credentials']))

    if useArgs:
        config = __load_args__(config)

    return config


def __load_args__(config):
    parser = argparse.ArgumentParser(description="Download the data")

    user_required = 'user' not in config
    user_group = parser.add_mutually_exclusive_group(required=user_required)
    user_group.add_argument("-u", "--user", type=str, help="username")
    user_group.add_argument("-U", "--userexec", metavar="EXEC", type=str, help="run command to get username")

    pass_required = 'user' not in config
    password_group = parser.add_mutually_exclusive_group(required=pass_required)
    password_group.add_argument("-p", "--password", type=str, help="password")
    password_group.add_argument("-P", "--passwordexec", metavar="EXEC", type=str, help="run command to get password")

    args = parser.parse_args()
    user = subprocess.check_output(args.userexec, shell=True).decode() \
        if args.userexec else \
        args.user

    password = subprocess.check_output(args.passwordexec, shell=True).decode() \
        if args.passwordexec else \
        args.password

    if user:
        config['user'] = user.strip()

    if password:
        config['password'] = password.strip()

    return config


def additional():
    for ds in config['dirs'].values():
        for name, val in ds.items():
            if name != "root":
                path = os.path.join(ds['root'], val)
                os.makedirs(path, exist_ok = True)
                ds[name] = path


