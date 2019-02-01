import os
import time
import configparser
import requests


class Validation:
    def __init__(self):
        config = configparser.ConfigParser()
        path = os.path.dirname(os.path.realpath(__file__))
        config_file = os.path.join(path, 'settings.cfg')
        config.read(config_file)

        username = config.get('User', 'username')
        password = config.get('User', 'password')
        self.url = config.get('Endpoints', 'validation')
        self.auth = (username, password)
        # self.csv = None
        self.id = None

    def submit_csv(self, file_path):
        with open(file_path) as f:
            data = f.read()

        headers = {'content-type': 'text/csv'}

        r = requests.post(self.url, data=data, auth=self.auth, headers=headers)
        self.id = r.json()['id']
        return self.id

    def get_status(self, val_id):
        validation_url = self.url + '/' + val_id

        while True:
            r = requests.get(validation_url, auth=self.auth)
            if r.json()['done']:
                break
            print('Waiting for validation to complete')
            time.sleep(1)

        return r.json()


if __name__ == '__main__':
    validator = Validation()
    d = '/Users/michael/Dropbox/Dev/nda/ccf-nda-behavioral/cache/qinteractive/'
    f = 'Q_WISC_Combined_Output.csv'
    validation_id = validator.submit_csv(d + f)
    results = validator.get_status(validation_id)
    print(results['status'])

    for warning, details in results['warnings'].items():
        print(warning)
        print("{} items".format(len(details)))

    for error, details in results['errors'].items():
        print(error)
        for e in details:
            print(e)
