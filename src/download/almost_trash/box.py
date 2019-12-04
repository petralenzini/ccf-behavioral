import os
import sys
from multiprocessing.dummy import Pool

from boxsdk import JWTAuth, OAuth2, Client

cur_dir = os.path.dirname(os.path.abspath(__file__))
# os.path.join('/', 'tmp', 'ccf-nda-cache')
default_cache = "/data/intradb/tmp/box2nda_cache"


class LifespanBox:
    def __init__(self, cache=default_cache, user='Lifespan Automation'):
        self.user = user
        self.cache = cache
        if not os.path.exists(cache):
            os.mkdir(cache)
        self.client = self.get_client()
        # self.client = self.get_dev_client()

    def get_client(self):
        #private_key_path = '/Users/michael/.ssh/box_private_key.pem'
        auth = JWTAuth.from_settings_file(
            "/data/intradb/home/.boxApp/config.json")
        # auth = JWTAuth(
        #    client_id='6tbnbadnd39w9ni14qtudmr6i5awjyqh',
        #    client_secret='J78YrWpvwG9bAYlkgmcFK9BUkYX5L2ig',
        #    enterprise_id='280321',
        #    jwt_key_id='6nb80fdd',
        #    rsa_private_key_file_sys_path='/data/intradb/home/.ssh/box_private_key.pem',
        #    rsa_private_key_passphrase=b'e64f408b853a70f2f66a05944236dcf1'
        # )

        # access_token = auth.authenticate_instance()
        admin_client = Client(auth)

        lifespan_user = None
        # lifespan_user = client.create_user('Lifespan Automation')
        for user in admin_client.users():
            if user.name == self.user:
                lifespan_user = user
                # print(lifespan_user.login)

        if not lifespan_user:
            print(self.user + ' user was not found. Exiting...')
            sys.exit(-1)

        return admin_client.as_user(lifespan_user)

    def get_dev_client(self):
        # Dev access token, active for 1 hour. Get new token here:
        # https://wustl.app.box.com/developers/console/app/333873/configuration
        auth = OAuth2(
            client_id='',
            client_secret='',
            access_token=''
        )
        return Client(auth)

    def folder_info(self, folder_id):
        f = self.client.folder(folder_id=str(folder_id)).get()
        print('folder owner: ' + f.owned_by['login'])
        print('folder name: ' + f['name'])

    def get_files(self, folder_id, pattern=None, maxfiles=None):
        """ Gets all files in a folder matching pattern up to maxfiles
        :pattern - Can be any string and can contain '*' for wildcards
        :maxfiles - May return slightly more than this due to the offset value
            and pattern matching
        """
        limit = 1000
        if maxfiles and maxfiles < limit:
            limit = maxfiles
        offset = 0
        root_folder = self.client.folder(folder_id=str(folder_id))
        files = []

        while True:
            items = root_folder.get_items(limit=limit, offset=offset)

            for f in items:
                if f.type != 'file':
                    continue
                if not pattern:
                    files.append(f)
                elif self._match(f.name, pattern):
                    files.append(f)

            # We either exhausted the listing or have reached maxfiles
            if not items:
                break
            if maxfiles and len(files) >= maxfiles:
                break

            offset += limit

        return files

    def search(
            self,
            pattern,
            limit=100,
            maxresults=1000,
            exclude=None,
            ancestor_folders=None,
            file_extensions=None):
        """
        Extends box search to narrow down based on glob like pattern
        Exclusions can be specified as comma separated string, like 'Not,This'
        """
        offset = 0
        results = []

#        while True:
#            print('looking for "{}" ...'.format(pattern))
#            result = self.client.search().query(pattern, limit=limit, offset=offset, ancestor_folders=ancestor_folders)
#            results.extend(result)
#
#            if not result:
#                break
#            if maxresults and len(results) >= maxresults:
#                break
#
#            offset += limit

        print('looking for "{}" ...'.format(pattern))
        result = self.client.search().query(
            pattern,
            limit=limit,
            offset=offset,
            ancestor_folders=ancestor_folders,
            file_extensions=file_extensions)
        results.extend(result)

        matches = []

        for r in results:
            match = True
            for substr in pattern.split('*'):
                if substr not in r.name:
                    match = False
            if match:  # and exclude and exclude not in r.name:
                if not exclude:
                    matches.append(r)
                else:
                    exclusions = exclude.split(',')
                    included = True
                    for exclusion in exclusions:
                        if exclusion in r.name:
                            included = False
                    if included:
                        matches.append(r)

        return matches

    def download_file(self, file_id):
        """
        Downloads a single file to cache space or provided directory
        """

        f = self.client.file(file_id=str(file_id))
        # print(dir(f))
        print(f.get().name)
        file_path = os.path.join(self.cache, f.get().name)

        with open(file_path, 'wb') as out:
            out.write(f.content())

        return f

    def download_files(self, file_ids, directory=None, workers=20):
        """
        Takes a list of file ids and downloads them all to cache space or user
        specified directory
        """
        if directory:
            self.cache = directory
        pool = Pool(workers)
        pool.map(self.download_file, file_ids)
        pool.close()
        pool.join()
        # Euivalent to this for loop
        # for f in file_ids:
        #     self.download_file(f)

    # def upload_file(self, source_path, folder_id):
    #     # filename = os.path.basename(source_path)
    #     f = self.client.folder(str(folder_id)).upload(source_path)
    #     print(f)

    def update_file(self, file_id, file_path):
        f = self.client.file(str(file_id))
        f.update_contents(file_path)

    @staticmethod
    def _match(string, pattern):
        match = True
        for substr in pattern.split('*'):
            # Skip "empty" matches
            if not substr:
                continue

            if substr not in string:
                # print(substr)
                match = False
        return match


if __name__ == '__main__':
    box = LifespanBox()

    # print(dir(box.client.folder('0')))

    # box.get_files(42902161768, '_Assessment_Scores_')

    # box.download_file(254256599622)

    # updated_file = os.path.join(box.cache, 'toolbox_combined.csv')
    # box.update_file('286624284128', updated_file)

    # f = box.client.file(file_id='286636032193')
    # print(f.get()['name'])

    # results = box.search(pattern='*KSADS*Screener*.xlsx', exclude='Key,MRH')
    # print(len(results))
    # for r in results:
    #     print(r)

    results = box.search(
        pattern='-Aging_scores.csv',
        limit=10,
        maxresults=10
    )
    for r in results[0:49]:
        print(r)
    print('^ {} results'.format(len(results)))
