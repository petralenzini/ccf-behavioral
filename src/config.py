import os
import yaml

with open('./config.yml', 'r') as fd:
    config = yaml.load(fd, Loader=yaml.BaseLoader)

for ds in config['dirs'].values():
    for name, val in ds.items():
        if name != "root":
            path = os.path.join(ds['root'], val)
            os.makedirs(path, exist_ok = True)
            ds[name] = path


