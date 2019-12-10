import yaml

with open('./config.yml', 'r') as fd:
    config = yaml.load(fd, Loader=yaml.BaseLoader)
