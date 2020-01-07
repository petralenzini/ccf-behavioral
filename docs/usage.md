# Usage

To use this code, make sure that your virtualenv has all required dependencies in the requirements.txt and that it is active.

There are two steps/scripts for integrating each data source:
1. The `Append_(source)_.py` scripts are run first. They download the new data
   and append it to the end of the existing data in box.

2. The `ksads_v2.py/penncnp.py` scripts are then run. These integrate the data
   into other files and perform quality check.

## KSADS
##### Optional
Set your credentials in `/creds/KSADS.yml`,
for example:
```yaml
user: email@example.org
password: password001
```

Download the ksads data and append to the box file by running:
```shell
(ccf) $ ./AppendKSADS.py
```
or if a credentials file was not created, you can pass credentials as parameters
```shell
(ccf) $ ./AppendKSADS.py -u user@email.com -p password002
```

Now run the integration script
```shell
(ccf) $ ./ksads_v2.py
```

## PennCNP
##### Optional
Set your credentials in `/creds/PennCNP.yml`,
for example:
```yaml
user: email@example.org
password: password001
```

Download the ksads data and append to the box file by running:
```shell
(ccf) $ ./AppendPennCNP.py
```
or if a credentials file was not created, you can pass credentials as parameters
```shell
(ccf) $ ./AppendPennCNP.py -u user@email.com -p password002
```


Now run the integration script
```shell
(ccf) $ ./penncnp.py
```
