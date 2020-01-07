# Usage

To use this code, make sure that your virtualenv has all required dependencies in the requirements.txt and that it is active.

## KSADS
Set your credentials in `/creds/KSADS.yml`, for example:
```yaml
user: email@example.org
password: password001
```

Download the ksads data and append to the box file by running:
```shell
(ccf) $ ./AppendKSADS.py
```
