
# returns dataframe of unique list of ids


def getids(token=token[0], field=field[0], event=event[0]):
    data = {
        'token': token,
        'content': 'record',
        'format': 'csv',
        'type': 'flat',
        'fields[0]': field,
        'events[0]': event,
        'rawOrLabel': 'raw',
        'rawOrLabelHeaders': 'raw',
        'exportCheckboxLabel': 'false',
        'exportSurveyFields': 'false',
        'exportDataAccessGroups': 'false',
        'returnFormat': 'json'
    }
    buf = io.BytesIO()
    ch = pycurl.Curl()
    ch.setopt(
        ch.URL,
        'https://redcap.wustl.edu/redcap/srvrs/prod_v3_1_0_001/redcap/api/')
    ch.setopt(ch.HTTPPOST, list(data.items()))
    ch.setopt(ch.WRITEDATA, buf)
    ch.perform()
    ch.close()
    htmlString = buf.getvalue().decode('UTF-8')
    buf.close()
    parent_ids = pd.DataFrame(htmlString.splitlines(), columns=['Subject_ID'])
    parent_ids = parent_ids.iloc[1:, ]
    parent_ids = parent_ids.loc[~(parent_ids.Subject_ID == '')]
    uniqueids = pd.DataFrame(
        parent_ids.Subject_ID.unique(),
        columns=['Subject_ID'])
    uniqueids['Subject_ID'] = uniqueids.Subject_ID.str.strip('\'"')
    new = uniqueids['Subject_ID'].str.split("_", 1, expand=True)
    uniqueids['subject'] = new[0].str.strip()
    uniqueids['flagged'] = new[1].str.strip()
    return uniqueids


# uniqueparents=uniqueparents.loc[~(uniqueparents.Subject_ID=='test')].copy()
# uniqueparents=uniqueparents.loc[~(uniqueparents.Subject_ID=='hcd')].copy()

# parents=getids(token=token[0],field=field[0],event=event[0])
# kids=getids(token=token[1],field=field[1],event=event[1])
# lateteens=getids(token=token[2],field=field[2],event=event[2])
# hca=getids(token=token[3],field=field[3],event=event[3])
