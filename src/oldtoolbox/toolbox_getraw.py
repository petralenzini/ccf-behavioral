import os, datetime
import csv
import pycurl
import sys
import shutil
from openpyxl import load_workbook
import pandas as pd
import download.box

from download.box import LifespanBox

verbose = True
#verbose = False
snapshotdate = datetime.datetime.today().strftime('%m_%d_%Y')

root_cache='/data/intradb/tmp/box2nda_cache/'
cache_space = os.path.join(root_cache, 'toolbox')
try: 
    os.mkdir(cache_space)
except:
    print("cache already exists")


root_store = '/home/shared/HCP/hcpinternal/ccf-nda-behavioral/store/'
store_space = os.path.join(root_store, 'toolbox') #this will be the place to save any snapshots on the nrg servers
try: 
    os.mkdir(store_space) #look for store space before creating it here
except:
    print("store already exists")

#connect to Box
box = LifespanBox(cache=cache_space)

endpointfolder=42957934974
assessmentsfolder=42902161768


cat_filescores = os.path.join(store_space,'CatToolBoxEndpointFiles_Scores.csv')
cat_fileraw = os.path.join(store_space,'CatToolBoxEndpointFiles_Data.csv')
cat_fileregistr= os.path.join(store_space,'CatToolBoxEndpointFiles_Registration.csv')

#new_box_files= os.path.join(cache_space,'AllBoxFiles_Toolbox.csv')

#funky files have csv extension in the middle of the name...still a csv...can use pandas to read
#testep=box.download_file(477284520881)
#file_path = os.path.join(cache_space, testep.get().name)
#scorefile=pd.read_csv(file_path,header=0,low_memory=False)
#testdata=box.download_file(477285060605)
#file_path2 = os.path.join(cache_space, testdata.get().name)
#rawfile=pd.read_csv(file_path2,header=0,low_memory=False)

files,folders = foldercontents(endpointfolder)

#compare allboxfiles to list in store of already processed files -- not done yet...
######################################
alreadyscores = pd.read_csv(cat_filescores,low_memory=False)
alreadyraw = pd.read_csv(cat_fileraw,low_memory=False)
alreadyreg = pd.read_csv(cat_fileregistr,low_memory=False)

processed1=alreadyscores[['file_id','raw_cat_date']].copy()
processed2=alreadyraw[['file_id','raw_cat_date']].copy()
processed3=alreadyreg[['file_id','raw_cat_date']].copy()
processed=pd.concat([processed1,processed2,processed3],axis=0)
processed.sort_values("file_id",inplace=True)
processed.drop_duplicates(subset='file_id',keep='first', inplace=True)

files['file_id']=files.file_id.astype('int')

files4process=pd.merge(files,processed,on='file_id',how='left')
files4process=files4process.loc[files4process.raw_cat_date.isnull()==True].copy()

files4process.drop(columns=['raw_cat_date'],inplace=True)
files4process['file_id']=files4process.file_id.astype('str')
print('Found '+str(len(files4process.file_id))+' new files in Box/Endpoints folder on ' + snapshotdate)

#download everything not already processed to cache 
box.download_files(files4process.file_id)
#startdl=files.head(50)
#box.download_files(startdl.file_id)

scoresfiles=files4process.loc[files4process.filename.str.contains('Scores')]
rawdatafiles=files4process.loc[files4process.filename.str.contains('Assessment') & ~(files4process.filename.str.contains('Scores'))]
regfiles=files4process.loc[files.filename.str.contains('Registration')]

 
scoresinit=catcontents(scoresfiles)
rawdatainit=catcontents(rawdatafiles)
regfilesinit=catcontents(regfiles)

#append to already files and send to csv
scoresinitfull=pd.concat([alreadyscores,scoresinit],axis=0)
rawdatainitfull=pd.concat([alreadyraw,rawdatainit],axis=0)
regfilesinitfull=pd.concat([alreadyreg,regfilesinit],axis=0)

#scoresinitfull=scoresinitfull.loc[scoresinitfull.PIN.isnull()==False].copy()
#rawdatainitfull=rawdatainitfull.loc[rawdatainitfull.PIN.isnull()==False].copy()
#regfilesinitfull=regfilesinitfull.loc[regfilesinitfull.PIN.isnull()==False].copy()
scoresinitfull.to_csv(cat_filescores,index=False)
rawdatainitfull.to_csv(cat_fileraw,index=False)
regfilesinitfull.to_csv(cat_fileregistr,index=False)

#first run had to upload file - subsequent runs just update
#box.upload_file(cat_filescores,42902124903) 
#box.upload_file(cat_fileraw,42902124903) 
#box.upload_file(cat_fileregistr,42902124903) 

box.update_file(477893784271, (cat_filescores)
box.update_file(477884281447, (cat_fileraw)
box.update_file(477887477048, (cat_fileregistr)

shutil.rmtree(box.cache)


def catcontents(files): #dataframe that has filename and file_id as columns
    scoresfiles=files.copy()
    scoresinit=pd.DataFrame()
    for i in scoresfiles.filename:
        filepath=os.path.join(cache_space,i)
        filenum=scoresfiles.loc[scoresfiles.filename==i,'file_id']
        try:
            temp=pd.read_csv(filepath,header=0,low_memory=False)
            temp['filename']=i
            temp['file_id']=pd.Series(int(filenum.values[0]),index=temp.index)
            temp['raw_cat_date']=snapshotdate
            scoresinit=pd.concat([scoresinit,temp],axis=0,sort=False)
        except:
            print(filepath+' wouldnt import')
            temp=pd.DataFrame()
            temp['filename']=pd.Series(i,index=[0]) 
            temp['file_id']=pd.Series(int(filenum.values[0]),index=[0])
            temp['raw_cat_date']=snapshotdate
            scoresinit=pd.concat([scoresinit,temp],axis=0,sort=False)
    return scoresinit



def foldercontents(folder_id):
    filelist=[]
    fileidlist=[]
    folderlist=[]
    folderidlist=[]
    WUlist=box.client.folder(folder_id=folder_id).get_items(limit=None, offset=0, marker=None, use_marker=False, sort=None, direction=None, fields=None)
    for item in WUlist:
        if item.type == 'file':
            filelist.append(item.name)
            fileidlist.append(item.id)
        if item.type == 'folder':
            folderlist.append(item.name)
            folderidlist.append(item.id)
    files=pd.DataFrame({'filename':filelist, 'file_id':fileidlist})
    folders=pd.DataFrame({'foldername':folderlist, 'folder_id':folderidlist})
    return files,folders

