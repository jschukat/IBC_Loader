import requests
from os import listdir
from os.path import isfile, join
import os
import cloud_upload_config as cl
import psutil
import re
import time
import datetime
import subprocess
import numpy
import glob
import shutil
from io import StringIO

import pandas as pd
import pyarrow
import pyarrow.parquet
import csv
from chardet.universaldetector import UniversalDetector
from collections import defaultdict


def files_left(path):
    t1 = glob.glob(''.join([path,'/*.csv']))
    t2 = glob.glob(''.join([path,'/*.xls']))
    t3 = glob.glob(''.join([path,'/*.xlsx']))
    left_overs = []
    left_overs.extend(t1)
    left_overs.extend(t2)
    left_overs.extend(t3)
    return left_overs

# =============================================================================
# moves all files that have a header file to the subfolder "abap"
# everything else is left in place
# returns the path where the abap files are stored
# =============================================================================
def sort_abap(path):

    t1 = glob.glob(''.join([path,'/*.csv']))
    # =========================================================================
    # look for header files in all the csv files
    # =========================================================================
    headers = []
    for i in range(len(t1)):
        t1[i] = os.path.split(t1[i])[1]
        header_name = re.findall('(.*)_HEADER_[0-9]{8}_[0-9]{6}.csv', t1[i])
        print(t1[i], header_name)
        if header_name:
            headers.append(header_name[0])

    # =========================================================================
    # move all csv files that have a header file
    # =========================================================================
    if headers:
        abap_dir = os.path.join(path, 'abap')
        if not os.path.isdir(abap_dir):
            os.mkdir(abap_dir)
        for file in t1:
            for header in headers:
                if header in file:
                    shutil.move(os.path.join(path, file), os.path.join(abap_dir, file))
        return abap_dir
    else:
        return None


class cloud:

    def get_api(self, path):
        return "https://{}.{}.celonis.cloud/{}".format(self.tenant, self.realm,
                                                       path)

    def __init__(self, tenant, realm, api_key):
        self.tenant = tenant
        self.realm = realm
        self.api_key = api_key

    def get_jobs_api(self, pool_id):
        return self.get_api("integration/api/v1/data-push/{}/jobs/"
                            .format(pool_id))

    def get_auth(self):
        return {'authorization': "Bearer {}".format(self.api_key)}

    def list_jobs(self, pool_id):
        api = self.get_jobs_api(pool_id)
        return requests.get(api, headers=self.get_auth()).json()

    def delete_job(self, pool_id, job_id):
        api = self.get_jobs_api(pool_id) + "/{}".format(job_id)
        return requests.delete(api, headers=self.get_auth())

    def create_job(self, pool_id, targetName, data_connection_id,
                   upsert=False):
        api = self.get_jobs_api(pool_id)
        job_type = "REPLACE"
        if upsert:
            job_type = "UPSERT"
        if not data_connection_id:
            payload = {'targetName': targetName, 'type': job_type,
                       'dataPoolId': pool_id}
        else:
            payload = {'targetName': targetName, 'type': job_type,
                       'dataPoolId': pool_id,
                       'connectionId': data_connection_id}
        return requests.post(api, headers=self.get_auth(), json=payload).json()

    def push_new_dir(self, pool_id, job_id, dir_path):
        files = [join(dir_path, f) for f in listdir(dir_path)
                 if isfile(join(dir_path, f))]
        parquet_files = list(filter(lambda f: f.endswith(".parquet"), files))
        for parquet_file in parquet_files:
            print("Uploading chunk {}".format(parquet_file))
            self.push_new_chunk(pool_id, job_id, parquet_file)

    def push_new_chunk(self, pool_id, job_id, file_path):
        api = self.get_jobs_api(pool_id) + "/{}/chunks/upserted".format(job_id)
        upload_file = {"file": open(file_path, "rb")}
        return requests.post(api, files=upload_file, headers=self.get_auth())

    def submit_job(self, pool_id, job_id):
        api = self.get_jobs_api(pool_id) + "/{}/".format(job_id)
        return requests.post(api, headers=self.get_auth())


def import_file(file, folder) :
    # determine delimiter of csv file
    # assumes normal encoding of the file
    print(ending(file))
    if ending(file) == 'csv':
        sniffer = csv.Sniffer()
        sniffer.preferred = [';', ',', '\t']
        dialect = ''

        detector = UniversalDetector()
        detector.reset()

        with open(file, 'rb') as file_detect:
            lines_to_analyze = file_detect.readlines(50000)

        for line in lines_to_analyze:
            detector.feed(line)
            if detector.done: break
        detector.close()
        enc = detector.result['encoding'].lower()
        print('encoding:', enc,'\n')

        with open(file, mode='r', encoding=enc, errors='replace') as f:
            dialect = sniffer.sniff(f.read(4096))
        print('delimiter:', dialect.delimiter, ' quotechar:', dialect.quotechar, ' escapechar:', dialect.escapechar)
        try:
            df = pd.read_csv(file, low_memory=False, encoding=enc, sep=dialect.delimiter, error_bad_lines=False, warn_bad_lines=True, quotechar=dialect.quotechar, escapechar=dialect.escapechar)
        except:
            try:
                print('error handling mode')
                with open(file, mode='r', encoding=enc, errors='replace') as file_backup:
                    data = StringIO(file_backup.read())
                df = pd.read_csv(data, low_memory=False, sep=dialect.delimiter, error_bad_lines=False, warn_bad_lines=True, quotechar=dialect.quotechar, escapechar=dialect.escapechar, chunksize=200000)
            except Exception as e:
                print('errorhandling failed, unable to read file:', file, '\nerror is', e)
    else:
        try:
            df = pd.read_excel(file)
        except Exception as e:
            print('unable to read', file, 'with the following error:', e)
    #convert NULL columns to dtype object
    if type(df) is pd.DataFrame:
        for col in df.columns:
            if all(df[col].isna()):
                df = df.astype(dtype={col:'object'})
                df[col] = 'none'
                null_cols[folder].append(col)
    return df

def remove_ending(files):
    if isinstance(files, (str,)):
        files = [files]
    return_list = []
    for file in files:
        #print('.'.join(file.split('.')[:-1]))
        return_list.append('.'.join(file.split('.')[:-1]))
    if len(return_list) > 1:
        return return_list
    else:
        return return_list[0]

def ending(file):
    return(file.split('.')[-1].lower())

# TODO: Change folder naming to replace dots with underscores and so on
def create_folders(files, path):
    return_dict = {}
    allowed = set('0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_-')
    for file in files:
        folder_name = ''.join(filter(lambda x: x in allowed, remove_ending(os.path.split(file)[1])))
        fldr = os.path.join(path, folder_name)
        return_dict[file] = fldr
        if not os.path.exists(fldr):
            print('create:', fldr)
            os.makedirs(fldr)
    ##print(return_dict)
    return return_dict

# TODO: change to fastparquet
def generate_parquet_file(df, folder):
    if type(df) is pd.DataFrame:
        pyarrowTable = pyarrow.Table.from_pandas(df, preserve_index=False)
        pyarrow.parquet.write_table(pyarrowTable, os.path.join(folder, ''.join([os.path.split(folder)[1],'.parquet'])), use_deprecated_int96_timestamps=True)
    else:
        suffix = 0
        for i in df:
            pyarrowTable = pyarrow.Table.from_pandas(i, preserve_index=False)
            pyarrow.parquet.write_table(pyarrowTable, os.path.join(folder, ''.join([os.path.split(folder)[1], '_', str(suffix), '.parquet'])), use_deprecated_int96_timestamps=True)
            suffix += 1

# =============================================================================
#                 ___  ___       ___   _   __   _        _____   _   _   __   _   _____   _____   _   _____   __   _
#                /   |/   |     /   | | | |  \ | |      |  ___| | | | | |  \ | | /  ___| |_   _| | | /  _  \ |  \ | |
#               / /|   /| |    / /| | | | |   \| |      | |__   | | | | |   \| | | |       | |   | | | | | | |   \| |
#              / / |__/ | |   / / | | | | | |\   |      |  __|  | | | | | |\   | | |       | |   | | | | | | | |\   |
#             / /       | |  / /  | | | | | | \  |      | |     | |_| | | | \  | | |___    | |   | | | |_| | | | \  |
#            /_/        |_| /_/   |_| |_| |_|  \_|      |_|     \_____/ |_|  \_| \_____|   |_|   |_| \_____/ |_|  \_|
#
# =============================================================================

vertica_commands_file = ''.join([datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S'),'_vertica_commands_file.sql'])

global null_cols
null_cols = defaultdict(list)


# TODO: Rewrite all the replacement parts,
#       as Python also works on windows with forward slashes
dir_path = cl.outputdir.replace('/', '\\\\')
transformationdir_general = cl.inputdir.replace('/', '\\\\')
if cl.transformation == 1:

    transformationdir = sort_abap(transformationdir_general)
    if transformationdir:
        sample = '''POIUZTREWQLKJHGFDSAMNBVCXYpoiuztrewqlkjhgfdsamnbvcxy0987654\
321POIUZTREWQLKJHGFDSAMNBVCXYpoiuztrewqlkjhgfdsamnbvcxy0987654321POIUZTREWQLKJH\
GFDSAMNBVCXYpoiuztrewqlkjhgfdsamnbvcxy0987654321'''
        availablememory = str(int(((psutil.virtual_memory().free)/1024.0**2)*0.95))
        cmdlist = ('java -Xmx', availablememory,
                   'm -jar connector-sap.jar convert "',
                   transformationdir, '" "', dir_path, '" NONE')

        transforamtioncmd = ''.join(cmdlist)
        print('starting transforamtion with the following command:\n',
              transforamtioncmd)
        sample_name = ''.join(numpy.random.choice([i for i in sample], size=20))
        with subprocess.Popen(transforamtioncmd, stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT) as proc:
            while proc.poll() is None:
                data = str(proc.stdout.readline(), 'utf-8')
                print(data)
                with open(sample_name, 'a') as tmp_output:
                    tmp_output.write(data)
        with open(sample_name, 'r') as tmp_file:
            tmp_file_read = tmp_file.read()
            error_logs = re.findall(re.compile('\[main\] ERROR(.*?)[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3} \[main\]', re.DOTALL), tmp_file_read)
            if error_logs:
                with open(logname, 'a') as error_log:
                    error_log.write('Transformation errors:\n')
                    for errors in error_logs:
                        error_log.write(errors)
                print('transforamtion finished, with errors. Logs have been written\
                      to',logname)
            else:
                print('transforamtion finished.')
        os.remove(sample_name)



    files = files_left(transformationdir_general)
    print('non abap files about to be transformed:',files)

    folders = create_folders(files, dir_path)
    print('\ncreate_folder completed\n')

    for file in files:
        print('start loop for:', file)
        file_df = import_file(file, folders[file])
        try:
            generate_parquet_file(file_df, folders[file])
        except:
            print(file, 'couldn\'t be processed')
        print('\n')

    if null_cols:
        with open(vertica_commands_file, 'w') as v:
            print('Some columns needed to be modified for the upload to work. After having uploaded the parquet files, please run the code from the following file in Vertica, to rectify this circumstance. "'+vertica_commands_file+'"')
            for i in null_cols:
                for n in null_cols[i]:
                    vertica_statement = ''.join(['UPDATE "', i, '" SET "', n, '" = NULL;\n'])
                    v.write(vertica_statement)



if cl.upload == 1:
    url = cl.url
    logname = ''.join([datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S'),
                                                        '_uploader.log'])
    parts = []
    connectionflag = 1
    try:
        parts.append(re.search('https://([a-z0-9-]+)\.', url).groups()[0])
        parts.append(re.search('\.([eus]+-1)\.celonis', url).groups()[0])
        parts.append(re.search('ui/pools/([a-z0-9-]+)', url).groups()[0])
        try:
            parts.append(re.search('data-connections/[a-z]+/([a-z0-9-]+)', url)
                         .groups()[0])
        except AttributeError:
            connectionflag = 0
    except AttributeError:
        print('this is an unvalid url.')
    print(connectionflag)

    tenant = parts[0]
    cluster = parts[1]
    apikey = cl.apikey
    poolid = parts[2]

    if connectionflag == 1:
        connectionid = parts[3]
    else:
        connectionid = ''

    
    jobstatus = {}
    dirs = [join(dir_path, f) for f in listdir(dir_path) if os.path.isdir(join(dir_path, f))]
    print('Dirs to be uploaded:\n',dirs)
    uppie = cloud(tenant=tenant, realm=cluster, api_key=apikey)
    for dr in dirs :
        if dr == '__pycache__' : continue
        print('\nuploading:',dr.split('\\')[-1])
        jobhandle = uppie.create_job(pool_id=poolid, data_connection_id=connectionid, targetName=dr.split('\\')[-1])
        jobstatus[jobhandle['id']] = False
        uppie.push_new_dir(pool_id=poolid, job_id=jobhandle['id'], dir_path=dr)
        uppie.submit_job(pool_id=poolid, job_id=jobhandle['id'])
    print('upload done, waiting for jobs to be finished\nyou\'ll get a status update every 5 seconds. Logs will be written to:', logname)
    running = True
    with open(logname, 'a') as fh:
        fh.write('\n\nUpload log:\n')
        while running:
            time.sleep(5)
            jobs = uppie.list_jobs(poolid)
            for jobids in jobstatus:
                for i in jobs:
                    if i['id'] == jobids:
                        if i['status'] == 'QUEUED':
                            print('job for',i['targetName'],'queued')
                        elif jobstatus[jobids] == True:
                            pass
                        elif i['status'] == 'DONE':
                            jobstatus[jobids] = True
                            printout = ' '.join([i['targetName'],'was successfully installed in the database'])
                            print(printout)
                            fh.write(''.join([printout,'\n']))
                        elif i['status'] != 'RUNNING':
                            jobstatus[jobids] = True
                            print(i)
                            fh.write(''.join([str(i),'\n']))
                        else:
                            print('job for',i['targetName'],'still running')
                        break
            if all(status == True for status in jobstatus.values()):
                running = False
print('all done')
