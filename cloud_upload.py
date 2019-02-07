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


url = cl.url
logname = ''.join([datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S'),'_uploader.log'])
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

dir_path = cl.outputdir.replace('/', '\\\\')
transforamtiondir = cl.inputdir.replace('/', '\\\\')
if cl.transformation == 1:
    sample = 'POIUZTREWQLKJHGFDSAMNBVCXYpoiuztrewqlkjhgfdsamnbvcxy0987654321POIUZTREWQLKJHGFDSAMNBVCXYpoiuztrewqlkjhgfdsamnbvcxy0987654321POIUZTREWQLKJHGFDSAMNBVCXYpoiuztrewqlkjhgfdsamnbvcxy0987654321'
    availablememory = str(int(((psutil.virtual_memory().free)/1024.0**2)*0.95))
    cmdlist = ('java -Xmx', availablememory,
               'm -jar connector-sap-1.1-SNAPSHOT.jar convert "',
               transforamtiondir, '" "', dir_path, '" NONE')

    transforamtioncmd = ''.join(cmdlist)
    print('starting transforamtion with the following command:\n',
          transforamtioncmd)
    sample_name = ''.join(numpy.random.choice([i for i in sample], size=20))
    with subprocess.Popen(transforamtioncmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT) as proc:
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
            print('transforamtion finished, with errors. Logs have been written to',logname)
        else:
            print('transforamtion finished.')
    os.remove(sample_name)
if cl.upload == 1:
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
    print('upload done, waiting for jobs to be finished\nyou\'ll get a status update every 15 seconds. Logs will be written to:', logname)
    running = True
    with open(logname, 'a') as fh:
        fh.write('\n\nUpload log:\n')
        while running:
            time.sleep(15)
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
                            fh.write(''.join([i,'\n']))
                        else:
                            print('job for',i['targetName'],'still running')
                        break
            if all(status == True for status in jobstatus.values()):
                running = False
print('all done')
