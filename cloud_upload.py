try:
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
    import zipfile
    import shutil
    import fastparquet as fp
    import snappy
    #import gzip
    import pandas as pd
    import csv
    from chardet.universaldetector import UniversalDetector
    from collections import defaultdict
    from pyxlsb import open_workbook as open_xlsb
except ModuleNotFoundError as e:
    print(e)
    print('please install missing packages to use this program.')
    print('shutting down')
    quit()


# =============================================================================
# returns all files for a path that are csv or Excel
#
# IN:   path as string
# OUT:  list containing files
# =============================================================================
def files_left(path):
    t1 = glob.glob(''.join([path,'/*.csv']))
    t2 = glob.glob(''.join([path,'/*.xls']))
    t3 = glob.glob(''.join([path,'/*.xlsx']))
    t4 = glob.glob(''.join([path,'/*.xlsb']))
    left_overs = []
    left_overs.extend(t1)
    left_overs.extend(t2)
    left_overs.extend(t3)
    left_overs.extend(t4)
    return left_overs

# =============================================================================
# moves all files that have a header file to the subfolder "abap"
# everything else is left in place
# returns the path where the abap files are stored
# or none if no ABAP files can be found
#
# IN:   path as string
# OUT:  directory as string or None
# =============================================================================
def sort_abap(path):
    t1 = glob.glob(os.path.join(path,'*'))
    for file in t1:
        splt = os.path.basename(file).split('.')
        if len(splt) > 2:
            print(f'found file to rename: {file}')
            if splt[-2] == 'csv':
                newname = '.'.join([os.path.dirname(file), '_'.join(splt[:-2]), splt[-2], splt[-1]])
            else:
                newname = '.'.join([os.path.dirname(file), '_'.join(splt[:-1]), splt[-1]])
            os.rename(file, newname)
    t1 = glob.glob(os.path.join(path,'*'))
    # =========================================================================
    # look for header files in all the csv files
    # =========================================================================
    headers = []
    for i in range(len(t1)):
        t1[i] = os.path.split(t1[i])[1]
        header_name = re.findall('(.*)_HEADER_[0-9]{8}_[0-9]{6}.', t1[i])
        print(t1[i], header_name)
        if header_name:
            headers.append(header_name[0])
    headers = set(headers)
    # =========================================================================
    # move all csv files that have a header file and return the directory where
    #   the files have been moved to.
    #   If no files could be found return None
    # =========================================================================
    abap_dir = os.path.join(path, 'abap')
    if headers or glob.glob(os.path.join(abap_dir, '*')):
        if not os.path.isdir(abap_dir):
            os.mkdir(abap_dir)
        # TODO: check if this can be made in one go (mass folder creation)
        header_dict = dict()
        for header_folder in headers:
            header_path = os.path.join(abap_dir, header_folder)
            if not os.path.isdir(header_path):
                os.mkdir(header_path)
            header_dict[header_folder] = header_path
        for file in t1:
            for header in header_dict.items():
                if ''.join([header[0], '_']) in file:
                    shutil.move(os.path.join(path, file), header[1])
                    break
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
            job_type = "DELTA"
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

def detect_encoding(file):
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
    if enc == 'ascii':
        enc = 'utf-8'
        print('determined encoding to be ascii, using utf-8 nonetheless as this has been less prone to errors in the past.')
    return enc

def test_float(x):
    '''
    Function to determine if an imput can be converted to float
    '''
    try:
        a = float(x)
    except:
        a = None
    if a:
        return True
    else:
        return False

def determine_dialect(file, enc):
    sniffer = csv.Sniffer()
    sniffer.preferred = [';', ',', '\t', '|']
    dialect = ''
    data = []
    counter = 0
    with open(file, mode='r', encoding=enc, errors='replace') as f:
        for line in f:
            data.append(line)
            counter += 1
            if counter == 10:
                break
    try:
        data_str = ''.join(data)
        dialect = sniffer.sniff(data_str)
        delimiter = dialect.delimiter
        quotechar = dialect.quotechar
        escapechar = dialect.escapechar
        header = sniffer.has_header(data_str)
    except:
        print('sniffer was unsuccessful, using a simplistic approach to determine the delimiter and existence of header.')
        line1 = data[0]
        delim = dict()
        for i in [';', ',', '\t', '|']:
            delim[i] = len(line1.split(i))
        delimiter = sorted(delim.items(), key=lambda kv: kv[1])[-1][0]
        quotechar = None
        escapechar = None
        if any(map(test_float, line1.split(delimiter))):
            header = False
        else:
            header = True
    print('delimiter:', delimiter, ' quotechar:', quotechar,
          ' escapechar:', escapechar, ' header:', header)
    return {'delimiter':delimiter, 'quotechar':quotechar,
            'escapechar':escapechar, 'header':header}

def determine_number_format(file, encoding, delimiter):
    counter = 0
    lines = []
    with open(file, mode='r', encoding=encoding) as f:
        while counter < 20:
            lines.append(f.readline())
            counter += 1
    number_format = {'eu': 0, 'us': 0}
    for line in lines:
        fields = line.split(delimiter)
        for field in fields:
            try:
                if ',' in field and '.' in field and len(re.findall('([\d\.,]+)', field)[0]) == len(field):
                    if len(field.split(',')[-1]) < len(field.split('.')[-1]):
                        number_format['eu'] += 1
                    else:
                        number_format['us'] += 1
            except:
                pass
    if number_format['eu'] > number_format['us']:
        return {'thousands': '.', 'decimal': ','}
    else:
        return {'thousands': ',', 'decimal': '.'}

def import_file(file, folder) :
    # determine delimiter of csv file
    # assumes normal encoding of the file
    df = None
    print(ending(file))
    if ending(file) == 'csv':
        # determine encoding and dialect
        enc = detect_encoding(file)
        dialect = determine_dialect(file, enc)
        delimiter = dialect['delimiter']
        quotechar = dialect['quotechar']
        escapechar = dialect['escapechar']
        number_format = determine_number_format(file, enc, delimiter)
        thousand = number_format['thousands']
        dec = number_format['decimal']

        # TODO: make it have 3 tries and just change variables as exception
        # add UnicodeDecodeError open(file, mode='r', encoding=enc, errors='replace') as f:
        try:
            print('start reading csv file')
            df = pd.read_csv(file, low_memory=False, encoding=enc,
                             sep=delimiter, error_bad_lines=False, parse_dates=True,
                             warn_bad_lines=True, quotechar=quotechar, skip_blank_lines=True,
                             escapechar=escapechar, thousands=thousand, decimal=dec)
            print('csv file successfully imported')
        except Exception as f:
            print(f)
            try:
                print('error handling mode')
                time.sleep(1)
                df = pd.read_csv(file, low_memory=False, encoding=enc,
                                 sep=delimiter, error_bad_lines=False, parse_dates=True,
                                 warn_bad_lines=True, quotechar=quotechar, skip_blank_lines=True,
                                 escapechar=escapechar, nrows=200000, thousands=thousand, decimal=dec)
                col_types = dict()
                for i in range(len(df.dtypes)):
                    col_types[i] = df.dtypes[i]
                time.sleep(1)
                df = pd.read_csv(file, encoding=enc, sep=delimiter, error_bad_lines=False,
                                 parse_dates=True, warn_bad_lines=True, quotechar=quotechar,
                                 escapechar=escapechar, chunksize=200, skip_blank_lines=True,
                                 dtype=col_types, thousands=thousand, decimal=dec)
            except Exception as e:
                print('errorhandling failed, unable to read file:', file,
                      '\nerror is', e)
    elif ending(file) == 'xlsb':
        try:
            df_lst = []
            with open_xlsb(file) as wb:
                with wb.get_sheet(1) as sheet:
                    for row in sheet.rows():
                        df_lst.append([item.v for item in row])
            df = pd.DataFrame(df_lst[1:], columns=df_lst[0])
        except ModuleNotFoundError as e:
            print(e)
            print('please install missing packages to use this program.')
            print('shutting down')
            quit()
        except Exception as e:
            print('unable to read', file, 'with the following error:', e)
    else:
        try:
            df = pd.read_excel(file)
        except Exception as e:
            print('unable to read', file, 'with the following error:', e)

    return df

def remove_ending(files):
    if isinstance(files, (str,)):
        files = [files]
    return_list = []
    for file in files:
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
        file_without_ending = remove_ending(os.path.split(file)[1]).replace(' ', '_').replace('.', '_')
        folder_name = ''.join(filter(lambda x: x in allowed,
                                     file_without_ending))
        return_dict[file] = create_folder(path, folder_name)
    return return_dict

def create_folder(path, name):
        fldr = os.path.join(path, name)
        if not os.path.exists(fldr):
            print('create:', fldr)
            os.makedirs(fldr)
        return fldr


def generate_parquet_file(df, folder):
    """
    This function transforms an input dataframe or dataframe iterator into
    parquet files in the specified folder

    Keyword arguments:
    df -- pandas DataFrame or DataFrame iterator
    folder -- folder where the parquet files are created as string

    Return value:
    None
    """
    file = os.path.split(folder)[1]
    if type(df) is pd.DataFrame:
        chunksize = 200000
        for pos in range(0, len(df), chunksize):
            tmp_filename = os.path.join(folder,
                                        ''.join([file, str(pos), '.parquet']))
            print('writing chunk', tmp_filename, 'to disk')
            fp.write(tmp_filename, df.iloc[pos:pos+chunksize,:],
                     compression='SNAPPY', write_index=False, times='int96')
    else:
        #suffix = 0
        chunk_counter = 0
        # this try / except block functions as safeguard against columns that
        # have been cast partially wrong and thereby corrupt the whole dataframe
        # TODO: Make this part recursive, so that it creates a new df which is a sum of all the
        df2 = []
        try:
            for i in df:
                #tmp_filename = os.path.join(folder,
                #                            ''.join([file, str(suffix), '.parquet']))
                #fp.write(tmp_filename, i, compression='SNAPPY', write_index=False, times='int96')
                #print('writing chunk', tmp_filename, 'to disk')
                #suffix += 1
                df2 = df2.append(i)
        except:
            chunk_counter += 1
        if chunk_counter > 0:
            print(str(chunk_counter), 'chunks were lost.')
        df_concat = pd.concat(df2, ignore_index=True)
        generate_parquet_file(df_concat, folder)
# =============================================================================
#                 ___  ___       ___   _   __   _        _____   _   _   __   _   _____   _____   _   _____   __   _
#                /   |/   |     /   | | | |  \ | |      |  ___| | | | | |  \ | | /  ___| |_   _| | | /  _  \ |  \ | |
#               / /|   /| |    / /| | | | |   \| |      | |__   | | | | |   \| | | |       | |   | | | | | | |   \| |
#              / / |__/ | |   / / | | | | | |\   |      |  __|  | | | | | |\   | | |       | |   | | | | | | | |\   |
#             / /       | |  / /  | | | | | | \  |      | |     | |_| | | | \  | | |___    | |   | | | |_| | | | \  |
#            /_/        |_| /_/   |_| |_| |_|  \_|      |_|     \_____/ |_|  \_| \_____|   |_|   |_| \_____/ |_|  \_|
#
# =============================================================================

# Probably not needed anymore
"""
#vertica_commands_file = ''.join([datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S'),'_vertica_commands_file.sql'])
#global null_cols
#null_cols = defaultdict(list)
"""
logname = ''.join([datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S'), '_uploader.log'])

dir_path = os.path.normpath(cl.outputdir)
transformationdir_general = os.path.normpath(cl.inputdir)
if cl.transformation == 1:
    transformationdir = sort_abap(transformationdir_general)
    if transformationdir:
        # ======================================================================
        # TODO: start multiple processes in parallel using: psutil.cpu_count()
        # to achieve this make sort_abap create a subfolder per header
        # ======================================================================
        sample = 'POIUZTREWQLKJHGFDSAMNBVCXYpoiuztrewqlkjhgfdsamnbvcxy0987654321'
        sample_name = ''.join(numpy.random.choice([i for i in sample], size=20))
        jar = glob.glob('connector*.jar')[0]
        transformationfolders = [x for x in glob.glob(os.path.join(transformationdir, '*')) if os.path.isdir(x)]
        while transformationfolders:
            availablememory = str(int(((psutil.virtual_memory().free)/1024.0**2)*0.95))
            current_working_folder = transformationfolders.pop()
            cwd_files = glob.glob(os.path.join(current_working_folder, '*'))
            if all(map(lambda x: ending(x) == 'csv', cwd_files)):
                compression = 'NONE'
            elif any(map(lambda x: ending(x) == '7z', cwd_files)):
                compression = 'SEVEN_ZIP'
            elif any(map(lambda x: ending(x) == 'gzip' or ending(x) == 'gz', cwd_files)):
                compression = 'GZIP'
                """
                for file in cwd_files:
                    if 'HEADER' in file:
                        name = file.split('.')[0]
                        with gzip.open(file, 'rb') as f_in:
                            with open('.'.join([name, 'csv']), 'wb') as f_out:
                                shutil.copyfileobj(f_in, f_out)
                """
            elif any(map(lambda x: ending(x) == 'zip', cwd_files)):
                # TODO: move moving zips to the extraction part so that finished zips
                # don't get mixed up with unfinished zips
                done = create_folder(current_working_folder, 'done')
                for i in glob.glob(os.path.join(current_working_folder, '*.zip')):
                    with zipfile.ZipFile(i, 'r') as zip_ref:
                        zip_ref.extractall(current_working_folder)
                root = current_working_folder
                tree = list(os.walk(root))
                del tree[0]
                while tree:
                    work_item = tree.pop()
                    if work_item[-1]:
                        for i in work_item[-1]:
                            shutil.move(os.path.join(work_item[0], i), root)
                for i in [x for x in glob.glob(os.path.join(current_working_folder, '*')) if os.path.isdir(x)]:
                    if os.path.split(i)[-1] != 'done':
                        shutil.rmtree(i, ignore_errors=True)
                for i in glob.glob(os.path.join(current_working_folder, '*.zip')):
                    shutil.move(i, done)
                compression = 'NONE'
            else:
                print('wrong file format in folder:', current_working_folder)
                continue
            cmdlist = ('java -Xmx', availablememory,
                       'm -jar ', jar, ' convert "',
                       current_working_folder, '" "', dir_path, '" ', compression)

            transforamtioncmd = ''.join(cmdlist)
            print('starting transforamtion with the following command:\n',
                  transforamtioncmd)

            with subprocess.Popen(transforamtioncmd, stdout=subprocess.PIPE,
                                  stderr=subprocess.STDOUT) as proc:
                while proc.poll() is None:
                    data = str(proc.stdout.readline(), 'utf-8')
                    print(data)
                    with open(sample_name, 'a') as tmp_output:
                        tmp_output.write(data)

        # ======================================================================
        # Opening the output file to find the errors and keep them in the log
        # TODO: Determine if this can also be achieved by utilizing stderr
        # ======================================================================
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
            print(file, 'couldn\'t be transformed to parquet')
        print('\n')



if cl.upload == 1:
    url = cl.url

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

    if cl.delta == 1:
        delta = True
    else:
        delta = False
    jobstatus = {}
    dirs = [join(dir_path, f) for f in listdir(dir_path) if os.path.isdir(join(dir_path, f))]
    print('Dirs to be uploaded:\n',dirs)
    uppie = cloud(tenant=tenant, realm=cluster, api_key=apikey)
    for dr in dirs :
        if dr == '__pycache__':
            continue
        # TODO: replace split \\ with os.path.split(file)[1])
        print('\nuploading:', os.path.split(dr)[-1])
        jobhandle = uppie.create_job(pool_id=poolid,
                                     data_connection_id=connectionid,
                                     targetName=os.path.split(dr)[-1],
                                     upsert=delta)
        jobstatus[jobhandle['id']] = False
        uppie.push_new_dir(pool_id=poolid, job_id=jobhandle['id'], dir_path=dr)
        uppie.submit_job(pool_id=poolid, job_id=jobhandle['id'])
    print('''upload done, waiting for jobs to be finished\nyou\'ll get a status\
update every 15 seconds. Logs will be written to:''', logname)
    running = True
    with open(logname, 'a') as fh:
        fh.write('\n\nUpload log:\n')
        while running:
            jobs = uppie.list_jobs(poolid)
            for jobids in jobstatus:
                for i in jobs:
                    try:
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
                                print('job for', i['targetName'], 'still running')
                            break
                    except (KeyboardInterrupt, SystemExit):
                        print('terminating program\n')
                        break
                    except:
                        pass
            if all(status == True for status in jobstatus.values()):
                running = False
            time.sleep(15)
print('all done')
