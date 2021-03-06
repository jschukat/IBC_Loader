# TODO: integrate _csv.Error Null bytes



import logging
import datetime
import sys

logname = ''.join([datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S'),
                   '_uploader.log'])
FORMAT = '%(asctime)s %(levelname)s %(message)s'
formatter = logging.Formatter(FORMAT)
log_level = logging.INFO
#logging.basicConfig(format=FORMAT, filename=logname, level=logging.INFO)

root = logging.getLogger()
root.setLevel(log_level)

file_handler = logging.FileHandler(filename=logname, mode='a', encoding='utf-8')
file_handler.setLevel(log_level)
file_handler.setFormatter(formatter)
root.addHandler(file_handler)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(log_level)
handler.setFormatter(formatter)
root.addHandler(handler)


class LogFile(object):

    def __init__(self, name=None):
        self.logger = logging.getLogger(name)

    def write(self, msg, level=logging.INFO):
        self.logger.log(level, msg)

    def flush(self):
        for handler in self.logger.handlers:
            handler.flush()


# Redirect stdout and stderr
sys.stdout = LogFile('stdout')
sys.stderr = LogFile('stderr')

try:
    import requests
    from os import listdir
    from os.path import isfile, join
    import os
    import re
    import time
    import subprocess
    import numpy
    import glob
    import zipfile
    import shutil
    import fastparquet as fp
    import pandas as pd
    import csv
    from chardet.universaldetector import UniversalDetector
    from pyxlsb import open_workbook as open_xlsb
    from io import StringIO
    import cloud_upload_config as cl
    from itertools import product
    import copy
    from pathlib import Path
except ModuleNotFoundError as e:
    logging.error(e)
    logging.error('please install missing packages to use this program.')
    print('shutting down')
    quit()

try:
    import psutil
    psutil_import = True
except ModuleNotFoundError as e:
    logging.error(e)
    logging.error('this will initialize the JVM without xmx, if you run into memory issues, please install psutil.')
    psutil_import = False

# =============================================================================
# returns all files for a path that are csv or Excel
#
# IN:   path as string
# OUT:  list containing files
# =============================================================================
def files_left(path):
    t1 = glob.glob(''.join([path, '/*.csv']))
    t2 = glob.glob(''.join([path, '/*.xls']))
    t3 = glob.glob(''.join([path, '/*.xlsx']))
    t4 = glob.glob(''.join([path, '/*.xlsb']))
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
    t1 = Path(path).glob('*')
    # =========================================================================
    # look for header files in all the csv files
    # =========================================================================
    headers = []
    for i in t1:
        header_name = re.findall('(.*)_HEADER_[0-9]{8}_[0-9]{6}.', i.name)
        logging.info(f'{i.name} {header_name}')
        if header_name:
            headers.append(header_name[0])
    headers = set(headers)
    # =========================================================================
    # move all csv files that have a header file and return the directory where
    #   the files have been moved to.
    #   If no files could be found return None
    # =========================================================================
    t1 = Path(path).glob('*')
    abap_dir = Path(path) / 'abap'
    if len(headers) > 0 or len(list(abap_dir.glob('*'))) > 0:
        if not abap_dir.is_dir():
            abap_dir.mkdir()
        # TODO: check if this can be made in one go (mass folder creation)
        header_dict = dict()
        for header_folder in headers:
            header_path = abap_dir / header_folder
            if not header_path.is_dir():
                header_path.mkdir()
            header_dict[header_folder] = header_path
        for file in t1:
            for header in header_dict.items():
                if ''.join([header[0], '_']) in str(file):
                    shutil.move(str(file), header[1])
                    break
        return abap_dir
    else:
        return None


def cleanup_abap_par(abap, par):
    logging.info(f'starting cleanup of {par} with regard to {abap}.')
    slashes = []
    for i in Path(abap).iterdir():
        foldername = i.name
        if len(foldername.split('.')) > 1:
            slashes.append(foldername)
    for folder in slashes:
        new_dir = (Path(par) / folder)
        if new_dir.is_dir():
            shutil.rmtree(str(new_dir))
        new_dir.mkdir()
        files = (Path(par) / folder.replace('.', '/')).glob('**/*.parquet')
        for file in files:
            shutil.move(str(file), str(new_dir))
    cleanup = set([f.split('.')[0] for f in slashes])
    for clean in cleanup:
        shutil.rmtree(str(Path(par) / clean))


class cloud:

    def get_api(self, path):
        return f"https://{self.tenant}.{self.realm}.celonis.cloud/{path}"

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
            logging.debug(f"Uploading chunk {parquet_file}")
            self.push_new_chunk(pool_id, job_id, parquet_file)

    def push_new_chunk(self, pool_id, job_id, file_path):
        api = self.get_jobs_api(pool_id) + "/{}/chunks/upserted".format(job_id)
        upload_file = {"file": open(file_path, "rb")}
        return requests.post(api, files=upload_file, headers=self.get_auth())

    def submit_job(self, pool_id, job_id):
        api = self.get_jobs_api(pool_id) + "/{}/".format(job_id)
        return requests.post(api, headers=self.get_auth())


def detect_encoding(file):
    logging.info(f'starting encoding determination')
    detector = UniversalDetector()
    detector.reset()
    counter = 0
    try:
        with open(file, 'rb') as file_detect:
            for line in file_detect:
                counter += 1
                detector.feed(line)
                if detector.done:
                    break
                elif counter > 50000:
                    break
        detector.close()
        enc = detector.result['encoding'].lower()
        logging.info(f'encoding: {detector.result}')
    except:
        enc = 'utf-8'
    return enc


def test_float(x):
    '''
    Function to determine if an imput can be converted to float
    '''
    try:
        float(x)
        return True
    except ValueError:
        return False


def determine_dialect(file, enc):
    sniffer = csv.Sniffer()
    sniffer.preferred = [';', ',', '\t', '|', '~', ' ']
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
        logging.warning('''sniffer was unsuccessful, using a simplistic approach
                        to determine the delimiter and existence of header.''')
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
    logging.info(f'''delimiter: {delimiter}, quotechar: {quotechar},
                     escapechar: {escapechar}, header: {header}''')
    return {'delimiter': delimiter, 'quotechar': quotechar,
            'escapechar': escapechar, 'header': header}


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


def import_file(file, folder, second_try=False):
    # determine delimiter of csv file
    # assumes normal encoding of the file
    df = None
    logging.debug(f'{ending(file)}')
    if ending(file) == 'csv':
        # determine encoding and dialect
        encoding = detect_encoding(file)
        dialect = determine_dialect(file, encoding)
        delimiter = dialect['delimiter']
        quotechar = dialect['quotechar']
        escapechar = dialect['escapechar']
        if escapechar is None:
            escapechar = '\\'
        if second_try is True:
            quotechar = None
        # TODO: make it have 3 tries and just change variables as exception
        # add UnicodeDecodeError open(file, mode='r', encoding=enc, errors='replace') as f:
        for enc in [encoding, 'utf-8', 'ascii', 'cp1252', 'latin_1', 'iso-8859-1']:
            logging.info(f'trying to import file using encoding: {enc}')
            encoding = enc
            try:
                try:
                    pd_config = {
                                'filepath_or_buffer': file,
                                'encoding': enc,
                                'sep': delimiter,
                                'error_bad_lines': False,
                                'parse_dates': False,
                                'warn_bad_lines': True,
                                'skip_blank_lines': True,
                                'escapechar': escapechar,
                                'chunksize': 200000,
                                'engine': 'python',
                                'keep_default_na': False,
                                }
                    if cl.as_string:
                        pd_config['low_memory'] = True
                        pd_config['dtype'] = str
                    if quotechar is not None and len(quotechar) > 0:
                        pd_config['quotechar'] = quotechar
                    else:
                        pd_config['quoting'] = 3
                    logging.info(f'start reading {file}')
                    df = pd.read_csv(**pd_config)
                    logging.info('csv file successfully imported')
                except MemoryError:
                    logging.error('''ran out of memory, will retry with all
                                     columns of type string''')
                    pd_config['low_memory'] = True
                    pd_config['dtype'] = str
                    df = pd.read_csv(**pd_config)
                    logging.info('csv file successfully imported')
                except UnicodeDecodeError:
                    logging.error('decode failed, trying to use utf-8 instead.')
                    pd_config['encoding'] = 'utf-8'
                    df = pd.read_csv(**pd_config)
                except Exception as f:
                    logging.exception(f'''errorhandling failed, unable to read
                                          file: {file}\nerror is {f}''')
                if type(df) is pd.DataFrame:
                    col_new = []
                    for col in df.columns:
                        for i in [' ', '.', ',', ';', ':']:
                            col = col.replace(i, '_')
                        col_new.append(col)
                    df.columns = col_new
                generation_result = generate_parquet_file(df, folder)
                if generation_result == 2:
                    logging.info(f'trying to cope with {file} in a different way. Encoding: {encoding}, Quotechar: {quotechar}')
                    with open(file, mode='r', encoding=enc, errors='replace') as inp:
                        for line in inp:
                            quote_count = len(re.findall(quotechar, line))
                            break
                    if quote_count > 1:
                        quoted = True
                    else:
                        quoted = False
                    if second_try is False and quoted is False:
                        return import_file(file, folder, True)
                    elif fix_csv_file(file, folder, encoding, quotechar, delimiter, escapechar) == 0:
                        logging.info(f'successfully fixed {file}')
                    else:
                        logging.error(f'{file} couldn\'t be fixed')
                elif generation_result != 0:
                    raise Exception(f'''Parquet generation failed with unknown
                                    error and returned {generation_result},
                                    trying again with different encoding.''')
                return None
            except:
                pass
    elif ending(file) == 'xlsb':
        try:
            df_lst = []
            with open_xlsb(file) as wb:
                with wb.get_sheet(1) as sheet:
                    for row in sheet.rows():
                        df_lst.append([item.v for item in row])
            df = pd.DataFrame(df_lst[1:], columns=df_lst[0])
        except ModuleNotFoundError as e:
            logging.error(e)
            logging.warning('please install missing packages to use this program.')
            print('shutting down')
            quit()
        except Exception as e:
            logging.error(f'unable to read {file} with the following error: {e}')
    else:
        try:
            matches = {}
            pd_config = {
                        'io': file,
                        'sheet_name': None,
                        'keep_default_na': False,
                        }
            if cl.as_string:
                pd_config['dtype'] = str
            df = pd.read_excel(**pd_config)
            for a, b in product(df, df):
                col_a = df[a].columns
                col_b = df[b].columns
                if (len(col_a) == len(col_b)
                    and (len([i for i, j in zip(col_a, col_b) if i == j])
                         == len(col_b))):
                    matches[str(col_b)] = (matches.get(str(col_b), [a, b])
                                           + [a, b])
            for i in matches:
                matches[i] = set(matches[i])
            if (len(matches) == 1
                and len(df) == 1
                and len(copy.deepcopy(matches).popitem()[1]) == len(df)):
                for i in df:
                    df = df[i]
            elif (len(matches) == 1
                  and len(copy.deepcopy(matches).popitem()[1]) == len(df)):
                dfs = []
                for i in df:
                    dfs.append(df[i])
                df = pd.concat(dfs, ignore_index=True)
            else:
                file = Path(file)
                file_name = Path(file).name.split('.')[0]
                folder = Path(folder)
                parent = folder.parent
                folder.rmdir()
                for match in matches:
                    dfs = []
                    sheet_name = list(matches[match])[0]
                    folder_name = f'{file_name}_{sheet_name}'
                    folder_name = folder_name.replace(' ', '_').replace('.', '_')
                    new_folder = create_folder(parent, folder_name)
                    for i in list(filter(lambda n: n in matches[match], df)):
                        dfs.append(df[i])
                    new_df = pd.concat(dfs, ignore_index=True)
                    generate_parquet_file(new_df, new_folder)
                return None
        except Exception as e:
            logging.error(f'unable to read {file} with the following error: {e}')
    generate_parquet_file(df, folder)
    return None


def manipulate_string(buffer, quotechar, sep, seps, escapechar, header, folder, enc):
    text = ''.join(buffer)
    if quotechar is None:
        text = text.replace('"', '')
        text = text.replace("'", "")
    else:
        text = text.replace(quotechar, '')
    text = re.sub(f'^([^{sep}\n]*{sep}){{{seps+1},}}[^{sep}\n]*$', '', text, flags=re.M)
    text = re.sub(f'^([^{sep}\n]*{sep}){{{0},{seps-1}}}[^{sep}\n]*$', '', text, flags=re.M)
    text = StringIO(text)
    pd_config = {
                'filepath_or_buffer': text,
                'encoding': enc,
                'sep': sep,
                'error_bad_lines': False,
                'parse_dates': False,
                'warn_bad_lines': True,
                'skip_blank_lines': True,
                'escapechar': escapechar,
                'chunksize': 400000,
                'engine': 'python',
                'low_memory': True,
                'dtype': str,
                'quoting': 3,
                'names': header,
                'engine': 'python',
                'keep_default_na': False,
                }
    result = pd.read_csv(**pd_config)
    logging.info('writing chunk to disk')
    generate_parquet_file(result, folder)
    logging.info('chunk has been written to disk')


def fix_csv_file(file, folder, enc, quotechar, sep, escapechar):
    logging.info('starting to fix csv')
    try:
        with open(file, mode='r', encoding=enc, errors='replace') as inp:
            for line in inp:
                seps = len(re.findall(sep, line))
                header = line.replace(quotechar, '').replace('\n', '').split(sep)
                logging.info(f'{seps+1} columns were found in the first line.')
                break
        uni, col_new = [], []
        counter = 0
        for col in header:
            for i in [' ', '.', ',', ';', ':']:
                col = col.replace(i, '_')
            col_new.append(col)
        header = col_new
        if len(header) != len(set(header)):
            logging.warning(f'''found {len(header)-len(set(header))} duplicates
                                in header, will rename duplicate columns to
                                prevent future errors.''')
            for i in header:
                counter += 1
                if i not in uni:
                    uni.append(i)
                else:
                    uni.append(str(i)+str(counter))
            header = uni
        counter = 0
        buffer = []
        with open(file, mode='r', encoding=enc, errors='replace') as inp:
            for line in inp:
                buffer.append(line)
                counter += 1
                if counter >= 200000:
                    manipulate_string(buffer, quotechar, sep, seps, escapechar, header, folder, enc)
                    counter = 0
                    buffer.clear()
        manipulate_string(buffer, quotechar, sep, seps, escapechar, header, folder, enc)
        return 0
    except Exception as e:
        logging.error(f'fixing csv failed with: {e}')
        return 1


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


def create_folders(files, path):
    # TODO: Change folder naming to replace dots with underscores and so on
    return_dict = {}
    allowed = set('0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_-')
    for file in files:
        if '.csv.gz' in file:
            file_without_ending = file.replace('.csv.gz', '').replace(' ', '_').replace('.', '_')
        else:
            file_without_ending = remove_ending(os.path.split(file)[1]).replace(' ', '_').replace('.', '_')
        folder_name = ''.join(filter(lambda x: x in allowed,
                                     file_without_ending))
        return_dict[file] = create_folder(path, folder_name)
    return return_dict


def create_folder(path, name):
    fldr = os.path.join(path, name)
    if os.path.exists(fldr):
        logging.info(f'remove existing folder: {fldr}')
        shutil.rmtree(fldr)
    time.sleep(1)
    logging.info(f'create: {fldr}')
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
    try:
        if type(df) is pd.DataFrame:
            col_new = []
            for col in df.columns:
                for i in [' ', '.', ',', ';', ':']:
                    col = col.replace(i, '_')
                col_new.append(col)
            df.columns = col_new
            if cl.as_string:
                df = df.astype(str)
            logging.info('starting to write dataframe to disk')
            chunksize = 400000
            for pos in range(0, len(df), chunksize):
                tmp_filename = os.path.join(folder,
                                            ''.join([file, str(pos), datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S'), '.parquet']))
                logging.debug(f'writing chunk {tmp_filename} to disk')
                try:
                    fp.write(tmp_filename, df.iloc[pos:pos+chunksize,:],
                             compression='SNAPPY', write_index=False, times='int96')
                except Exception as e:
                    logging.exception(f'Got exception {e} while trying to generate the parquet file.')
                    raise
        else:
            logging.info('starting to write textreaderfile to disk')
            suffix = 0
            #chunk_counter = 0
            # this try / except block functions as safeguard against columns that
            # have been cast partially wrong and thereby corrupt the whole dataframe
            # TODO: Make this part recursive, so that it creates a new df which is a sum of all the
            #df2 = []
            for i in df:
                try:
                    tmp_filename = os.path.join(folder,
                                                ''.join([file, str(suffix), datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S'), '.parquet']))
                    fp.write(tmp_filename, i, compression='SNAPPY', write_index=False, times='int96')
                    logging.debug(f'writing chunk {tmp_filename} to disk')
                    suffix += 1
                    #df2 = df2.append(i)
                except Exception as e:
                    logging.exception(f'Got exception {e} while trying to generate the parquet file.')
                    raise
                    #chunk_counter += 1
        logging.info('successfully written data to disk')
        return 0
    except Exception as e:
        logging.exception(f'Got exception "{e}" while executing function generate_parquet_file.\n\n{df}')
        if "expected after" in str(e):
            return 2
        else:
            return 1
# =============================================================================
#                 ___  ___       ___   _   __   _        _____   _   _   __   _   _____   _____   _   _____   __   _
#                /   |/   |     /   | | | |  \ | |      |  ___| | | | | |  \ | | /  ___| |_   _| | | /  _  \ |  \ | |
#               / /|   /| |    / /| | | | |   \| |      | |__   | | | | |   \| | | |       | |   | | | | | | |   \| |
#              / / |__/ | |   / / | | | | | |\   |      |  __|  | | | | | |\   | | |       | |   | | | | | | | |\   |
#             / /       | |  / /  | | | | | | \  |      | |     | |_| | | | \  | | |___    | |   | | | |_| | | | \  |
#            /_/        |_| /_/   |_| |_| |_|  \_|      |_|     \_____/ |_|  \_| \_____|   |_|   |_| \_____/ |_|  \_|
#
# =============================================================================


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
            current_working_folder = transformationfolders.pop()
            cwd_files = glob.glob(os.path.join(current_working_folder, '*'))
            if all(map(lambda x: ending(x) == 'csv', cwd_files)):
                compression = 'NONE'
            elif any(map(lambda x: ending(x) == '7z', cwd_files)):
                compression = 'SEVEN_ZIP'
            elif any(map(lambda x: ending(x) == 'gzip' or ending(x) == 'gz', cwd_files)):
                compression = 'GZIP'
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
                logging.error(f'wrong file format in folder: {current_working_folder}')
                continue
            if psutil_import is True:
                availablememory = str(int(((psutil.virtual_memory().free)/1024.0**2)*0.95))
                cmdlist = ('java -Xmx', availablememory,
                           'm -jar ', jar, ' convert "',
                           current_working_folder, '" "', dir_path, '" ', compression)
            else:
                cmdlist = ('java -jar ', jar, ' convert "',
                           current_working_folder, '" "', dir_path, '" ', compression)

            transformationcmd = ''.join(cmdlist)
            logging.info(f'starting transformation with the following command:\n{transformationcmd}')

            with subprocess.Popen(transformationcmd, stdout=subprocess.PIPE,
                                  stderr=subprocess.STDOUT) as proc:
                while proc.poll() is None:
                    try:
                        data = str(proc.stdout.readline(), 'utf-8')
                        logging.debug(data)
                        with open(sample_name, 'a') as tmp_output:
                            tmp_output.write(data)
                    except Exception as e:
                        logging.error(f'{e} occured while parsing sap-connector output')

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
                logging.error(f'transforamtion finished, with errors. Logs have been written\
                      to {logname}')
            else:
                logging.info('transforamtion finished.')
        os.remove(sample_name)

        cleanup_abap_par(transformationdir, dir_path)
    files = files_left(transformationdir_general)
    logging.info(f'non abap files about to be transformed: {files}')

    folders = create_folders(files, dir_path)
    logging.info('\ncreate_folder completed\n')

    for file in files:
        logging.info(f'start loop for: {file}')
        try:
            file_df = import_file(file, folders[file])
        except Exception as e:
            logging.exception(f'importing file failed with: {str(e)}')
        print('\n')

if cl.upload == 1:
    url = cl.url
    parts = []
    connectionflag = 1
    try:
        parts.append(re.search('https://([a-z0-9-]+)\.', url).groups()[0])
        parts.append(re.search('\.([a-z0-9-]+)\.celonis', url).groups()[0])
        parts.append(re.search('ui/pools/([a-z0-9-]+)', url).groups()[0])
        try:
            parts.append(re.search('data-connections/[a-z-]+/([a-z0-9-]+)', url)
                         .groups()[0])
        except AttributeError:
            connectionflag = 0
    except AttributeError:
        logging.error(f'{url} this is an unvalid url.')
    logging.info(connectionflag)

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
    single_par_files = [f for f in glob.glob(os.path.join(dir_path, '*.parquet'))]
    for i in single_par_files:
        splt = os.path.basename(i).split('.')
        destination = os.path.join(dir_path, splt[0])
        if not os.path.isdir(destination):
            os.mkdir(destination)
        try:
            shutil.move(i, destination)
        except Exception as e:
            logging.exception(f'encountered {e} when trying to sort parquet files')
    dirs = [join(dir_path, f) for f in listdir(dir_path) if os.path.isdir(join(dir_path, f))]
    logging.info(f'Dirs to be uploaded:\n{dirs}')
    uppie = cloud(tenant=tenant, realm=cluster, api_key=apikey)
    for dr in dirs:
        if dr == '__pycache__':
            continue
        logging.info(f'\nuploading: {Path(dr).name.replace(".", "/")}')
        jobhandle = uppie.create_job(pool_id=poolid,
                                     data_connection_id=connectionid,
                                     targetName=Path(dr).name.replace('.', '/'),
                                     upsert=delta)
        logging.info(f'jobhandle : {jobhandle}')
        jobstatus[jobhandle['id']] = False
        uppie.push_new_dir(pool_id=poolid, job_id=jobhandle['id'], dir_path=dr)
        uppie.submit_job(pool_id=poolid, job_id=jobhandle['id'])
    logging.debug(f'''upload done, waiting for jobs to be finished\nyou\'ll get a status\
update every 15 seconds. Logs will be written to: {logname}''')
    running = True
    logging.info('\n\nUpload log:\n')
    while running:
        jobs = uppie.list_jobs(poolid)
        for jobids in jobstatus:
            for i in jobs:
                try:
                    if i['id'] == jobids:
                        if i['status'] == 'QUEUED':
                            logging.debug(f'job for {i["targetName"]} queued')
                        elif jobstatus[jobids] is True:
                            pass
                        elif i['status'] == 'DONE':
                            jobstatus[jobids] = True
                            logging.info(f"{i['targetName']} was successfully installed in the database")
                        elif i['status'] != 'RUNNING':
                            jobstatus[jobids] = True
                            logging.error(f"{i['targetName']} failed with: {i['logs']}")
                        else:
                            print(f"job for {i['targetName']} still running")
                        break
                except (KeyboardInterrupt, SystemExit):
                    logging.error('terminating program\n')
                    quit()
                except:
                    pass
        if all(status is True for status in jobstatus.values()):
            running = False
            for i in jobs:
                if i['id'] in jobstatus:
                    if i['status'] == 'DONE':
                        logging.info(f"{i['targetName']} was successfully installed in the database")
                    else:
                        logging.error(f"{i['targetName']} failed with: {i['logs']}")
        else:
            time.sleep(15)
logging.info('all done')
