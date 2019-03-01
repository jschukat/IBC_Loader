from tkinter import filedialog
from tkinter import *
import os
import subprocess
# TODO: make module import failsafe
import cloud_upload_config as ctc


# ********** Install dependencies ********** #
print('installing dependencies')
flag = os.system('pip --version')

if flag == 1:
    print('install pip')
    cmd = ''.join(['python.exe "', os.path.join(os.getcwd(), 'get-pip.py'),
                   '"'])
    os.system(cmd)

pkgs = os.popen('pip freeze').read()

for pkg in ['psutil', 'requests']:
    if pkg not in pkgs:
        print('\ninstalling', pkg)
        cmd = ''.join(['pip install ', pkg])
        os.system(cmd)

fast = os.popen('conda list').read()
if not 'python-snappy' in fast or not 'fastparquet' in fast:
    conda_check = subprocess.run(['where.exe', 'conda'])
    if conda_check.returncode == 0:
        print('installing fastparquet and snappy')
        subprocess.run(['conda', 'install', 'fastparquet', '-y'])
        subprocess.run(['conda', 'install', 'python-snappy', '-y'])

global defaultTenant
defaultTenant = 'demo, if url is https://demo.eu-1.celonis.cloud/'
global defaultCluster
defaultCluster = 'us-1, if url is https://demo.us-1.celonis.cloud/'
global defaultApikey
defaultApikey = 'create api key in the cloud team under "My Account" at the very bottom'
global defaultPoolid
defaultPoolid = 'Event Collection -> select data pool -> in url copy string between pools/ and /overview'
global defaultConnectionid
defaultConnectionid = 'Event Collection -> open config of data connection -> in url copy string between /data-connections/.../ and /edit'
global inputdir
inputdir = 'This should be the directory, where the unzipped ABAP and or other csv / excel files are stored.'
global outputdir
outputdir = 'This should be an empty directory, where the parquet files can be placed in.'
global defaultUrl
defaultUrl = 'Open connection settings or data pool (whichever applies) and paste the whole url in here'

root = Tk()


def rmText(event):
    if eval(''.join(['entry', str(event.widget).split('.')[2]])).get() in [defaultCluster, defaultTenant, defaultPoolid, defaultApikey, defaultConnectionid, inputdir, outputdir, defaultUrl]:
        eval(''.join(['text', str(event.widget).split('.')[2]])).set('')


def checkContent(event):
    if eval(''.join(['entry', str(event.widget).split('.')[2]])).get() == '':
        if str(event.widget).split('.')[2] == 'tenant':
            texttenant.set(defaultTenant)
        elif str(event.widget).split('.')[2] == 'cluster':
            textcluster.set(defaultCluster)
        elif str(event.widget).split('.')[2] == 'apikey':
            textapikey.set(defaultApikey)
        elif str(event.widget).split('.')[2] == 'connectionid':
            textconnectionid.set(defaultConnectionid)
        elif str(event.widget).split('.')[2] == 'input':
            textinput.set(inputdir)
        elif str(event.widget).split('.')[2] == 'output':
            textoutput.set(outputdir)
        elif str(event.widget).split('.')[2] == 'url':
            texturl.set(defaultUrl)
        else:
            textpoolid.set(defaultPoolid)


def selectIn():
    inputdirname = filedialog.askdirectory(initialdir="/", title="Select dir")
    textinput.set(inputdirname)
    print(inputdirname)


def selectOut():
    outputdir = filedialog.askdirectory(initialdir="/", title="Select dir")
    textoutput.set(outputdir)
    print(outputdir)

def setButtonTitle():
    tr = transformationflag.get()
    up = uploadflag.get()
    if tr == 1 and up == 1:
        runbutton["text"] = 'Run Transformation and Upload'
    elif tr == 1 and up == 0:
        runbutton["text"] = 'Run Transformation'
    elif tr == 0 and up == 1:
        runbutton["text"] = 'Upload'
    else:
        runbutton["text"] = 'Nothing to be done here'

def runlolarun():
    ur = texturl.get()
    # te = texttenant.get()
    # cl = textcluster.get()
    ap = textapikey.get()
    # po = textpoolid.get()
    # co = textconnectionid.get()
    tr = transformationflag.get()
    up = uploadflag.get()
    # if co == defaultConnectionid:
    #     co = ''
    out = textoutput.get()
    inp = textinput.get()
    with open('cloud_upload_config.py', 'w+') as conffile:
        conffile.write("""# Example: url of cloud team: https://demo.eu-1.celonis.cloud/
# tenant = 'demo'
# cluster = 'eu-1'
# apikey: create key in the cloud team under "My Account"
# pool: Pool Analytics -> select target pool -> in url copy string after ui/

url = '{}'
apikey = '{}'
outputdir = '{}'
inputdir = '{}'
transformation = {}
upload = {}\n""".format(ur, ap, out, inp, tr, up))
    print('saved to config.')
    cmd = ''.join(['python.exe "', os.path.join(os.getcwd(),
                   'cloud_upload.py'), '"'])
    os.system(cmd)
    root.destroy()


root.title('Cloud Table Loader')
root.geometry('900x300+200+100')

# TODO: convert the following parts to objects

# *********** URL *********** #
frameurl = Frame(root)
frameurl.pack(fill=X)

labelurl = Label(frameurl, text='URL')
labelurl.pack(side=LEFT, padx=10, pady=5)
texturl = StringVar()
entryurl = Entry(frameurl, textvariable=texturl, name='url')
if ctc.url == '':
    texturl.set(defaultUrl)
else:
    texturl.set(ctc.url)

entryurl.bind('<FocusIn>', rmText)
entryurl.bind('<FocusOut>', checkContent)
entryurl.pack(fill=X, padx=10, pady=5)


'''
# *********** Tenant *********** #
frametenant = Frame(root)
frametenant.pack(fill=X)

labeltenant = Label(frametenant, text='Tenant')
labeltenant.pack(side=LEFT, padx=10, pady=5)
texttenant = StringVar()
entrytenant = Entry(frametenant, textvariable=texttenant, name='tenant')
if ctc.tenant == '':
    texttenant.set(defaultTenant)
else:
    texttenant.set(ctc.tenant)

entrytenant.bind('<FocusIn>', rmText)
entrytenant.bind('<FocusOut>', checkContent)
entrytenant.pack(fill=X, padx=10, pady=5)


# *********** Cluster *********** #
framecluster = Frame(root)
framecluster.pack(fill=X)

labelcluster = Label(framecluster, text='Cluster')
labelcluster.pack(side=LEFT, padx=10, pady=5)
textcluster = StringVar()
entrycluster = Entry(framecluster, textvariable=textcluster, name='cluster')
if ctc.cluster == '':
    textcluster.set(defaultCluster)
else:
    textcluster.set(ctc.cluster)
entrycluster.bind('<FocusIn>', rmText)
entrycluster.bind('<FocusOut>', checkContent)
entrycluster.pack(fill=X, padx=10, pady=5)
'''

# *********** Api-Key *********** #
frameapikey = Frame(root)
frameapikey.pack(fill=X)

labelapikey = Label(frameapikey, text='ApiKey')
labelapikey.pack(side=LEFT, padx=10, pady=5)
textapikey = StringVar()
entryapikey = Entry(frameapikey, textvariable=textapikey, name='apikey')
if ctc.apikey == '':
    textapikey.set(defaultApikey)
else:
    textapikey.set(ctc.apikey)
entryapikey.bind('<FocusIn>', rmText)
entryapikey.bind('<FocusOut>', checkContent)
entryapikey.pack(fill=X, padx=10, pady=5)

'''
# *********** Pool ID *********** #
framepoolid = Frame(root)
framepoolid.pack(fill=X)

labelpoolid = Label(framepoolid, text='Pool ID')
labelpoolid.pack(side=LEFT, padx=10, pady=5)
textpoolid = StringVar()
entrypoolid = Entry(framepoolid, textvariable=textpoolid, name='poolid')
if ctc.poolid == '':
    textpoolid.set(defaultPoolid)
else:
    textpoolid.set(ctc.poolid)
entrypoolid.bind('<FocusIn>', rmText)
entrypoolid.bind('<FocusOut>', checkContent)
entrypoolid.pack(fill=X, padx=10, pady=5)


# *********** Connection ID *********** #
frameconnectionid = Frame(root)
frameconnectionid.pack(fill=X)

labelconnectionid = Label(frameconnectionid, text='Connection ID')
labelconnectionid.pack(side=LEFT, padx=10, pady=5)
textconnectionid = StringVar()
entryconnectionid = Entry(frameconnectionid, textvariable=textconnectionid,
                          name='connectionid')
if ctc.connectionid == '':
    textconnectionid.set(defaultConnectionid)
else:
    textconnectionid.set(ctc.connectionid)
entryconnectionid.bind('<FocusIn>', rmText)
entryconnectionid.bind('<FocusOut>', checkContent)
entryconnectionid.pack(fill=X, padx=10, pady=5)
'''

# *********** Input Dir *********** #
frameinput = Frame(root)
frameinput.pack(fill=X)

labelinput = Button(frameinput, text='Select ABAP / csv / excel dir', command=selectIn)
labelinput.pack(side=LEFT, padx=10, pady=5)
textinput = StringVar()
entryinput = Entry(frameinput, textvariable=textinput, name='input')
if ctc.inputdir == '':
    textinput.set(inputdir)
else:
    textinput.set(ctc.inputdir)
entryinput.bind('<FocusIn>', rmText)
entryinput.bind('<FocusOut>', checkContent)
entryinput.pack(fill=X, padx=10, pady=5)


# *********** Output Dir *********** #
frameoutput = Frame(root)
frameoutput.pack(fill=X)

labeloutput = Button(frameoutput, text='Select parquet dir', command=selectOut)
labeloutput.pack(side=LEFT, padx=10, pady=5)
textoutput = StringVar()
entryoutput = Entry(frameoutput, textvariable=textoutput, name='output')
if ctc.outputdir == '':
    textoutput.set(outputdir)
else:
    textoutput.set(ctc.outputdir)
entryoutput.bind('<FocusIn>', rmText)
entryoutput.bind('<FocusOut>', checkContent)
entryoutput.pack(fill=X, padx=10, pady=5)


# *********** Buttons *********** #
framebuttons = Frame(root)

transformationflag = IntVar()
transformationbutton = Checkbutton(framebuttons, text='Run Transformation',
                                   variable=transformationflag, command=setButtonTitle)
if ctc.transformation == 1:
    transformationflag.set(1)
else:
    transformationflag.set(0)

transformationbutton.grid(row=0, column=1, sticky='WE', padx=10, pady=5)

uploadflag = IntVar()
uplaodbutton = Checkbutton(framebuttons, text='Upload parquet files',
                           variable=uploadflag, command=setButtonTitle)
uplaodbutton.grid(row=0, column=2, sticky='WE', padx=10, pady=5)
if ctc.upload == 1:
    uploadflag.set(1)
else:
    uploadflag.set(0)

runbutton = Button(framebuttons, text='Run Transformation and Upload',
                   command=runlolarun)
runbutton.grid(row=1, column=1, sticky='WE', padx=10, pady=5)
setButtonTitle()
exitbutton = Button(framebuttons, text='Exit', command=root.destroy)
exitbutton.grid(row=1, column=2, sticky='WE', padx=10, pady=5)

framebuttons.pack()

root.mainloop()
