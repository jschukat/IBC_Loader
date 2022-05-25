This Python script is for uploading flat files into the EMS. It can be used for ABAP Exports, csvs, Excel files and parquet. Files larger than 1GB shouldn't pose a problem.

# Prerequisites:

  1. Install the latest Anaconda version (https://www.anaconda.com/distribution/#windows) and make sure that you add Anaconda to your Path<br/>![add Anaconda to Path](https://github.com/jschukat/IBC_Loader/blob/master/pictures/1.png?raw=true)
  3. Install git (https://git-scm.com/download/win)
  4. Install IBC Loader
  - Choose an empty directory into which the IBC Loader shall be installed
  - Once you opened it in the Windows Explorer (Win + E), right click and choose Git Bash Here<br/>![open git bash](https://github.com/jschukat/IBC_Loader/blob/master/pictures/2.png?raw=true)
  - In the newly opened console run: git clone https://github.com/jschukat/IBC_Loader.git.
  - This should download the latest version of the IBC Loader and install it into the directory

If you want to transfrom ABAP exported files make sure to have:
- Java 8 installed
- Microsoft Visual C++ 2010 Redistributable Package (x64), available for download here: https://www.microsoft.com/en-us/download/details.aspx?id=14632
- Java Hadoop Winutils (this is packaged with the software, but on some systems it needed to be provided separately - if this is the case Celonis is going to provide the necessary libraries)
- and put the connector-sap module into the IBC Loader folder


# 1. Run Cloud Table Loader
Run the batch file contained in the folder (or, if you aren't allowed to execute batch files, run "python.exe main.py" from your command line) which will open up the following window:

![Run Cloud Table Loader](https://github.com/jschukat/IBC_Loader/blob/master/pictures/3.png?raw=true)

# 2. Enter URL
The URL to be entered is the url you can see in the destination Data Pool. Go to the Event Collection:

![Event Collection](https://github.com/jschukat/IBC_Loader/blob/master/pictures/4.png?raw=true)

Choose your Data Pool:

![Choose your Data Pool](https://github.com/jschukat/IBC_Loader/blob/master/pictures/5.png?raw=true)

In the Data Pool, if you want to load your data into the global schema, use the Overview URL:

![Overview](https://github.com/jschukat/IBC_Loader/blob/master/pictures/6.png?raw=true)

Or if it is supposed to go into a connection schema, go to Data Connections and click on the desired target connection, the use the URL depicted:

![Data Connections](https://github.com/jschukat/IBC_Loader/blob/master/pictures/7.png?raw=true)

And paste either URL in the top most text field:

![URL](https://github.com/jschukat/IBC_Loader/blob/master/pictures/8.png?raw=true)


# 3. Create API Key


Then you need to create an API key. To do so, click on your profile picture in the top right corner:

![open menu](https://github.com/jschukat/IBC_Loader/blob/master/pictures/9.png?raw=true)

And choose "Edit Profile":

![editr profile](https://github.com/jschukat/IBC_Loader/blob/master/pictures/10.png?raw=true)

In there scroll down until you see the option to create an API key:

![name api key](https://github.com/jschukat/IBC_Loader/blob/master/pictures/11.png?raw=true)

To do so, you need to enter an arbitrary name for the key first and then press the button "Create API Key".

![create api key](https://github.com/jschukat/IBC_Loader/blob/master/pictures/12.png?raw=true)

Once done a window will pop up and show you the created API key.
Copy this key and paste it into the second text field:

![enter API Key](https://github.com/jschukat/IBC_Loader/blob/master/pictures/13.png?raw=true)



# 4. Select Directories


Once this is done you just need to select the directory where the csv / excel files are stored by clicking the button:

![select the raw directory](https://github.com/jschukat/IBC_Loader/blob/master/pictures/folder_selection.png?raw=true)

Now do the same for the parquet dir (this should be an empty dir). After this your window should look something like this:

![select the parquet directory](https://github.com/jschukat/IBC_Loader/blob/master/pictures/15.png?raw=true)

# 5. Transform / Upload

Then you can choose if you just want to transform the files to parquet and upload them, just transform the files to parquet, or, if you already have parquet files, just upload parquet files.

# 6. Done!
