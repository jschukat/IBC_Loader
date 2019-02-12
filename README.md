1) Connect to the Application Server

2) Copy all the files that need to be transformed to the App Server via WinSCP
   or however they are available to you and store them in one folder.

3) Create an empty folder to be used for the parquet files

if you have the batch file "IBC Loader.bat" on your desktop:
      execute it
      if asked for: use the git credentials saved on the desktop to update to the latest version
else:
      navigate to: 'Dropbox (Celonis)\Celonis_Connectors\Know-How & Tools\Cloud Table Loader'
      and copy it onto the app server, then execute: "upload_parquet.bat"

follow the instructions in the newly opened window
