REQUIREMENTS for converting ABAP output:

Java (JRE) 8 64-bit
On Windows:
Microsoft Visual C++ 2010 Redistributable Package (x64), available for download here: https://www.microsoft.com/en-us/download/details.aspx?id=14632
Java Hadoop Winutils (this is packaged with the software, but on some systems it needed to be provided separately - if this is the case Celonis is going to provide the necessary libraries)
Administrative rights for setup

you should consider updating to the following hardware setup if the conversion fails:
Virtual machine or physical server
CPU: Min. Intel Xeon processor with 4 Cores
RAM: min. 16 GB
64-bit Operating System


1) Create an empty folder to be used for the parquet files

TO TRANSFORM ABAP OUTPUT AN EXTRA FILE IS NEEDED:
  contact Celonis to obtain the required jar file and place it in the
  folder containing all the python files.

2) Run the upload_parquet.bat file and follow the instructions in the newly opened window


For feedback please contact j.schukat (at) celonis.com
