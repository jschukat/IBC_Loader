@ECHO off

rd /s /q __pycache__
git.exe pull origin master
python.exe main.py
pause
