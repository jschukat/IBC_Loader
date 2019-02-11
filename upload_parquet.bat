@ECHO off

rd /s /q __pycache__
git.exe pull
python.exe main.py
pause
