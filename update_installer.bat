
@echo off
timeout /t 2 /nobreak > NUL
:RETRY
del "C:\Project\uma_tracking\dummy_app.exe.old" 2>NUL
move /y "C:\Project\uma_tracking\dummy_app.exe" "C:\Project\uma_tracking\dummy_app.exe.old"
if exist "C:\Project\uma_tracking\dummy_app.exe" goto RETRY
move /y "C:\Project\uma_tracking\new_update.exe.tmp" "C:\Project\uma_tracking\dummy_app.exe"
start "" "C:\Project\uma_tracking\dummy_app.exe"
del "%~f0"
    