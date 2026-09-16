@echo off
REM ============================================================
REM  run_pipeline.bat -- self-healing wrapper for
REM  master_attendance_pipeline.py
REM
REM  - Re-launches the pipeline if it crashes or is killed
REM    (checkpoint makes every restart resume where it stopped)
REM  - Stops looping on clean success (exit code 0)
REM  - 60s pause between restarts so a hard failure can't hot-loop
REM
REM  EDIT the two lines below, then run this instead of python:
REM      run_pipeline.bat
REM
REM  For automatic recovery after device shutdown / power loss:
REM  Task Scheduler > Create Task > Trigger "At startup" >
REM  Action: start this .bat > tick "Run whether user is logged
REM  on or not". After a reboot it resumes the run by itself.
REM ============================================================

set SCRIPT_DIR=D:\Shubham\Month_wise_work\July2026\New folder
set PIPELINE_ARGS=--from 2018-01-01 --to 2026-06-30

cd /d "%SCRIPT_DIR%"

:run
echo [%date% %time%] Starting pipeline...
python master_attendance_pipeline.py %PIPELINE_ARGS%

if %ERRORLEVEL% EQU 0 (
    echo [%date% %time%] Pipeline finished successfully. Done.
    goto end
)

echo [%date% %time%] Pipeline exited with code %ERRORLEVEL%. Restarting in 60s (checkpoint will resume)...
timeout /t 60 /nobreak >nul
goto run

:end
pause
