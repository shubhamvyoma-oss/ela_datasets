@echo off
setlocal
cd /d "%~dp0"
echo Generate one Edmingle API key and email it to the configured recipients.
echo Credentials entered at prompts are hidden and are not saved.
echo.
python edmingle_generate_api_key.py
echo.
echo Exit code: %ERRORLEVEL%
pause
endlocal
