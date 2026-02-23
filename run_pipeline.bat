@echo off
setlocal



set "PROJECT_DIR=C:\clients\tpr\planner_data_pipeline"
cd /d "%PROJECT_DIR%"

call "%PROJECT_DIR%\.venv\Scripts\activate.bat"

REM Make sure Python can import from src\
set "PYTHONPATH=%PROJECT_DIR%\src"

python -m pipeline.run_pipeline

exit /b %ERRORLEVEL%