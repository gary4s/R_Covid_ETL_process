@ECHO OFF
SETLOCAL ENABLEDELAYEDEXPANSION

REM =================================================================
REM Batch script to: 1. Run R script, 2. Run SQL row count check
REM =================================================================

REM --- 1. Define Paths and Variables ---
SET SERVER=LENOVO
SET DATABASE=RF_Workforce

SET SCHEMA=dbo
SET ORG_TABLENAME=BI_Organisation_Absence_Import_Daily_Covid19
SET MED_TABLENAME=BI_Medical_Suspensions_Import_Daily_Covid19
SET HEL_TABLENAME=Healthroster_Absence_Import_Daily_Covid19

SET TEMP_ORG_ROW_COUNT_FILE=C:\Users\gary4\Documents\R\OrgAbs_row_count.tmp
SET R_SCRIPT="C:\Users\gary4\Documents\Data Engineering\Portfolio\ETL Covid project\Covid Daily ETL Project.R"
SET R_EXE="C:\Program Files\R\R-4.5.1\bin\Rscript.exe"
REM SET OUTPUT_FILE="C:\Users\gary4\Documents\R\sql_row_count_output.txt"

REM --- Recommended: Clear any existing log file ---
IF EXIST "%OUTPUT_FILE%.log" DEL "%OUTPUT_FILE%.log"

REM -----------------------------------------------------------------
@ECHO.
@ECHO =================================================================
@ECHO STEP 1: EXECUTING R SCRIPT TO PROCESS/INSERT DATA
@ECHO =================================================================
@ECHO Running: %R_SCRIPT%
@ECHO.

REM *** CRITICAL: Redirect R output to prevent console clutter and execution flow issues ***
CALL %R_EXE% %R_SCRIPT% > "%OUTPUT_FILE%.log" 2>&1

REM Check if the R script returned an error code (0 means success)
IF ERRORLEVEL 1 (
    @ECHO.
    @ECHO ERROR: The R script failed. Check %OUTPUT_FILE%.log
    GOTO :END_SCRIPT
)

@ECHO.
@ECHO R Script execution complete. R output logged to %OUTPUT_FILE%.log
@ECHO.

REM -----------------------------------------------------------------
@ECHO.
@ECHO =================================================================
@ECHO STEP 2: RUNNING SQL ROW COUNT CHECK
@ECHO =================================================================
@ECHO.

@ECHO Getting row count for Org Abs Table

REM ----- Get Org Abs table rowcount
sqlcmd -S %SERVER% -d %DATABASE% -E -Q "SET NOCOUNT ON; SELECT COUNT(*) FROM [%SCHEMA%].[%ORG_TABLENAME%];" -h -1 -W > Org_temp_rowcount.txt

REM ----- Get Med Sus Abs table rowcount
sqlcmd -S %SERVER% -d %DATABASE% -E -Q "SET NOCOUNT ON; SELECT COUNT(*) FROM [%SCHEMA%].[%MED_TABLENAME%];" -h -1 -W > Med_temp_rowcount.txt

REM ----- Get Health roster table rowcount
sqlcmd -S %SERVER% -d %DATABASE% -E -Q "SET NOCOUNT ON; SELECT COUNT(*) FROM [%SCHEMA%].[%HEL_TABLENAME%];" -h -1 -W > Hel_temp_rowcount.txt


REM === Read the result into a variable ===
SET /p ORG_ROWCOUNT=<Org_temp_rowcount.txt
SET /p MED_ROWCOUNT=<Med_temp_rowcount.txt
SET /p HEL_ROWCOUNT=<Hel_temp_rowcount.txt

@ECHO Org Abs Row count: %ORG_ROWCOUNT%
@ECHO.
@ECHO Med Sus Row count: %MED_ROWCOUNT%
@ECHO.
@ECHO Health Roster Row count: %HEL_ROWCOUNT%


REM === Cleanup ===
DEL Org_temp_rowcount.txt

REM -----------------------------------------------------------------
:END_SCRIPT

@ECHO.
@ECHO =================================================================
@ECHO BATCH PROCESS FINISHED.
@ECHO =================================================================
@ECHO.

REM *** THIS IS THE ONLY PAUSE COMMAND. IT MUST BE THE FINAL LINE. ***
PAUSE