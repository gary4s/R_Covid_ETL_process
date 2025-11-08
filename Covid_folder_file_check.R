# Load all required libraries at the beginning
library(DBI)
library(odbc)

#change to test git

##################################################################################
##### Connect to SQL Server and Database  ########################################
################################################################################

# --- Connection Details ---
# These parameters are directly equivalent to your Python connection string.
# Ensure 'ODBC Driver 17 for SQL Server' is installed on your system.
driver_name <- "ODBC Driver 17 for SQL Server"
server_name <- "LENOVO"
database_name <- "RF_Workforce"

# --- Attempt to connect to the database ---
con <- NULL # Initialize connection object to NULL

tryCatch({
  message("Attempting to connect to the database...")
  
  # Establish the connection using odbc::dbConnect
  # Trusted_Connection=yes is handled by the trusted_connection parameter
  con <- dbConnect(odbc(),
                   Driver = driver_name,
                   Server = server_name,
                   Database = database_name,
                   Trusted_Connection = "yes")
  
  message("Successfully connected to the database!")
  
}, error = function(e) {
  # This block runs if an error occurs during connection
  message(paste("Connection failed! Error:", e$message))
  # Set con to NULL explicitly if connection fails, to avoid issues in finally block
  con <- NULL
  
}, finally = {
  # The connection will be closed at the very end of the script's main execution block,
  # not here, as requested for further operations.
  if (!is.null(con) && dbIsValid(con)) {
    message("Connection remains open for further operations.")
  } else if (is.null(con)) {
    message("No active connection to close from this block.")
  }
})


###############################################################################################################
###### Folder check process#####################################################################################
###############################################################################################################

# Define folder paths
covid_folder <- "C:/Users/gary4/Documents/Data Engineering/Portfolio/ETL Covid project/Covid data import"
roster_folder <- "C:/Users/gary4/Documents/Data Engineering/Portfolio/ETL Covid project/Health Roster data import"
log_path <- "C:/Users/gary4/Documents/Data Engineering/Portfolio/ETL Covid project/File_Check_log.txt"

# Define file patterns
org_pattern <- "BI_Organisation_Absence_Export_Daily_Covid19[ _-]+.*\\.csv"
medsus_pattern <- "BI_Medical_Suspensions_Export_Daily_Covid19[ _-]+.*\\.csv"
roster_pattern <- "Heath_Roster[ _-]+.*\\.xlsx"

# List matching files
org_files <- list.files(covid_folder, pattern = org_pattern, full.names = TRUE)
medsus_files <- list.files(covid_folder, pattern = medsus_pattern, full.names = TRUE)
roster_files <- list.files(roster_folder, pattern = roster_pattern, full.names = TRUE)

# Get timestamp
timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")

# Check conditions
if (length(org_files) >= 1 && length(medsus_files) >= 1 && length(roster_files) == 1) {
  message(paste(timestamp, "- ✅ Triggering batch file..."))
  
  # Run the batch file
  shell("C:/Users/gary4/Documents/Data Engineering/Portfolio/ETL Covid project/Daily_Absence.bat")
  
  # Log success
  write(paste(timestamp, "- ✅ Batch file triggered."), file = log_path, append = TRUE)
  
  ##############################################
  # Read the tables
  ########################################################
  table_org <- 'BI_Organisation_Absence_Import_Daily_Covid19'
  table_med <- 'BI_Medical_Suspensions_Import_Daily_Covid19'
  table_health <- 'Healthroster_Absence_Import_Daily_Covid19'
  
  df_org <- dbReadTable(con, table_org)
  df_med <- dbReadTable(con, table_med)
  df_health <- dbReadTable(con, table_health)
  
  # Count rows and log
  row_count_org <- nrow(df_org)
  log_entry <- paste(timestamp, "- ✅ SQL table [", table_org, "] row count:", row_count_org)
  write(log_entry, file = log_path, append = TRUE)
  
  row_count_med <- nrow(df_med)
  log_entry <- paste(timestamp, "- ✅ SQL table [", table_med, "] row count:", row_count_med)
  write(log_entry, file = log_path, append = TRUE)
  
  row_count_health <- nrow(df_health)
  log_entry <- paste(timestamp, "- ✅ SQL table [", table_health, "] row count:", row_count_health)
  write(log_entry, file = log_path, append = TRUE)
  # Disconnect
  dbDisconnect(con)
  
} else {
  msg <- paste(timestamp, "- ❌ Files not ready. Org:", length(org_files),
               "MedSus:", length(medsus_files), "Roster:", length(roster_files))
  message(msg)
  
  # Log failure
  write(msg, file = log_path, append = TRUE)
}

