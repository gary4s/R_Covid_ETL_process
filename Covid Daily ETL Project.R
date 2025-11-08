# Install necessary packages if you haven't already
# install.packages("DBI")
# install.packages("odbc")
# install.packages("readxl")
# install.packages("dplyr")
# install.packages("fs")
# install.packages("lubridate")
# install.packages("stringr")
# install.packages("tidyr") # For drop_na and other tidying functions

#change to test git
# Load all required libraries at the beginning
library(DBI)
library(odbc)
library(readxl)
library(dplyr)
library(fs)
library(lubridate) # For working with dates
library(stringr)   # For string manipulation
library(tidyr)     # For data tidying, e.g., drop_na
library(readr)



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

################################################################################################
############  Health Roster File wrangling ####################################################
###############################################################################################
# Health roster file is received as a xlsx and needs to be converted to csv
# Monday file usually contains three days using a report date to specify data for each day and this needs to be 
#extracted and split into three separate dated csv files

# --- Define Paths ---
file_path <- "C:/Users/gary4/Documents/Data Engineering/Portfolio/ETL Covid project/Health Roster data import"
export_path <- "C:/Users/gary4/Documents/Data Engineering/Portfolio/ETL Covid project/Covid data import"
move_path <- "C:/Users/gary4/Documents/Data Engineering/Portfolio/ETL Covid project/Received data"

# --- Function to check if a directory is empty ---
directory_is_empty <- function(directory_path) {
  # Use fs::dir_ls to list contents; length 0 means empty
  return(length(fs::dir_ls(directory_path)) == 0)
}

# --- Main File Processing Logic (Excel to CSV conversion) ---
tryCatch({
  message("Starting Excel to CSV file conversion process...")
  
  if (directory_is_empty(file_path)) {
    message("Health Roster import folder is empty.")
    
  } 
  else 
    {
    message(paste("Processing files in:", file_path))
    
    files_to_process <- fs::dir_ls(file_path, type = "file")
    
    if (length(files_to_process) == 0) 
      {
      message("No Excel files found in the Health Roster import directory.")
      
    } 
    else 
      {
      for (file in files_to_process) {
        message(paste("Reading file:", fs::path_file(file)))
        
        df <- tryCatch(
          {
          # Use read_excel with name_repair = "minimal" to preserve column names
          read_excel(file, .name_repair = "minimal")
        }, 
        error = function(e) 
          {
          message(paste("Error reading Excel file", fs::path_file(file), ":", e$message))
          return(NULL) # Return NULL if reading fails
        })
        
        if (!is.null(df)) 
          {
          # Check if 'Report Date' column exists
          if (!"Report Date" %in% names(df)) 
            {
            # If 'Report Date' column is missing, save as CSV with original filename
            original_filename_no_ext <- fs::path_ext_remove(fs::path_file(file))
            new_csv_filename <- paste0(original_filename_no_ext, ".csv")
            output_path_no_report_date <- fs::path(export_path, new_csv_filename)
            
            tryCatch(
              {
              readr::write_csv(df, output_path_no_report_date)
              message(paste("File '", fs::path_file(file), "' has no 'Report Date' column. Saved as '", new_csv_filename, "' in export folder.", sep = ""))
            }, 
            error = function(e) 
              {
              message(paste("Error saving file '", fs::path_file(file), "' without 'Report Date' column:", e$message))
            })
            
          }
          else 
            {
            # Convert 'Report Date' to a proper date format if it isn't already
            df <- df %>%
              mutate(`Report Date` = as_date(`Report Date`))
            
            tryCatch(
              {
              list_of_dfs <- df %>%
                group_by(`Report Date`) %>%
                group_split()
              
              for (new_df in list_of_dfs) 
                {
                file_date <- unique(new_df$`Report Date`)[1]
                
                if (is.na(file_date)) 
                  {
                  message(paste("Skipping export for a group with missing or invalid 'Report Date' in file:", fs::path_file(file)))
                  next # Skip to the next iteration of the loop
                }
                
                file_date_formatted <- format(file_date, "%Y-%m-%d")
                file_name <- paste0("Heath_Roster - ", file_date_formatted, ".csv")
                output_path <- fs::path(export_path, file_name)
                
                write.csv(new_df, output_path, row.names = FALSE)
                message(paste("Exported:", file_name))
              }
              message("Files Exported.")
              
            }, error = function(e) 
              {
              message(paste("Error during data processing and export for file", fs::path_file(file), ":", e$message))
            })
          }
          
          # Move the processed file to the received data folder
          destination_file <- fs::path(move_path, fs::path_file(file))
          tryCatch(
            {
            fs::file_move(file, destination_file)
            message(paste("Moved file '", fs::path_file(file), "' to '", move_path, "'", sep = ""))
          }, 
          error = function(e) 
            {
            message(paste("Error moving file", fs::path_file(file), ":", e$message))
          })
        }
      }
    }
  }
  
}, error = function(e) {
  message(paste("An error occurred during Excel to CSV conversion:", e$message))
})

#################################################################################################
#############  Get the Report date ##################
################################################################################################
#
## This section now acts as a function call within the main ETL loop.
## It extracts the report date from the first relevant file (Org_pattern)
## in the 'Covid data import' folder and stores it in the SQL table.
#
## --- Define Paths (re-defined for clarity, points to export_path) ---
data_path_for_report_date <- "C:/Users/gary4/Documents/Data Engineering/Portfolio/ETL Covid project/Covid data import"
#
## --- Define Patterns for File Naming ---
Org_pattern <- "BI_Organisation_Absence_Export_Daily_Covid19"
MedSus_pattern <- "BI_Medical_Suspensions_Export_Daily_Covid19"
HealRos_pattern <- "Heath_Roster"
#
## --- Function to get the report date from the first relevant file ---
#report_date <- function(directory_path, con) {
#  
#  filenames <- character(0) # Initialize an empty character vector to store extracted dates
#  
#  files_in_dir <- fs::dir_ls(directory_path, type = "file")
#  message(paste("Checking directory:", directory_path, "Found", length(files_in_dir), "files for report date extraction."))
#  
#  if (length(files_in_dir) == 0) {
#    message(paste("No files found in", directory_path, "to extract report date."))
#    return(NULL)
#  }
#  
#  for (file in files_in_dir) {
#    file_name <- fs::path_file(file)
#    if (stringr::str_detect(file_name, Org_pattern)) {
#      res <- stringr::str_extract(file_name, "\\d{4}-\\d{2}-\\d{2}")
#      if (!is.na(res)) {
#        filenames <- c(filenames, res)
#        break # Stop after finding the first relevant file and date
#      }
#    }
#  }
#  
#  first_date <- NULL
#  if (length(filenames) > 0) 
#    {
#    first_date <- ymd(filenames[1])
#    message(paste("First extracted date as R Date object:", format(first_date, "%Y-%m-%d")))
#  }
#  
#  if (!is.null(first_date)) 
#    {
#    if (!DBI::dbIsValid(con)) 
#      {
#      message("Error: Database connection is not valid. Cannot write report date to SQL.")
#      return(NULL)
#    }
#    
#    df_rpt <- data.frame(`Report Date` = first_date)
#    
#    tryCatch(
#      {
#        
#        #Truncate the date table
#        message("Truncating report date table")
#        DBI::dbExecute(con, "TRUNCATE TABLE [Covid19-Report-Date]")
#        
#        #write report date to table
#        DBI::dbWriteTable(con, "[Covid19-Report-Date]", df_rpt, append = TRUE, row.names = FALSE)
#        message(paste("Report date '", format(first_date, "%Y-%m-%d"), "' loaded to SQL table 'Covid19-Report-Date'.", sep = ""))
#    }, 
#    error = function(e)
#      {
#      message(paste("Error writing report date to SQL:", e$message))
#    }
#    )
#  }
#  else 
#    {
#    message("No valid report date found to load to SQL.")
#  }
#  return(first_date)
#}
#
## --- Function to get the report date from SQL ---
#get_report_date <- function(con) {
#  sql <- "SELECT TOP(100)* FROM [Covid19-Report-Date]"
#  df_sql_result <- NULL
#  if (!DBI::dbIsValid(con)) 
#    {
#    message("Error: Database connection is not valid. Cannot retrieve report date from SQL.")
#    return(NULL)
#  }
#  tryCatch(
#    {
#    df_sql_result <- DBI::dbGetQuery(con, sql)
#    message("Successfully retrieved report date from SQL.")
#  }, 
#  error = function(e) 
#    {
#    message(paste("Error retrieving report date from SQL:", e$message))
#  })
#  return(df_sql_result)
#}

################################################################################################
############  ETL Process (Extract, Transform, Load) ##########################################
###############################################################################################

###############################################################################################
# --- Function to find specific files for the current report date ---
###############################################################################################

# This function looks for the three types of files (Org, MedSus, HealthRoster)
# that contain the given date pattern in their filename.

find_specific_files <- function(folder_path, date_pattern_str) 
{
  # Construct expected filenames using predefined patterns and the report date
  org_file_pattern_full <- paste0(Org_pattern, " - ", date_pattern_str, ".csv")
  medsus_file_pattern_full <- paste0(MedSus_pattern, " - ", date_pattern_str, ".csv")
  healthroster_file_pattern_full <- paste0(HealRos_pattern, " - ", date_pattern_str, ".csv")
  
  # Initialise output variables
  org_file <- NULL
  medsus_file <- NULL
  healthroster_file <- NULL
  
  # List all files in the target directory
  files_in_dir <- fs::dir_ls(folder_path, type = "file")
  message(paste("Searching for files with date pattern:", date_pattern_str))
  
  # Loop through each file and match against expected patterns
  for (file_path_full in files_in_dir) {
    file_name <- fs::path_file(file_path_full)
    
    if (file_name == org_file_pattern_full) {
      org_file <- file_path_full
      message(paste("Found Org file:", file_name))
      
    } else if (file_name == medsus_file_pattern_full) {
      medsus_file <- file_path_full
      message(paste("Found MedSus file:", file_name))
      
    } else if (file_name == healthroster_file_pattern_full) {
      healthroster_file <- file_path_full
      message(paste("Found HealthRoster file:", file_name))
    }
  }
  
  # Return matched file paths as a named list
  return(list(org = org_file, medsus = medsus_file, healthroster = healthroster_file))
}


################################################################################################################
## Check that export has the required columns. A list that maps each file type to its required columns.
#################################################################################################################

#Add columns below to check
required_columns_map <- list(
  "Org" = c("Employee", "Last Name", "First Name", "Title", "Assignment Category", "Assignment Number",
            "Primary Assignment", "FTE", "Assignment Status", "Absence Reason",
            "Absence Type", "Staff Group", "Suit Int Rec", "Role", "Occupation Code", "Grade", "First Day Absent",
            "Absence Start Date", "Absence End Date", "Total Days Lost",
            "Predicted Fitness Date", "Work Related", "Third Party", "Return To Work Discussion Date", 
            "Occ Health Referral Date", "Afc Annual Leave", "OSP Nil Rate Start Date",
            "OSP Half Rate Start Date", "Total Days Lost Per Assignment", "Calendar Days Lost", 
            "Bradford Factor By Employee Only", "No of Episodes", "Leave Date", "Cost Centre",
            "Number of Episodes Squared", "FTE Days Lost", "Supervisor Name", "Absence Estimated Cost", 
            "Hours Lost", "Level 2 Reason", "Notifiable Disease","Related Reason"),
  
  "MedSus" = c("Employee", "Last Name", "First Name", "Title", "Assignment Category", "Assignment Number",
               "Primary Assignment", "FTE", "Assignment Status", "Absence Reason",
               "Absence Type", "Staff Group", "Suit Int Rec", "Role", "Occupation Code", "Grade", "First Day Absent",
               "Absence Start Date", "Absence End Date", "Total Days Lost",
               "Predicted Fitness Date", "Work Related", "Third Party", "Return To Work Discussion Date", 
               "Occ Health Referral Date", "Afc Annual Leave", "OSP Nil Rate Start Date",
               "OSP Half Rate Start Date", "Total Days Lost Per Assignment", "Calendar Days Lost", 
               "Bradford Factor By Employee Only", "No of Episodes", "Leave Date", "Cost Centre",
               "Number of Episodes Squared", "FTE Days Lost", "Supervisor Name", "Absence Estimated Cost", 
               "Hours Lost", "Level 2 Reason", "Notifiable Disease","Related Reason"),
  
  'Heath_Roster' = c("Unit Long Name", "Cost Centre", "Unit", "Submitted Upto Date", "Unlocked", "Surname", "Forenames",
                     "Name", "Staff Number", "Team", "Grade", "Grade Type", "Group", "Reason", "Start", "End", "Hours In Period (hh:mm)",
                     "State", "Hours In Period", "Total Duration", "Department", "Last Note", "Requested Date", "Lead Time",
                     "Is Open Ended")
)


# --- Function to Validate Columns in a Single File ---
# This function checks if a file has the required columns.
# It returns TRUE if the file is valid, and FALSE otherwise.

validate_file_columns <- function(file_path, file_type) {
  # Get the required columns for the given file type from our map.
  columns_to_check <- required_columns_map[[file_type]]
  
  # Use `tryCatch` to safely read the file. If an error occurs,
  # it returns NULL and the script moves to the next file.
  df <- tryCatch(
    {
      readr::read_csv(file_path)
    },
    error = function(e) {
      # Return NULL if the file cannot be read, preventing the script from crashing.
      message(paste("-> [ERROR] Could not read file:", fs::path_file(file_path)))
      return(NULL)
    }
  )
  
  # Check if the file was read successfully.
  if (is.null(df)) {
    return(FALSE)
  }
  
  # Check for missing columns and return the result.
  missing_columns <- setdiff(columns_to_check, names(df))
  if (length(missing_columns) == 0) {
    message(paste("-> [SUCCESS] All required columns found in", file_type, "file."))
    return(TRUE)
  } else {
    message(paste("-> [FAIL] ", file_type, " file is missing columns:", 
                  paste(missing_columns, collapse = ", ")))
    return(FALSE)
  }
}

################################################################################################################
# --- Extract functions (read CSV and move file) ---
##################################################################################################################

extract_data <- function(file_path, move_destination_path)
  {
  if (is.null(file_path) || !fs::file_exists(file_path)) 
    {
    message(paste("File not found or path is NULL:", file_path))
    return(NULL)
  }
  data <- NULL
  tryCatch(
    {
    data <- readr::read_csv(file_path, show_col_types = FALSE) # Read CSV
    message(paste("Successfully read:", fs::path_file(file_path)))
    
    # Move the file
    destination_file <- fs::path(move_destination_path, fs::path_file(file_path))
    fs::file_move(file_path, destination_file)
    message(paste("Moved file '", fs::path_file(file_path), "' to '", move_destination_path, "'", sep = ""))
  }, 
  error = function(e) 
    {
    message(paste("Error extracting/moving file", fs::path_file(file_path), ":", e$message))
  }
  )
  return(data)
}

#####################################################################################################################
# --- Transform functions ---
###################################################################################################################

transform_org <- function(data) 
  {
  if (is.null(data)) return(NULL)
  data <- data %>%
    #drop_na(everything()) %>% # Drop rows where all columns are NA
    mutate(
      `Absence Start Date` = lubridate::as_date(lubridate::dmy_hm(`Absence Start Date`)), # Coerce errors
      `Absence End Date` = dplyr::case_when( 
            stringr::str_detect(`Absence End Date`, "4712") ~ as.Date(NA),
            TRUE ~ lubridate::as_date(lubridate::dmy_hm(`Absence End Date`))
          )# Coerce errors
    ) %>%
    rename(`DH Monitoring` = `Related Reason`) # Rename column
  return(data)
  # R's dbWriteTable will infer types. No explicit dtype_mapping needed here like SQLAlchemy.
}



transform_med <- function(data) 
  {
  if (is.null(data)) return(NULL)
  data <- data %>%
    #drop_na(everything()) %>% # Drop rows where all columns are NA
    mutate(
      `Absence Start Date` = lubridate::as_date(lubridate::dmy_hm(`Absence Start Date`)), # Coerce errors
      `Absence End Date` = dplyr::case_when( 
        stringr::str_detect(`Absence End Date`, "4712") ~ as.Date(NA),
        TRUE ~ lubridate::as_date(lubridate::dmy_hm(`Absence End Date`))
      ) # Coerce errors
    ) %>%
    rename(`DH Monitoring` = `Related Reason`) # Rename column
  return(data)
}




transform_health <- function(data) {
  # Define the required columns in a vector.
  # This makes the code more readable and easier to debug.
  required_cols <- c(
    "Unit Long Name", "Cost Centre", "Unit", "Submitted Upto Date", "Unlocked", "Surname", "Forenames",
    "Name", "Staff Number", "Team", "Grade", "Grade Type", "Group", "Reason", "Start", "End",
    "Hours In Period (hh:mm)", "State", "Hours In Period", "Total Duration", "Department",
    "Last Note", "Requested Date", "Lead Time", "Is Open Ended"
  )

  # Check if any required columns are missing and print a message.
  missing_cols <- setdiff(required_cols, names(data))
  if (length(missing_cols) > 0) {
    message("Warning: The following columns are missing from the data and will be ignored:")
    message(paste(missing_cols, collapse = ", "))
  }

  
  data <- data %>%
    #drop_na(everything()) %>% # Drop rows where all columns are NA
    #select the columns we require. This will also make sure that the file has the columns.
    select(any_of(required_cols))  %>%
    mutate(
      'Start' = lubridate::as_date(lubridate::dmy(Start)), # Coerce errors
      'End' = dplyr::case_when(stringr::str_detect(End, "9999") ~ as.Date(NA),
                               TRUE ~ lubridate::as_date(lubridate::dmy(End))),
      # Clean and convert the 'Hours In Period (hh:mm)' column to ensure it's in a valid HH:MM:SS format
      # or replaced with NA for NULL values.
      `Hours In Period (hh:mm)` = dplyr::case_when(
               # Handle NA values first
               is.na(`Hours In Period (hh:mm)`) ~ as.character(NA),
               # If it's already in HH:MM:SS format, keep it as is
               stringr::str_detect(`Hours In Period (hh:mm)`, "^\\d{1,2}:\\d{2}:\\d{2}$") ~ as.character(`Hours In Period (hh:mm)`),
               # If it's in HH:MM format, append ":00"
               stringr::str_detect(`Hours In Period (hh:mm)`, "^\\d{1,2}:\\d{2}$") ~ paste0(`Hours In Period (hh:mm)`, ":00"),
               # For any other format, return NA
               TRUE ~ as.character(NA)
      ),
      'MB Flag' = NA_character_ # Add MB Flag column with NA values (character type)
    )
  return(data)
  
}



#######################################################################################################
# --- Load functions (write to SQL) ---#
#######################################################################################################

load_data <- function(data, con, table_name) 
  {
  if (is.null(data) || nrow(data) == 0) 
    {
    message(paste("No data to load for table:", table_name))
    return(FALSE)
  }
  if (!DBI::dbIsValid(con)) 
    {
    message(paste("Error: Database connection is not valid for loading to", table_name))
    return(FALSE)
  }
  
  tryCatch(
    {
      #Truncate the table
      message(paste("Truncating table:", table_name))
      DBI::dbExecute(con, paste0("TRUNCATE TABLE [", table_name, "]"))
      
      #Write data to table
      DBI::dbWriteTable(con, table_name, data, append = TRUE, row.names = FALSE)
      message(paste(table_name, "data uploaded successfully!"))
    return(TRUE)
  },
  
  error = function(e) 
    {
    message(paste(table_name, "data upload failed! Error:", e$message))
    return(FALSE)
  }
  )
}



#####################################################################################
# --------------------------- Main ETL Loop -----------------------------------------
#####################################################################################

# This loop will continue as long as there are files in the export_path (data_path_for_report_date)
# that match the expected patterns for the ETL process.

message("Starting main ETL loop...")

while (!directory_is_empty(data_path_for_report_date))
  {
  message("ETL Cycle")
  
  #################################################################
  # 1. Get the latest file date from the folder and write to SQL
  #################################################################
  # This logic is duplicated from the report_date function but is required here
  # to get the specific date for the current ETL run.
  
  current_report_date <- NULL
  filenames_for_date_extraction <- character(0)
  files_in_etl_dir <- fs::dir_ls(data_path_for_report_date, type = "file")
  

  
  for (file_in_etl_dir in files_in_etl_dir) {
    file_name_etl <- fs::path_file(file_in_etl_dir)
    if (stringr::str_detect(file_name_etl, Org_pattern)) 
      {
      res_etl <- stringr::str_extract(file_name_etl, "\\d{4}-\\d{2}-\\d{2}")
      if (!is.na(res_etl)) {
        filenames_for_date_extraction <- c(filenames_for_date_extraction, res_etl)
        break # Take the first one found
      }
    }
  }
  
  if (length(filenames_for_date_extraction) > 0) 
    {
    current_report_date <- ymd(filenames_for_date_extraction[1])
    message(paste("Current ETL Report Date identified:", format(current_report_date, "%Y-%m-%d")))
    

    # Update date in SQL table 'Covid19-Report-Date'
    if (!DBI::dbIsValid(con)) {
      message("Error: Database connection is not valid. Cannot update report date for ETL cycle. Breaking loop.")
      break # Exit loop if connection is bad
    }
    
    df_rpt_etl <- data.frame(current_report_date)
    names(df_rpt_etl) <- "Report Date"
    
    tryCatch(
      {
        
        #Truncate the date table
        message("Truncating report date table")
        DBI::dbExecute(con, "TRUNCATE TABLE [Covid19-Report-Date]")
        
        #Write date to SQL
        DBI::dbWriteTable(con, "Covid19-Report-Date", df_rpt_etl, append = TRUE, row.names = FALSE)
        message(paste("Report date '", format(current_report_date, "%Y-%m-%d"), "' loaded to SQL table 'Covid19-Report-Date' for current ETL cycle.", sep = ""))
    
        }, error = function(e) {
      message(paste("Error writing report date to SQL for ETL cycle:", e$message))
      message("Breaking ETL loop due to SQL write error.")
      break # Exit loop if SQL write fails
    }
    )
    
  }
  else
    {
    message("No valid report date found in files for this ETL cycle. Exiting loop.")
    break # Exit loop if no report date can be determined
  }
  df_rpt_etl  
  # Format the current report date for filename matching
  date_pattern_for_files <- format(current_report_date, "%Y-%m-%d")
  
  ####################################################################
  # 2. Find the specific files for this report date
  ####################################################################
  
  found_files <- find_specific_files(data_path_for_report_date, date_pattern_for_files)
  
  found_files
  
  #check if files exist in folder
  if (is.null(found_files$org) && is.null(found_files$medsus) && is.null(found_files$healthroster)) 
    {
    message(paste("No files found for date pattern", date_pattern_for_files, ". Ending ETL loop."))
    break # Exit loop if no files for this date
  }
  
  # validate columns for each file
  if (!is.null(found_files$org)){
      validate_file_columns(found_files$org, 'Org')
  }
  else {
    message("Org file column count mismatch.")
  }
  
  if (!is.null(found_files$org)){
    validate_file_columns(found_files$medsus, 'MedSus')
  }
  else {
    message("MedSus file column count mismatch.")
  }
  
  if (!is.null(found_files$org)){
    validate_file_columns(found_files$healthroster, 'Health_Roster')
  }
  else {
    message("Health Roster file column count mismatch.")
  }
  
  ###################################################################
  # 3. Extract, Transform, Load for each file type
  ###################################################################
  # Organisation Absence
  
  org_data <- extract_data(found_files$org, move_path)
  if (!is.null(org_data))
    {
    clean_org <- transform_org(org_data)
    load_data(clean_org, con, 'BI_Organisation_Absence_Import_Daily_Covid19')
  }
  else 
    {
    message("Skipping Organisation Absence ETL due to extraction failure.")
  }
  
  # Medical Suspensions
  med_data <- extract_data(found_files$medsus, move_path)
  if (!is.null(med_data)) 
    {
    clean_med <- transform_med(med_data)
    load_data(clean_med, con, 'BI_Medical_Suspensions_Import_Daily_Covid19')
  } 
  else 
    {
    message("Skipping Medical Suspensions ETL due to extraction failure.")
  }
  
  # Health Roster
  hel_data <- extract_data(found_files$healthroster, move_path)
  if (!is.null(hel_data))
    {
    clean_heal <- transform_health(hel_data)
    load_data(clean_heal, con, 'Healthroster_Absence_Import_Daily_Covid19')
  }
  else 
    {
    message("Skipping Health Roster ETL due to extraction failure.")
  }
  
  ##################################################################
  # 4. Execute the stored procedure
  #################################################################
  
  procedure_name <- "Covid19_UpdateAll"
  if (!is.null(con) && DBI::dbIsValid(con)) 
    {
    tryCatch(
      {
      DBI::dbExecute(con, paste0("EXEC ", procedure_name))
      message(paste("Stored procedure '", procedure_name, "' executed successfully!", sep = ""))
      
      # Check if stored procedure ran (audit log)
      sql_audit <- "SELECT TOP 1 execution_time FROM ProcedureAudit ORDER BY execution_time DESC"
      sp_runtime <- DBI::dbGetQuery(con, sql_audit)
      if (nrow(sp_runtime) > 0) {
        message(paste("Stored Procedure run at:", sp_runtime$execution_time[1]))
      } 
      else
        {
        message("Could not retrieve stored procedure audit time.")
      }
    },
    error = function(e) 
      {
      message(paste("Error executing stored procedure '", procedure_name, "':", e$message, sep = ""))
    }
    )
  } 
  else 
    {
    message("Cannot execute stored procedure: Database connection is not valid.")
  }
  
  # Small delay to prevent hammering the file system/database in a tight loop
  Sys.sleep(1) # Wait for 1 second
  
  # The loop condition will re-evaluate directory_is_empty(data_path_for_report_date)
  # to determine if there are more files to process.
}

message("ETL process completed or no more files to process.")

# IMPORTANT: Ensure the database connection 'con' is closed when all operations are complete
# in your overall script that orchestrates these parts.
if (!is.null(con) && DBI::dbIsValid(con))
  {
  DBI::dbDisconnect(con)
  message("Database connection closed.")
} else if(is.null(con)) 
  {
  message("No active connection to close.")
}


  
