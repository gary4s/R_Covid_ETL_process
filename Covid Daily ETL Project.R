# Install necessary packages if you haven't already
#.libPaths() 
# install.packages("odbc")
# install.packages("odbc")
# install.packages("odbc")
# install.packages("readxl")
# install.packages("dplyr")
# install.packages("fs")
#install.packages("lubridate")
# install.packages("stringr")
# install.packages("tidyr")


#install.packages("DBI")
#Load all required libraries at the beginning
#libraries with lib.loc= "C:/Program Files/R/R-4.5.2/library") are loaded first 
#These libraries need to be installed with admin rights due to the c++ compilation required

suppressPackageStartupMessages({
  library(rlang, lib.loc= "C:/Program Files/R/R-4.5.2/library")
  library(dplyr, lib.loc= "C:/Program Files/R/R-4.5.2/library")
  library(odbc, lib.loc= "C:/Program Files/R/R-4.5.2/library")
  library(tzdb, lib.loc= "C:/Program Files/R/R-4.5.2/library")
  library(readxl, lib.loc= "C:/Program Files/R/R-4.5.2/library")
  library(fs, lib.loc= "C:/Program Files/R/R-4.5.2/library")
  
  library(vroom, lib.loc= "C:/Program Files/R/R-4.5.2/library")
  library(readr, lib.loc= "C:/Program Files/R/R-4.5.2/library")
  #library(lubridate) # For working with dates
  library(stringr, lib.loc= "C:/Program Files/R/R-4.5.2/library")   # For string manipulation
  library(tidyr, lib.loc= "C:/Program Files/R/R-4.5.2/library")     # For data tidying, e.g., drop_na
  library(DBI, lib.loc= "C:/Program Files/R/R-4.5.2/library")
})



##################################################################################
##### Connect to SQL Server and Database  ########################################
################################################################################

# --- Connection Details ---

# Ensure 'ODBC Driver 17 for SQL Server' is installed on your system.
driver_name <- "ODBC Driver 13 for SQL Server"
server_name <- "rfh-information"
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
############  Global variables           ####################################################
###############################################################################################

# --- Define Paths ---
file_path <- "W:/WorkForce&OD/WORKFORCE/Workforce Information/Covid 19/Report Production/R Covid Report Production/Health Roster data import"
export_path <- "W:/WorkForce&OD/WORKFORCE/Workforce Information/Covid 19/Report Production/R Covid Report Production/Data for import"
move_path <- "W:/WorkForce&OD/WORKFORCE/Workforce Information/Covid 19/Report Production/SQL Import/Dated Exports"
HRoster_move_path <- "W:/WorkForce&OD/WORKFORCE/Workforce Information/Covid 19/Report Production/Data Received"

data_path_for_report_date <- "W:/WorkForce&OD/WORKFORCE/Workforce Information/Covid 19/Report Production/R Covid Report Production/Data for import"

## --- Define Patterns for File Naming ---
Org_pattern <- "BI_Organisation_Absence_Export_Daily_Covid19"
MedSus_pattern <- "BI_Medical_Suspensions_Export_Daily_Covid19"
HealRos_pattern <- "Health_Roster"

log_file_path <- "W:/WorkForce&OD/WORKFORCE/Workforce Information/Covid 19/Report Production/R Covid Report Production/ETL_Run_Log.txt"


####################################################################################################
### Log progress 
##################################################################################################

# --- Start Logging ---
# Open the file for logging, appending to it if it already exists
# 'split=TRUE' sends output to both the console AND the file (optional, but helpful)
# 'type="message"' ensures message() output is captured
log_connection <- file(log_file_path, open = "wt")
sink(log_connection, type = "output", append = TRUE, split = TRUE)
sink(log_connection, type = "message")

# --- Add a timestamp for this run ---
message(paste("/n--- ETL Run Started:", Sys.time(), "---"))
message(paste("Log file location:", log_file_path))

################################################################################################
############  Health Roster File wrangling ####################################################
###############################################################################################
# Health roster file is received as a xlsx and needs to be converted to csv
# Monday file usually contains three days using a report date to specify data for each day and this needs to be 
#extracted and split into three separate dated csv files


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
          
          #set the date col variable in the case it finds either column name
          date_col_name <- NULL
          if ("Report Date" %in% names(df)) {
            date_col_name <- "Report Date"
          } else if ("Date" %in% names(df)) {
            date_col_name <- "Date"
          }
          
          # Check if 'Report Date' column exists
          if (is.null(date_col_name)) 
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
            
            # Determine the name of the date column found
            standard_date_col_name <- "Report Date" # The name we want the column to have
            
            # Convert 'Report Date' to a proper date format if it isn't already
            df <- df %>%
              
              # 1. Rename the found date column using the variable
              #    to the standardized "Report Date".
              dplyr::rename(!!rlang::sym(standard_date_col_name) := !!rlang::sym(date_col_name)) %>%
              
              # 2. Convert the *now standardized* column to a proper R Date object
              dplyr::mutate(!!rlang::sym(standard_date_col_name) := as.Date(!!rlang::sym(standard_date_col_name)))
            
            tryCatch(
              {
                list_of_dfs <- df %>%
                  dplyr::group_by(!!rlang::sym(standard_date_col_name)) %>% 
                  dplyr::group_split()
                
                for (new_df in list_of_dfs) 
                {
                  file_date <- unique(new_df[[standard_date_col_name]])[1]
                  
                  if (is.na(file_date)) 
                  {
                    message(paste("Skipping export for a group with missing or invalid 'Report Date' in file:", fs::path_file(file)))
                    next # Skip to the next iteration of the loop
                  }
                  
                  file_date_formatted <- format(file_date, "%Y-%m-%d")
                  file_name <- paste0("Health_Roster - ", file_date_formatted, ".csv")
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
          destination_file <- fs::path(HRoster_move_path, fs::path_file(file))
          tryCatch(
            {
              fs::file_move(file, destination_file)
              message(paste("Moved file '", fs::path_file(file), "' to '", HRoster_move_path, "'", sep = ""))
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

#####Function to get the report date from the first relevant file ---

###report_date <- function(directory_path, con) {
###  
###  filenames <- character(0) # Initialize an empty character vector to store extracted dates
###  
###  # Keeping fs::dir_ls
###  files_in_dir <- fs::dir_ls(directory_path, type = "file")
###  message(paste("Checking directory:", directory_path, "Found", length(files_in_dir), "files for report date extraction."))
###  
###  if (length(files_in_dir) == 0) {
###    message(paste("No files found in", directory_path, "to extract report date."))
###    return(NULL)
###  }
###  
###  for (file in files_in_dir) {
###    # Keeping fs::path_file and stringr::str_detect/str_extract
###    file_name <- fs::path_file(file)
###    if (stringr::str_detect(file_name, Org_pattern)) {
###      res <- stringr::str_extract(file_name, "\\d{4}-\\d{2}-\\d{2}")
###      if (!is.na(res)) {
###        filenames <- c(filenames, res)
###        break # Stop after finding the first relevant file and date
###      }
###    }
###  }
###  
###  first_date <- NULL
###  if (length(filenames) > 0) {
###    # *** Base R equivalent for lubridate::ymd() ***
###    # as.Date handles "YYYY-MM-DD" format by default
###    first_date <- as.Date(filenames[1], format = "%Y-%m-%d") 
###    
###    message(paste("First extracted date as R Date object:", format(first_date, "%Y-%m-%d")))
###  }
###  
###  if (!is.null(first_date)) {
###    # Keeping DBI functions
###    if (!DBI::dbIsValid(con)) {
###      message("Error: Database connection is not valid. Cannot write report date to SQL.")
###      return(NULL)
###    }
###    
###    # Base R data.frame creation
###    df_rpt <- data.frame(`Report Date` = first_date, check.names = FALSE)
###    
###    tryCatch(
###      {
###        #Truncate the date table
###        message("Truncating report date table")
###        DBI::dbExecute(con, "TRUNCATE TABLE [Covid19-Report-Date]")
###        
###        #write report date to table
###        DBI::dbWriteTable(con, "[Covid19-Report-Date]", df_rpt, append = TRUE, row.names = FALSE)
###        message(paste("Report date '", format(first_date, "%Y-%m-%d"), "' loaded to SQL table 'Covid19-Report-Date'.", sep = ""))
###      }, 
###      error = function(e) {
###        message(paste("Error writing report date to SQL:", e$message))
###      }
###    )
###  } else {
###    message("No valid report date found to load to SQL.")
###  }
###  return(first_date)
###}
###
#### . Function Execution ---
#### NOTE: Ensure the 'con' object is defined and points to your live SQL connection.
####report_date(data_path_for_report_date, con)
###
####
##### --- Function to get the report date from SQL ---
####get_report_date <- function(con) {
####  sql <- "SELECT TOP(100)* FROM [Covid19-Report-Date]"
####  df_sql_result <- NULL
####  if (!DBI::dbIsValid(con)) 
####    {
####    message("Error: Database connection is not valid. Cannot retrieve report date from SQL.")
####    return(NULL)
####  }
####  tryCatch(
####    {
####    df_sql_result <- DBI::dbGetQuery(con, sql)
####    message("Successfully retrieved report date from SQL.")
####  }, 
####  error = function(e) 
####    {
####    message(paste("Error retrieving report date from SQL:", e$message))
####  })
####  return(df_sql_result)
####}
###
###current_report_date

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
      message(" ")
      
    } else if (file_name == medsus_file_pattern_full) {
      medsus_file <- file_path_full
      message(paste("Found MedSus file:", file_name))
      message(" ")
      
    } else if (file_name == healthroster_file_pattern_full) {
      healthroster_file <- file_path_full
      message(paste("Found HealthRoster file:", file_name))
      message(" ")
    }
  }
  
  # Return matched file paths as a named list
  return(list(org = org_file, medsus = medsus_file, healthroster = healthroster_file))
}


################################################################################################################
## Check that export has the required columns. A list that maps each file type to its required columns.
#################################################################################################################

#Add columns below to check - this could do with future proofing by adding these to individual files that are referenced here.
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
  
  'Health_Roster' = c("Unit Long Name", "Cost Centre", "Unit", "Submitted Upto Date", "Unlocked", "Surname", "Forenames",
                      "Name", "Staff Number", "Team", "Grade", "Grade Type", "Group", "Reason", "Start", "End", "Hours In Period (hh:mm)",
                      "State", "Hours In Period", "Total Duration", "Department", "Last Note")
)

#, "Requested Date", "Lead Time","Is Open Ended"

# --- Function to Validate Columns in a Single File ---
# This function checks if a file has the required columns.
# It returns TRUE if the file is valid, and FALSE otherwise.

validate_file_columns <- function(file_path, file_type) {
  
  # Define the specification to read ALL columns as character
  # This prevents readr from guessing types and throwing warnings/errors on mixed data.
  all_char_spec <- readr::cols(
    .default = readr::col_character() 
  )
  
  # Get the required columns for the given file type from our map.
  columns_to_check <- required_columns_map[[file_type]]
  
  # Use `tryCatch` to safely read the file. If an error occurs,
  # it returns NULL and the script moves to the next file.
  df <- tryCatch(
    {
      ## update to read all columns as characters
      readr::read_csv(file_path, col_types = all_char_spec, show_col_types = FALSE)
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
    message(" ")
    return(TRUE)
  } else {
    message(paste("-> [FAIL] ", file_type, " file is missing columns:", 
                  paste(missing_columns, collapse = ", ")))
    return(FALSE)
  }
}

#found_files
#validate_file_columns(found_files$healthroster, 'Health_Roster')
#df <- readr::read_csv(found_files$healthroster, show_col_types = FALSE)
#problems(df)

#this is working

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
  
  all_char_spec <- readr::cols(.default = readr::col_character())
  
  data <- NULL
  
  tryCatch(
    {
      data <- readr::read_csv(file_path, col_types = all_char_spec,  show_col_types = FALSE) # Read CSV
      message(paste("Successfully read:", fs::path_file(file_path)))
      message(" ")
      
      # Move the file
      destination_file <- fs::path(move_destination_path, fs::path_file(file_path))
      fs::file_move(file_path, destination_file)
      message(paste("Moved file '", fs::path_file(file_path), "' to '", move_destination_path, "'", sep = ""))
      message(" ")
    }, 
    error = function(e) 
    {
      message(paste("Error extracting/moving file", fs::path_file(file_path), ":", e$message))
    }
  )
  return(data)
}

##extract_data(found_files$org, move_path)


#####################################################################################################################
# --- Transform functions ---
###################################################################################################################


transform_org <- function(data) {
  
  if (is.null(data)) return(NULL)
  
  # Define the expected format for 'Day/Month/Year Hour:Minute'
  # Example: "23/11/2025 08:08"
  date_time_format <- "%d/%m/%Y %H:%M" 
  
  data <- data %>%
    # drop_na(everything()) %>% # Keep if desired, as it's a tidyr function
    
    # Mutate columns (dplyr)
    dplyr::mutate(
      
      # 1. Convert 'Absence Start Date'
      # Base R equivalent for lubridate::as_date(lubridate::dmy_hm())
      `Absence Start Date` = suppressWarnings(
        as.Date(strptime(`Absence Start Date`, format = date_time_format))
      ),
      
      # 2. Convert 'Absence End Date'
      `Absence End Date` = dplyr::case_when(
        
        # Condition: Keeping stringr::str_detect
        stringr::str_detect(`Absence End Date`, "4712") ~ as.Date(NA),
        
        # Result: Base R equivalent for lubridate::as_date(lubridate::dmy_hm())
        TRUE ~ suppressWarnings(
          as.Date(strptime(`Absence End Date`, format = date_time_format))
        )
      )
    ) %>%
    
    # Rename column (dplyr)
    dplyr::rename(`DH Monitoring` = `Related Reason`) 
  
  return(data)
}



transform_med <- function(data) {
  
  if (is.null(data)) return(NULL)
  
  # Define the expected format for 'Day/Month/Year Hour:Minute'
  # Example: "23/11/2025 08:08"
  date_time_format <- "%d/%m/%Y %H:%M" 
  
  data <- data %>%
    # drop_na(everything()) %>% # Keep if desired, as it's a tidyr function
    
    # Mutate columns (dplyr)
    dplyr::mutate(
      
      # 1. Convert 'Absence Start Date'
      # Base R equivalent for lubridate::as_date(lubridate::dmy_hm())
      `Absence Start Date` = suppressWarnings(
        as.Date(strptime(`Absence Start Date`, format = date_time_format))
      ),
      
      # 2. Convert 'Absence End Date'
      `Absence End Date` = dplyr::case_when(
        
        # Condition: Keeping stringr::str_detect
        stringr::str_detect(`Absence End Date`, "4712") ~ as.Date(NA),
        
        # Result: Base R equivalent for lubridate::as_date(lubridate::dmy_hm())
        TRUE ~ suppressWarnings(
          as.Date(strptime(`Absence End Date`, format = date_time_format))
        )
      )
    ) %>%
    
    # Rename column (dplyr)
    dplyr::rename(`DH Monitoring` = `Related Reason`) 
  
  return(data)
}

#clean_med <- transform_med(med_data)


transform_health <- function(data) {
  
  if (is.null(data)) return(NULL)
  
  # Define the expected format for 'Day/Month/Year'
  # Example: "23/11/2025"
  date_format <- "%d/%m/%Y" 
  
  # Define the required columns in a vector.
  ### required_cols <- c(
  ###   "Unit Long Name", "Cost Centre", "Unit", "Submitted Upto Date", "Unlocked", "Surname", "Forenames",
  ###   "Name", "Staff Number", "Team", "Grade", "Grade Type", "Group", "Reason", "Start", "End",
  ###   "Hours In Period (hh:mm)", "State", "Hours In Period", "Total Duration", "Department",
  ###   "Last Note", "Requested Date", "Lead Time", "Is Open Ended"
  ### )
  
  required_cols <- required_columns_map$Health_Roster
  
  # Check if any required columns are missing and print a message. (Base R)
  missing_cols <- setdiff(required_cols, names(data))
  if (length(missing_cols) > 0) {
    message("Warning: The following columns are missing from the data and will be ignored:")
    message(paste(missing_cols, collapse = ", "))
  }
  
  data <- data %>%
    
    
    # convert all blank strings ("") to NA
    # This ensures tidyr::drop_na can catch them.
    # We apply this across all columns (`everything()`).
    
    dplyr::mutate(
      dplyr::across(
        .cols = dplyr::everything(),
        .fns = ~dplyr::na_if(.x, "")
      )
    ) %>%
    
    # Filter out rows where the sum of NAs equals the total number of columns.
    dplyr::filter(
      rowSums(is.na(dplyr::across(dplyr::everything()))) != ncol(data)
    ) %>%
    
    # Select columns (dplyr)
    dplyr::select(dplyr::any_of(required_cols)) %>% 
    
    # Mutate columns (dplyr)
    dplyr::mutate(
      
      #  Convert 'Start' Date: Replacing lubridate::as_date(lubridate::dmy())
      'Start' = suppressWarnings(
        as.POSIXct(as.Date(Start, format = date_format))
      ),
      
      #  Convert 'End' Date: Replacing lubridate::as_date(lubridate::dmy()) in the TRUE condition
      'End' = dplyr::case_when(
        stringr::str_detect(End, "9999") ~ as.POSIXct(NA), # Must return POSIXct NA
        TRUE ~ suppressWarnings(as.POSIXct(as.Date(End, format = date_format)))
      ),
      
      # --- FLOAT CONVERSIONS (SQL FLOAT) ---
      # These MUST be numeric in R before writing to a SQL FLOAT column.
      # Cost Centre might fail if it still contains codes like 'DS002', 
      # but if it's supposed to be numeric, this is required.
      `Cost Centre` = suppressWarnings(as.numeric(`Cost Centre`)),
      `Hours In Period` = suppressWarnings(as.numeric(`Hours In Period`)),
      `Total Duration` = suppressWarnings(as.numeric(`Total Duration`)),
      
      
      # --- HH:MM:SS FORMATTING FOR SQL TIME (This is still Nvarchar in SQL, but let's re-confirm the formatting) ---
      # This block is required to clean the format, but since the SQL column is VARCHAR,
      # we ensure the output is character.
      `Hours In Period (hh:mm)` = dplyr::case_when(
        is.na(`Hours In Period (hh:mm)`) ~ as.character(NA),
        stringr::str_detect(`Hours In Period (hh:mm)`, "^\\d{1,2}:\\d{2}:\\d{2}$") ~ as.character(`Hours In Period (hh:mm)`),
        stringr::str_detect(`Hours In Period (hh:mm)`, "^\\d{1,2}:\\d{2}$") ~ paste0(`Hours In Period (hh:mm)`, ":00"),
        TRUE ~ as.character(NA)
      ),
      
      'MB Flag' = NA_character_ # Add MB Flag column with NA values (character type)
    )
  
  return(data)
}



#clean_heal <- transform_health_test(hel_data)


######################################################################################################
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

# Define the function to check if the directory contains files
directory_is_empty <- function(path) {
  # list.files (Base R) or fs::dir_ls (preferred here) returns a vector of file/dir paths.
  # The length of the result tells us if anything is in the folder.
  # We use type = "file" to ignore sub-directories.
  files_in_dir <- fs::dir_ls(path, type = "file")
  return(length(files_in_dir) == 0)
}

# Initial check before entering the loop
if (directory_is_empty(data_path_for_report_date)) {
  message(paste("No files found in", data_path_for_report_date, ". ETL process complete."))
  # You might want to skip the 'while' loop entirely if no files are found.
  # If the rest of your script follows the loop, you can just stop here or return.
}

message("Starting main ETL loop...")

tryCatch({
  
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
    
    # Base R equivalent for fs::dir_ls(..., type = "file")
    # Assumes data_path_for_report_date is defined.
    files_in_etl_dir <- list.files(
      path = data_path_for_report_date,
      all.files = FALSE,
      full.names = TRUE,
      recursive = FALSE
    )
    
    # Loop through each file path
    for (file_in_etl_dir in files_in_etl_dir) {
      
      # Base R equivalent for fs::path_file(file_in_etl_dir)
      file_name_etl <- basename(file_in_etl_dir)
      
      # Base R equivalent for stringr::str_detect(file_name_etl, Org_pattern)
      # Assumes Org_pattern is defined.
      if (grepl(Org_pattern, file_name_etl)) {
        
        # Base R equivalent for stringr::str_extract(file_name_etl, "\\d{4}-\\d{2}-\\d{2}")
        date_pattern <- "\\d{4}-\\d{2}-\\d{2}"
        match_info <- regexpr(date_pattern, file_name_etl)
        res_etl <- regmatches(file_name_etl, match_info)
        
        # Check if a date was found (regmatches returns an empty character vector if no match)
        if (length(res_etl) > 0) {
          filenames_for_date_extraction <- c(filenames_for_date_extraction, res_etl[1])
          break # Take the first one found
        }
      }
    }
    
    if (length(filenames_for_date_extraction) > 0) {
      
      # *** Base R equivalent for lubridate::ymd() ***
      current_report_date <- as.Date(filenames_for_date_extraction[1], format = "%Y-%m-%d")
      message(paste("Current ETL Report Date identified:", format(current_report_date, "%Y-%m-%d")))
      
      # Update date in SQL table 'Covid19-Report-Date'
      
      # NOTE: The 'break' statements below are typically used inside a loop construct 
      # (like a 'while' loop) which is not explicitly shown encapsulating this entire block. 
      # If this code is *inside* a larger loop, these 'break' statements are correct. 
      # If not, 'stop()' or 'return()' is usually needed to halt execution in a script.
      
      if (!DBI::dbIsValid(con)) {
        message("Error: Database connection is not valid. Cannot update report date for ETL cycle. Breaking loop.")
        # If this code is inside a loop, use 'break'. Otherwise, 'stop()' is often cleaner.
        # break 
      }
      
      df_rpt_etl <- data.frame(current_report_date)
      names(df_rpt_etl) <- "Report Date"
      
      tryCatch(
        {
          #Truncate the date table
          message("Truncating report date table")
          message(" ")
          DBI::dbExecute(con, "TRUNCATE TABLE [Covid19-Report-Date]")
          
          #Write date to SQL
          DBI::dbWriteTable(con, "Covid19-Report-Date", df_rpt_etl, append = TRUE, row.names = FALSE)
          message(paste("Report date '", format(current_report_date, "%Y-%m-%d"), "' loaded to SQL table 'Covid19-Report-Date' for current ETL cycle.", sep = ""))
          message(" ")
          
        }, error = function(e) {
          message(paste("Error writing report date to SQL for ETL cycle:", e$message))
          message("Breaking ETL loop due to SQL write error.")
          # If this code is inside a loop, use 'break'. Otherwise, 'stop()' is often cleaner.
          # break 
        }
      )
      
    } else {
      message("No valid report date found in files for this ETL cycle. Exiting loop.")
      # If this code is inside a loop, use 'break'. Otherwise, 'stop()' is often cleaner.
      # break 
    }
    
    if (!is.null(current_report_date)) {
      # Format the current report date for filename matching
      date_pattern_for_files <- format(current_report_date, "%Y-%m-%d")
    } else {
      date_pattern_for_files <- NULL
    }
    
    ##-- this portion is working ##
    ##current_report_date
    
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
    
    # 1. Validate Org file
    org_valid <- FALSE
    if (!is.null(found_files$org)) {
      # Capture the TRUE/FALSE result from the validation function
      org_valid <- validate_file_columns(found_files$org, 'Org')
    } 
    if (!org_valid) {
      message("Org file validation failed or file not found.")
      # Consider what action to take if a required file fails validation
    }
    
    # 2. Validate MedSus file
    medsus_valid <- FALSE
    if (!is.null(found_files$medsus)) {
      # Capture the TRUE/FALSE result
      medsus_valid <- validate_file_columns(found_files$medsus, 'MedSus')
    } 
    if (!medsus_valid) {
      message("MedSus file validation failed or file not found.")
      message(" ")
    }
    
    # 3. Validate Health Roster file
    healthroster_valid <- FALSE
    if (!is.null(found_files$healthroster)) {
      # Capture the TRUE/FALSE result
      healthroster_valid <- validate_file_columns(found_files$healthroster, 'Health_Roster')
    } 
    if (!healthroster_valid) {
      message("Health Roster file validation failed or file not found.")
      message(" ")
    }    
    
    ##-- this portion is working ##     
    
    # ###################################################################
    # # 3. Extract, Transform, Load for each file type
    # ###################################################################
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
      message(" ")
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
      message(" ")   
    }
    
    # Health Roster
    hel_data <- extract_data(found_files$healthroster, HRoster_move_path)
    if (!is.null(hel_data))
    {
      clean_heal <- transform_health(hel_data)
      load_data(clean_heal, con, 'Healthroster_Absence_Import_Daily_Covid19')
    }
    else 
    {
      message("Skipping Health Roster ETL due to extraction failure.")
      message(" ")
    }
    
    ###################################################################
    # 3.5. CLEANUP EXPORTED CSV FILE (Delete CSVs older than current batch)
    ###################################################################
    
    # this step will delete the health roster csv file created that is before the current batch date, as
    # the health roster file often contains the date before the date on which we want to begin the ETL process
    # and would have been imported on the previous run
    
    if (!is.null(current_report_date)) {
      
      # 1. Define the cutoff date (the day BEFORE the current batch starts)
      # If current_report_date is 2025-11-22, this is 2025-11-21.
      date_cutoff <- current_report_date - 1 
      
      # Construct the file name based on the cutoff date
      file_date_formatted <- format(date_cutoff, "%Y-%m-%d")
      
      # The file we suspect is the leftover from the previous run
      file_name_to_delete <- paste0("Health_Roster - ", file_date_formatted, ".csv")
      file_path_to_delete <- fs::path(export_path, file_name_to_delete)
      
      # 2. Check and Delete the file
      if (fs::file_exists(file_path_to_delete)) {
        tryCatch({
          fs::file_delete(file_path_to_delete)
          message(paste("Cleaned up overlapping CSV file:", file_name_to_delete, " (Date older than current batch)."))
          message(" ")
        },
        error = function(e) {
          message(paste("Error deleting file", file_name_to_delete, ":", e$message))
          message(" ")
        })
      } else {
        message(paste("No overlapping CSV file found to delete for date:", file_date_formatted))
        message(" ")
      }
    }
    
    ##################################################################
    # 4. Execute the Covid_UpdateAll stored procedure
    #################################################################
    
    procedure_name <- "Covid19_UpdateAll"
    if (!is.null(con) && DBI::dbIsValid(con)) 
    {
      tryCatch(
        {
          DBI::dbExecute(con, paste0("EXEC ", procedure_name))
          message(paste("Stored procedure '", procedure_name, "' executed successfully!", sep = ""))
          message(" ")
          
          # Check if stored procedure ran (audit log)
          sql_audit <- "SELECT TOP 1 LogDateTime FROM [ProcedureLogs] ORDER BY LogDateTime DESC"
          sp_runtime <- DBI::dbGetQuery(con, sql_audit)
          if (nrow(sp_runtime) > 0) {
            message(paste("Stored Procedure run at:", sp_runtime$LogDateTime[1]))
            message(" ")
          } 
          else
          {
            message("Could not retrieve stored procedure audit time.")
          }
          sql_view <- "SELECT * FROM [Covid_SP_Import_Counts]"
          sp_rowcount_view <- DBI::dbGetQuery(con, sql_view)
          if (nrow(sp_rowcount_view) >0){
            message("Row counts:")
            print(sp_rowcount_view)
            message(" ")
          }
          else
          {
            message("Could not get view data.")
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
      message("Cannot execute Covid19_UpdateAll stored procedure: Database connection is not valid.")
    }
    
    
    ##################################################################
    # 5. Execute the Covid19_Sickness_Report_Archive_Append stored procedure
    #################################################################
    
    procedure_name <- "Covid19_Sickness_Report_Archive_Append"
    if (!is.null(con) && DBI::dbIsValid(con)) 
    {
      tryCatch(
        {
          DBI::dbExecute(con, paste0("EXEC ", procedure_name))
          message(paste("Stored procedure '", procedure_name, "' executed successfully!", sep = ""))
          message(" ")
          
          # Check if stored procedure ran (audit log)
          sql_audit <- "SELECT TOP 1 LogDateTime FROM [ProcedureLogs] ORDER BY LogDateTime DESC"
          sp_runtime <- DBI::dbGetQuery(con, sql_audit)
          if (nrow(sp_runtime) > 0) {
            message(paste("Stored Procedure run at:", sp_runtime$LogDateTime[1]))
            message(" ")
          } 
          else
          {
            message("Could not retrieve stored procedure audit time.")
          }
          #get row count
          sql_rowCnt <- "SELECT TOP 1 Rows_actioned FROM [ProcedureLogs] ORDER BY LogDateTime DESC"
          sp_rowruntime <- DBI::dbGetQuery(con, sql_rowCnt)
          if (nrow(sp_rowruntime) > 0) {
            message(paste("Sickness Report rows appended:", sp_rowruntime$Rows_actioned[1]))
            message(" ")
          } else {
            message("Could not get row count.")
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
      message("Cannot execute Covid19_Sickness_Report_Archive_Append stored procedure: Database connection is not valid.")
    }
    
    ######################################################################################
    # 6. Execute the WF_Covid19_Sickness_Report_Archive_Append stored procedure to 
    #    import to SSRS_Reports_Landing database for information team to use.
    #####################################################################################
    
    procedure_name <- "WF_Covid19_Sickness_Archive_Append"
    if (!is.null(con) && DBI::dbIsValid(con)) 
    {
      tryCatch(
        {
          DBI::dbExecute(con, paste0("EXEC ", procedure_name))
          message(paste("Stored procedure '", procedure_name, "' executed successfully!", sep = ""))
          message(" ")
          
          # Check if stored procedure ran (audit log)
          sql_audit <- "SELECT TOP 1 LogDateTime FROM [ProcedureLogs] ORDER BY LogDateTime DESC"
          sp_runtime <- DBI::dbGetQuery(con, sql_audit)
          if (nrow(sp_runtime) > 0) {
            message(paste("Stored Procedure run at:", sp_runtime$LogDateTime[1]))
            message(" ")
          } 
          else
          {
            message("Could not retrieve stored procedure audit time.")
          }
          #get row count
          sql_rowCnt <- "SELECT TOP 1 Rows_actioned FROM [ProcedureLogs] ORDER BY LogDateTime DESC"
          sp_rowruntime <- DBI::dbGetQuery(con, sql_rowCnt)
          if (nrow(sp_rowruntime) > 0) {
            message(paste("Sickness Report rows appended:", sp_rowruntime$Rows_actioned[1]))
            message(" ")
          } else {
            message("Could not get row count.")
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
      message("Cannot execute WF_Covid19_Sickness_Report_Archive_Append stored procedure: Database connection is not valid.")
    }
    
    
  } 
  message(paste("--- ETL Run Completed Successfully:", Sys.time(), "---"))
  
}, error = function(e) {
  # If a critical error occurs, capture it in the log
  message(paste("--- CRITICAL ETL FAILURE:", Sys.time(), "---"))
  message(paste("Error Details:", e$message))
})



message("ETL process completed or no more files to process.")

# ---  Stop Logging ---
# Must call sink() twice to close both 'output' and 'message' sinks.
# Use flush=TRUE to ensure all buffered output is written immediately.
sink(type = "message")
sink(type = "output")
close(log_connection)

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



