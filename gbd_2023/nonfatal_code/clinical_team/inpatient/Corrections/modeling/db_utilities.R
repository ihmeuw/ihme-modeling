#### Hospital DB functions ####
library(DBI)
source('FILEPATH/get_age_metadata.R')

if (Sys.info()[1] == "Linux") {
  j <- "FILEPATH"
  h <- "FILEPATH"
} else if (Sys.info()[1] == "Windows") {
  j <- "FILEPATH"
  h <- "FILEPATH"
}

db_connection <- function(){
  # Connect to hospital database
  odbc <- ini::read.ini("FILEPATH")
  conn_def <- "DATABASE"
  
  myconn <- RMySQL::dbConnect(RMySQL::MySQL(),
                              host = odbc[[conn_def]]$SERVER,
                              username = odbc[[conn_def]]$USER,
                              password = odbc[[conn_def]]$PASSWORD)
  return(myconn)
}

# Function to pull the ICG to bundle map.
# Argument of max_version is to subset for only the current greatest map_version or to get all bundles across all map_versions
bundle_icg <- function(latest = TRUE){
  myconn <- db_connection()
  
  df <- dbGetQuery(myconn, sprintf("QUERY"))
  
  max_version <- max(df$map_version)
  df <- as.data.table(df)
  
  print(latest)
  if(latest == TRUE){
    df <- df[map_version == max_version]
  }
  dbDisconnect(myconn)
  return(data.table(df))
}

age_binner <- function(dat){
  # marketscan data has single year ages so create gbd age bins
  # age bin
  agebreaks <- c(0,1,2,5,10,15,20,25,30,35,40,45,50,55,60,65,70,75,80,85,90,95,125)
  agelabels = c("0","1","2","5","10","15","20","25","30","35","40",
                "45","50","55","60","65","70","75","80","85","90", "95")
  setDT(dat)[, age_start := cut(age, breaks = agebreaks, right = FALSE, labels = agelabels)]
  # convert age groups to numeric
  dat$age_start <- as.numeric(as.character(dat$age_start))
  dat$age_end <- dat$age_start + 5
  dat[age_start == 0, 'age_end'] <- 1
  dat[age_start == 1, 'age_end'] <- 2
  dat[age_start == 2, 'age_end'] <- 5
  dat[age_end == 100, age_end := 125]
  return(dat)
}

get_icg_restrictions <- function(){
  myconn <- db_connection()
  
  df <- dbGetQuery(myconn, sprintf("QUERY"))
  dbDisconnect(myconn)
  return(data.table(df))
}

get_bundle_restrictions <- function(){
  myconn <- db_connection()
  
  # Get latest map_version
  bundle_map <- bundle_icg()
  map_version <- max(bundle_map$map_version)
  
  df <- dbGetQuery(myconn, sprintf(paste0("QUERY")))
  dbDisconnect(myconn)
  return(df)
}