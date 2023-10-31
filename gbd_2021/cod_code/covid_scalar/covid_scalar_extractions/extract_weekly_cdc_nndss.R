## Load packages\
rm(list=ls())
library(tidyverse) # includes stringr
library(rvest)
library(data.table)
library(openxlsx)

###################################################################################
# Supporting Functions and objects (need all for script to work)
###################################################################################
# Get list of letters for each table
table_letters <- data.table(letters)
table_letters$double_letters <- paste0(letters, letters)
double_letters <- data.table(table_letters$double_letters)
names(double_letters) <- "letters"
table_letters <- table_letters[,-"double_letters"]
table_letters <- rbindlist(list(table_letters, double_letters))

base_url <- "https://wonder.cdc.gov/nndss/static/"

# Format table pulled from HTML:
# GET HEADING LEVELS
# figure out how many levels there are in the header by
# incrementing until "current week" is found in index 1 of colnames(temp_dt). While incrementing,
# save each colname set. At the end of incrementing, paste0 all cause name levels with -- in between
# Example: arboviral disease--encephalitis--current week. This will be split to create heading level columns.
# MAKE NEW HEADER
# run a loop that unlists the first row for the number of cause levels there are.
# set the concatenated case_level names as the new headers
# MELT
# melt by all column names except for "Regional area"
format_dt <- function(dt, year_id) {
  col1 <- ""
  level <- 0
  level <- as.integer(level)
  concat_headings <- c()

  # Get all heading levels and concatenate
  while (!col1 %in% c("Current week", "Currentweek")) {
    if(level>0) {
      dt <- setNames(dt, unlist(dt[1,]))
      dt <- dt[-1,]
    }
    heading_lvl <- names(dt)
    col1 <- heading_lvl[2]
    ifelse(level>0, concat_headings <- paste0(concat_headings, "--", heading_lvl), concat_headings <- heading_lvl)
    level <- level+1
  }

  if (year_id %in% c(2017, 2018)) {
    dt <- dt[-1,] # get rid of first row which contains duplicate headers that aren't captured by the while loop
  }
  names(dt) <- concat_headings
  names(dt)[1] <- "location_name"
  dt <- data.table(dt)

  # Change any logical columns to character
  index <- grep(TRUE, sapply(dt, is.logical))
  if (length(index) > 0) {
    temp <- dt[,index,with=FALSE]
    names(temp) <- paste0(names(temp), 1:length(names(temp))) #add a dummy number because this doesn't work if all cols are the same name
    temp[] <- lapply(temp, as.character)
    names(temp) <- gsub("([0-9])", "", names(temp))

    dt <- dt[,!index, with=FALSE]
    dt <- cbind(dt, temp)
  }

  # Reshape data table
  dt <- melt(dt, id.vars="location_name", variable.name="variable")

  # Replacing character values. From the CDC website:
  # N: Not reportable — The disease or condition was not reportable by law, statute, or regulation in the reporting jurisdiction.
  # NN: Not nationally notifiable — This condition was not designated as being nationally notifiable.
  # NP: Nationally notifiable but not published.
  # NC: Not calculated — There is insufficient data available to support the calculation of this statistic.
  # U: Unavailable — The reporting jurisdiction was unable to send the data to CDC or CDC was unable to process the data.
  dt[value=="-", value:=0]
  dt$value <- gsub(",","",dt$value) #remove commas from any numbers
  dt[value=="N", value:=NA]
  dt[value=="NN", value:=NA]
  dt[value=="NP", value:=NA]
  dt[value=="NC", value:=NA]
  dt[value=="U", value:=NA]
  dt$value <- as.numeric(dt$value)

  # add NY and NYC if both locations are present per disease
  dt[location_name %like% "New York City", location_name:="NY"]
  ny_sums <- dt[location_name %like% "NY"]
  ny_sums <- ny_sums[,lapply(.SD, sum), by="variable", .SDcols="value"]
  ny_sums$location_name <- "New York"
  dt <- dt[!location_name %like% "NY"]
  dt <- rbindlist(list(dt,ny_sums), use.names=TRUE, fill=TRUE)

  # Create heading level columns
  split_var <- str_split_fixed(dt$variable, "--", level)
  split_var <- data.table(split_var)
  dt <- cbind(dt, split_var)
  dt <- data.table(dt)

  # If this is a measles table, sum all "current week" columns if multiple current week cols exist.
  # This will sum imported and indigenous cases.
  if (length(agrep("Measles", dt$V1)) > 0) {
    measles_index <- agrep("Measles", dt$V1)
    dt[measles_index, V1:="measles"]
    ifelse("V3" %in% names(dt), dt <- dt[V1=="measles" & V3 %like% "Current week" | V1=="measles" & V3 %like% "Currentweek",],
           dt <- dt[V1=="measles" & V2 %like% "Current week" | V1=="measles" & V2 %like% "Currentweek",])
    temp <- dt[,.N, by="location_name"] #if only one set of locations, measles was not split into indigenous and imported
    if (length(unique(temp$N)) > 1) {
      dt <- aggregate(dt[,"value"], by=dt[,"location_name"], sum)
    }
    dt$V1 <- "measles"
    dt$variable <- "measles"
  }
  return(dt)
}


# Pull table from HTML
get_cdc_table <- function(index, url) {
  dt <- url %>% read_html() %>%
    html_nodes("table") %>% ## find all tables (can also put specific xpath here)
    .[[index]] %>% ## take the right table (might need to inspect a bit to find right #)
    html_table(fill = T) ## read
  dt <- data.table(dt)
  return(dt)
}


# MAIN EXTRACTION FUNCTION
# Also includes calls to functions that extract table 1's from 2017 and 2018.
extract_cdc_nndss <- function(year_id, max_week, stop_letter, table_num) {
  dt <- data.table()
  for (week in 1:max_week) {
    ifelse(stop_letter == "", stop <- 1, stop <- which(table_letters$letters == stop_letter))

    for (i in 1:stop) { # find exact letter match
      url_letter <- ifelse(year_id %in% c(2017,2018) & table_num==1, "", table_letters$letters[[i]])
      ifelse(week<=9,
             url <- paste0(base_url, year_id, "/0", week, "/", year_id, "-0", week, "-table", table_num, url_letter, ".html"),
             url <- paste0(base_url, year_id, "/", week, "/", year_id, "-", week, "-table", table_num, url_letter, ".html"))

      # If any warnings appear, collect the table letters to investigate
      # and print the warnings.
      tryCatch(
        expr = {
          temp_dt <- get_cdc_table(1, url=url)
          if (nrow(temp_dt) == 1) {
            temp_dt <- get_cdc_table(2, url=url)

            if (nrow(temp_dt) == 1) {
              temp_dt <- get_cdc_table(3, url=url)
            }
          }
        },
        warning = function(w){
          message(paste0('Warning level 1: Caught a warning at table ', letters[i], ' week ', week))
          print(w)
        }
      )

      # Get all-age h.influenzae manually because the heading level columns do not distinguish it
      # from <5 due to how the CDC formatted the table.
      hib_index <- grep("Haemophilus influenzae, invasive disease", names(temp_dt))
      if(length(hib_index) > 0 & !(temp_dt[1][[2]] %like% "Age <5 years")) {
        temp_dt <- cbind(temp_dt[,1], temp_dt[,hib_index[1], with=FALSE])
      }

      tryCatch(
        expr = {
          if (year_id %in% c(2017,2018) & table_num==1) {
            temp_dt <- extract_tbl1(temp_dt)
          } else {
            temp_dt <- format_dt(temp_dt, year_id)
          }
        },
        warning = function(w){
          message(paste0('Warning level 2: Caught a warning at table ', letters[i], ' week ', week))
          print(w)
        }
      )
      temp_dt$week <- week
      dt <- rbindlist(list(dt, temp_dt), fill=TRUE)
    }
  }
  if (!(year_id %in% c(2017,2018) & table_num==1)) {
    setnames(dt, c("variable","value"), c("disease","cases"))
  }
  dt$year_id <- year_id
  return(dt)
}


# Extract all table 1's for 2017 and 2018 #########################################
# Function to fully format table 1's extracted from 2017 and 2018.
extract_tbl1 <- function(dt) {
  dt <- setNames(dt, unlist(dt[1,]))
  dt <- dt[-1,]
  names(dt)[length(names(dt))] <- "state_cases"
  names(dt) <- tolower(names(dt))
  imd_index <- agrep("Meningococal disease", dt$disease)
  dt[imd_index, currentweek:=0]
  dt[imd_index:(imd_index+4), disease:="meningococcal disease"]
  dt[agrep("measles", dt$disease), disease:="measles"]
  dt[state_cases == "", state_cases:=0]

  dt[currentweek=="-", currentweek:=0]
  dt$currentweek <- gsub(",","",dt$currentweek) #remove commas from any numbers
  dt[currentweek=="N", currentweek:=NA]
  dt[currentweek=="NN", currentweek:=NA]
  dt[currentweek=="NP", currentweek:=NA]
  dt[currentweek=="NC", currentweek:=NA]
  dt[currentweek=="U", currentweek:=NA]
  dt$currentweek <- as.numeric(dt$currentweek)

  dt_formatted <- apply(dt, 1, format_tbl1) # apply function to each row
  dt_formatted <- rbindlist(dt_formatted)

  # Aggregate and format IMD. Then combine with overall dt
  meningo_sums <- dt_formatted[disease %like% "meningococcal disease",sum(cases), by=c("disease","location_name")]
  meningo_sums <- meningo_sums[!is.na(location_name)]
  setnames(meningo_sums, "V1", "cases")
  dt_formatted <- dt_formatted[!disease %like% "meningococcal disease"]
  dt_formatted <- rbindlist(list(dt_formatted, meningo_sums), use.names=TRUE)

  # add NY and NYC if both locations are present per disease
  dt_formatted[location_name %like% "NYC", location_name:="NY"]
  ny_sums <- dt_formatted[location_name %like% "NY",sum(cases), by="disease"]
  ny_sums$location_name <- "NY"
  setnames(ny_sums, "V1", "cases")
  dt_formatted <- dt_formatted[!location_name %like% "NY"]
  dt_formatted <- rbindlist(list(dt_formatted,ny_sums), use.names=TRUE)

  # Match abbreviated state names to full names to add location ids later.
  # This uses the state database embedded in R.
  dt_formatted$location_name <- state.name[match(dt_formatted$location_name, state.abb)]
  dt_formatted$disease <- tolower(dt_formatted$disease)
  dt_formatted <- dt_formatted[disease %like% 'diphtheria|measles|meningococcal disease|cholera']

  # Extend to full 50 states using preloaded state database in R (these table 1's exclude US territories)
  final_dt <- data.table(location_name=rep(state.name, times=4), disease=sort(rep(c("diphtheria","measles","meningococcal disease","cholera"), times=50)))
  index <- match(paste0(final_dt$location_name, final_dt$disease), paste0(dt_formatted$location_name, dt_formatted$disease))
  final_dt$cases <- dt_formatted[index]$cases
  final_dt[is.na(cases), cases:=0]

  return(final_dt)
}

# Function to split data by state from 2017 and 2018 table 1's.
format_tbl1 <- function(table) {
  cause <- table["disease"][[1]]
  cases <- strsplit(table["state_cases"][[1]], ", ")
  cases <- unlist(cases)
  cases_num <- as.numeric(str_extract(cases, "[0-9]+")) # get case numbers
  states <- str_extract(cases, "[aA-zZ]+") # get state abbreviations

  temp_dt <- data.table(disease=rep(cause, times=length(states)), location_name=states, cases=cases_num)
  return(temp_dt)
}

###################################################################################
# Call functions to extract data from years 2017-2020
###################################################################################
extract_2020 <- extract_cdc_nndss(year_id=2020, max_week=53, stop_letter="pp", table_num=1)
extract_2019 <- extract_cdc_nndss(year_id=2019, max_week=52, stop_letter="kk", table_num=1)

#ignore table 3s in 2017 and 2018 because it's only TB
extract_2018_tbl1 <- extract_cdc_nndss(year_id=2018, max_week=52, stop_letter="", table_num=1)
extract_2018_tbl1[, V1:=disease]
extract_2018 <- extract_cdc_nndss(year_id=2018, max_week=52, stop_letter="s", table_num=2)

extract_2017_tbl1 <- extract_cdc_nndss(year_id=2017, max_week=52, stop_letter="", table_num=1)
extract_2017_tbl1[, V1:=disease]
extract_2017 <- extract_cdc_nndss(year_id=2017, max_week=52, stop_letter="q", table_num=2)



##################################################################################
# Formatting to fit extraction sheet
###################################################################################

# Remove 2019 week 5 table u table that had an error -- we don't need the diseases so I didn't fix
# the code for this.
extract_2019 <- extract_2019[,!names(extract_2019) %like% "Le", with=FALSE]
extract_2019 <- extract_2019[,!names(extract_2019) %like% "Li", with=FALSE]
extract_2019 <- extract_2019[,!names(extract_2019) %like% "Report", with=FALSE]

extraction <- rbindlist(list(extract_2020, extract_2019, extract_2018, extract_2017,
                             extract_2018_tbl1, extract_2017_tbl1), fill=TRUE)
# filter to 'current week' and export list of diseases to build dictionary of diseases to keep.
extraction <- extraction[V1 %in% c("V1", "V2", "V3","V4","V5"), V1:=V2] #cryptosporidium had V1-5 in the level 1 header for some reason
tbl1_keep <- unique(extract_2017_tbl1$V1)
extraction <- extraction[V1 %in% tbl1_keep | V1 %like% "Current" | V2 %like% "Current" | V3 %like% "Current" | V4 %like% "Current"]
extraction$V1 <- tolower(extraction$V1)
extraction$V2 <- tolower(extraction$V2)
extraction$V3 <- tolower(extraction$V3)
extraction$V4 <- tolower(extraction$V4)
    #write.csv(unique(extraction[,.(disease, V1)]), paste0(getwd(),"/diseases.csv"), row.names=FALSE) # export to mark diseases to keep

# Build dictionary and subset.
# Get diseases, remove <5.
diseases <- c("campylobacteriosis",
              "cholera",
              "cryptosporidiosis",
              "shigellosis",
              "tetanus",
              "measles",
              "pertussis",
              "diphtheria")
extraction <- extraction[V1 %in% diseases | V1 %like% "pertussis"
                                    | V1 %like% "salmon"
                                    | V1 %like% "varicella"
                                    | V1 %like% "haemophil"
                                    | V1 %like% "invasive pneumo"
                                    | V1 %like% "meningo"]
u5_indices <- grep("[5]", extraction$disease)
extraction <- extraction[!u5_indices]
    #write.csv(unique(extraction[,.(disease, V1,V2,V3,V4)]), paste0(getwd(), "/diseases2.csv"), row.names=FALSE)


# Add extraction sheet columns ###################################################################################
extraction_copy <- copy(extraction)
#extraction <- copy(extraction_copy)
#write.xlsx(extraction, paste0(getwd(), "/inprogress_cdc_nndss.xlsx"))


# Add parent_cause and cause_name.
names(extraction)[grep("V1", names(extraction))] <- "cause_name"
extraction[cause_name %in% c("campylobacteriosis",
                             "cholera",
                             "cryptosporidiosis",
                             "shigellosis"), parent_cause:="diarrhea"]
extraction[cause_name %like% "salmon", parent_cause:="diarrhea"]
extraction[cause_name %like% "haemophil" | cause_name %like% "meningo" | cause_name %like% "invasive pneumo", parent_cause:="meningitis"]
extraction[is.na(parent_cause), parent_cause := cause_name]
extraction[parent_cause %like% "pertussis", parent_cause:="pertussis"]
extraction[parent_cause %like% "varicella", parent_cause:="varicella"]
extraction <- extraction[!(cause_name %like% "meningococcal" & V2 %in% c("serogroup b", "unknown serogroup", "serogroups acwy", "other serogroups"))]

# Add location_ids
source("/filepath/get_location_metadata.R")
locs <- get_location_metadata(location_set_id=35, gbd_round_id=7)
locs <- locs[!ihme_loc_id %like% "GEO"]

extraction[, location_name:= gsub("\\.", "", extraction$location_name)]
index.2 <- nchar(extraction$location_name) == 2
extraction[index.2]$location_name <- extraction[index.2, lapply(.SD, toupper), .SDcols=c("location_name")]

extraction[location_name=="Conn", location_name:="Connecticut"]
extraction[location_name=="Mass", location_name:="Massachusetts"]
extraction[location_name=="Ill", location_name:="Illinois"]
extraction[location_name=="Ind", location_name:="Indiana"]
extraction[location_name=="Mich", location_name:="Michigan"]
extraction[location_name=="Kans", location_name:="Kansas"]
extraction[location_name=="Minn", location_name:="Minnesota"]
extraction[location_name=="Nebr", location_name:="Nebraska"]
extraction[location_name=="N Dak", location_name:="North Dakota"]
extraction[location_name=="S Dak", location_name:="South Dakota"]
extraction[location_name=="Del", location_name:="Delaware"]
extraction[location_name=="DC", location_name:="District of Columbia"]
extraction[location_name=="Fla", location_name:="Florida"]
extraction[location_name=="W Va", location_name:="West Virginia"]
extraction[location_name=="Ala", location_name:="Alabama"]
extraction[location_name=="Miss", location_name:="Mississippi"]
extraction[location_name=="Tenn", location_name:="Tennessee"]
extraction[location_name=="Ark", location_name:="Arkansas"]
extraction[location_name=="Okla", location_name:="Oklahoma"]
extraction[location_name=="Tex", location_name:="Texas"]
extraction[location_name=="Ariz", location_name:="Arizona"]
extraction[location_name=="Colo", location_name:="Colorado"]
extraction[location_name=="Mont", location_name:="Montana"]
extraction[location_name=="Nev", location_name:="Nevada"]
extraction[location_name=="N Mex", location_name:="New Mexico"]
extraction[location_name=="Wyo", location_name:="Wyoming"]
extraction[location_name=="Calif", location_name:="California"]
extraction[location_name=="Oreg", location_name:="Oregon"]
extraction[location_name=="Wash", location_name:="Washington"]
extraction[location_name=="Wash", location_name:="Washington"]
extraction[location_name=="Wis", location_name:="Wisconsin"]
extraction[location_name=="Amer Samoa", location_name:="American Samoa"]
extraction[location_name=="PR", location_name:="Puerto Rico"]
extraction[location_name=="VI", location_name:="United States Virgin Islands"]
extraction[location_name=="US Virgin Islands", location_name:="United States Virgin Islands"]
extraction[location_name=="CNMI" | location_name=="Commonwealth of Northern Mariana Islands", location_name:="Northern Mariana Islands"]


index.2 <- nchar(extraction$location_name) == 2
index.3 <- match(extraction$location_name[index.2], state.abb)
extraction[index.2, temp:=state.name[index.3]]
extraction[!is.na(temp), location_name := temp]
extraction <- extraction[,!"temp"]

index <- match(extraction$location_name, locs$location_name)
extraction$ihme_loc_id <- locs$ihme_loc_id[index]
extraction$location_id <- locs$location_id[index]

# Fix more NY rows that I missed in the loop because it didn't catch "NY (upstate)"
extraction_formatted <- extraction[location_name %like% "NY", location_name:="NY"]
cols <- names(extraction_formatted)[-grep("cases", names(extraction_formatted))]
ny_sums <- extraction_formatted[location_name %like% "NY",sum(cases), by=cols]
ny_sums$location_name <- "New York"
setnames(ny_sums, "V1", "cases")
extraction_formatted <- extraction_formatted[!location_name %like% "NY"]
extraction_formatted <- rbindlist(list(extraction_formatted,ny_sums), use.names=TRUE)
index <- match(extraction_formatted$location_name, locs$location_name)
extraction_formatted$ihme_loc_id <- locs$ihme_loc_id[index]
extraction_formatted$location_id <- locs$location_id[index]
extraction_formatted <- extraction_formatted[!is.na(ihme_loc_id)]

# Add dates by matching week and years from the epi weeks sheet.
weeks <- read.xlsx(paste0(getwd(),"/epi_weeks.xlsx"))
index <- match(paste0(extraction_formatted$week, extraction_formatted$year_id), paste0(weeks$epi_week, weeks$year_id))
extraction_formatted$start_date <- weeks$start_date[index]
extraction_formatted$end_date <- weeks$end_date[index]

extraction_formatted$start_date <- convertToDate(extraction_formatted$start_date)
extraction_formatted$end_date <- convertToDate(extraction_formatted$end_date)
extraction_formatted$start_date <- format(extraction_formatted$start_date, "%m/%d/%Y")
extraction_formatted$end_date <- format(extraction_formatted$end_date, "%m/%d/%Y")
extraction_formatted[is.na(start_date), start_date := "12/27/2020"]
extraction_formatted[is.na(end_date), end_date := "1/2/2021"]


# More formatting
extraction_formatted[,sex_id := 3]
extraction_formatted[,extractor := "name"]
extraction_formatted[,age_start := 0] # double check that this doesn't default to "FALSE"
extraction_formatted[,age_end := 99]
extraction_formatted[V2=="confirmed",case_status := V2]
extraction_formatted[V2=="probable",case_status := V2]
extraction_formatted[is.na(case_status), case_status:="unspecified"]
extraction_formatted[,sex := "both"]
extraction_formatted[,surveillance_name := "cdc - NNDSS"]
extraction_formatted[,link := "https://wonder.cdc.gov/nndss/nndss_weekly_tables_menu.asp?mmwr_year=2020&mmwr_week=53"]
extraction_formatted[,source_type := "surveillance"]
extraction_formatted[,measure_type := "incidence"] # flu is only reported as pediatric mortality, so we are dropping this.
                                                   # Also not including novel infuenza A because it isnt' representative of typical flu.
extraction_formatted[year_id==2020, nid:=468459]
extraction_formatted[year_id==2019, nid:=442216]
extraction_formatted[year_id==2018, nid:=373829]
extraction_formatted[year_id==2017, nid:=354339]
extraction_formatted[,year_start := year_id]
extraction_formatted[,year_end := year_id]
extraction_formatted[,age_demographer := 1]

extraction_formatted$samples_tested <- NA
extraction_formatted$is_outlier <- NA
extraction_formatted$group <- 1
extraction_formatted$specificity <- "new_weekly"
extraction_formatted$group_review <- NA
extraction_formatted$notes <- NA
extraction_formatted$sample_size <- NA
extraction_formatted <- extraction_formatted[,!c("V2","V3","V4", "disease")]

# Remove duplicate IMD rows from 2017 and 2018
# There were duplicate rows because IMD is reported as separate serogroups in table 1s (which I aggregated) and again in its own table as all serogroups.
extraction_formatted <- extraction_formatted[!(year_id %in% c(2017, 2018) & cause_name == "meningococcal disease")]
extraction_formatted <- extraction_formatted[order(parent_cause, cause_name, case_status)]


# VALIDATIONS #################################################################
# Check differences in columns with extraction template
extraction_template <- read.xlsx(paste0(getwd(), "/filepath/extraction_template.xlsx"))
diff_cols <- setdiff(names(extraction_template), names(extraction_formatted))


# Make sure same number of diseases are present in all years
check_disease_yrs <- unique(final_extraction[,.(cause_name, year_id)])
check_disease_n <- check_disease_yrs[,.N, year_id]

# COMPLETE #################################################################
# write cdc nndss extraction sheet out
write.xlsx(extraction_formatted, paste0(getwd(), "/cdc_nndss_weekly_extract.xlsx"))

# remove week column to bind with overall extraction sheet
final_extraction <- extraction_formatted[,!"week"]

final_extraction <- read.xlsx(paste0(getwd(),"/cdc_nndss_weekly_extract.xlsx"))
final_extraction <- data.table(final_extraction)
final_extraction$group <- 1
final_extraction$specificity <- as.character(final_extraction$specificity)
final_extraction$specificity <- "new_weekly"
final_extraction <- final_extraction[,!"week"]

recent_extract <- read.xlsx("/filepath/2021_06_01_all_extractions.xlsx")
recent_extract <- rbindlist(list(recent_extract, final_extraction), use.names = TRUE, fill = TRUE)
write.xlsx(recent_extract, "/filepath/2021_06_04_all_extractions.xlsx")

#---------
#---------
#---------
#---------
#---------MORE CLEANING - set old CDC - NNDSS rows to group_review = 0 and new weekly to group_review=1.
recent_extract <- read.xlsx("/filepath/2021_06_04_all_extractions.xlsx")
recent_extract <- data.table(recent_extract)
recent_extract[specificity=="new_weekly", group_review:=1]
recent_extract[surveillance_name %like% "cdc - NNDSS" & specificity != "new_weekly", group_review:=0]
recent_extract[surveillance_name %like% "cdc - NNDSS" & is.na(specificity), group_review:=0]
new_rows <- recent_extract[surveillance_name %like% "cdc - NNDSS" & specificity %like% "new_weekly"]
recent_extract <- recent_extract[!(surveillance_name %like% "cdc - NNDSS" & specificity %like% "new_weekly")]

recent_extract$start_date <- convertToDate(recent_extract$start_date)
recent_extract$end_date <- convertToDate(recent_extract$end_date)
recent_extract$start_date <- format(recent_extract$start_date, "%m/%d/%Y")
recent_extract$end_date <- format(recent_extract$end_date, "%m/%d/%Y")

recent_extract <- rbindlist(list(recent_extract, new_rows))
write.xlsx(recent_extract, "/filepath/2021_06_07_all_extractions.xlsx")
